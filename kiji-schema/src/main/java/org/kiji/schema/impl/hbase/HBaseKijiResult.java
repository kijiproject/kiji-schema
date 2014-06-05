/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kiji.schema.impl.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.EntityId;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiColumnPagingNotEnabledException;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiResult;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.impl.BoundColumnReaderSpec;
import org.kiji.schema.layout.ColumnReaderSpec;
import org.kiji.schema.layout.KijiColumnNameTranslator;
import org.kiji.schema.layout.impl.CellDecoderProvider;

/** HBase implementation of KijiResult. */
@ApiAudience.Private
@ApiStability.Experimental
public final class HBaseKijiResult implements KijiResult {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseKijiResult.class);
  private static final Comparator<KeyValue> KV_COMPARATOR = new KVComparator();
  private static final Comparator<IndexRange> INDEX_RANGE_COMPARATOR = new IndexStartComparator();

  // -----------------------------------------------------------------------------------------------
  // Static classes and methods
  // -----------------------------------------------------------------------------------------------

  /** Start and end index of an array slice. Start index is inclusive; end index is exclusive. */
  private static final class IndexRange {
    private final int mStart;
    private final int mEnd;

    /**
     * Initialize a new IndexRange.
     *
     * @param start Inclusive start index of the range.
     * @param end Exclusive end index of the range.
     */
    private IndexRange(
        final int start,
        final int end
    ) {
      Preconditions.checkArgument(start <= end,
          "Start index must be less than or equal to end index.");
      mStart = start;
      mEnd = end;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return Objects.toStringHelper(IndexRange.class)
          .add("start", mStart)
          .add("end", mEnd)
          .toString();
    }
  }

  /** Comparator for sorting IndexRanges by their start index. */
  private static final class IndexStartComparator implements Comparator<IndexRange> {

    /** {@inheritDoc} */
    @Override
    public int compare(
        final IndexRange indexRange,
        final IndexRange indexRange2
    ) {
      if (indexRange.mStart == indexRange2.mStart) {
        return 0;
      } else if (indexRange.mStart > indexRange2.mStart) {
        return 1;
      } else {
        return -1;
      }
    }
  }

  /**
   * Get the KeyValue representing the earliest value of the next family.
   *
   * @param kv KeyValue from which to get the next family.
   * @return the KeyValue representing the earliest possible value of the next family.
   */
  private static KeyValue nextFamilyKV(
      final KeyValue kv
  ) {
    return new KeyValue(
        kv.getRow(),
        Arrays.copyOf(kv.getFamily(), kv.getFamily().length + 1),
        new byte[0],
        Long.MAX_VALUE,
        kv.getValue());
  }

  /**
   * Get the KeyValue representing the earliest possible value of the next qualifier.
   *
   * @param kv KeyValue from which to get the next qualifier.
   * @return the KeyValue representing the earliest possible value of the next qualifier.
   */
  private static KeyValue nextQualifierKV(
      final KeyValue kv
  ) {
    return new KeyValue(
        kv.getRow(),
        kv.getFamily(),
        Arrays.copyOf(kv.getQualifier(), kv.getQualifier().length + 1),
        Long.MAX_VALUE,
        kv.getValue());
  }

  /**
   * Get the index of the given KeyValue in the given Result.
   *
   * <p>
   *   If the KeyValue is not present, returns the insertion point where the KeyValue would be.
   * </p>
   *
   * @param result Result in which to find the given KeyValue.
   * @param toFind KeyValue to find in the given Result.
   * @return the index of the given KeyValue in the given Result.
   */
  private static int kvIndex(
      final Result result,
      final KeyValue toFind
  ) {
    return kvIndex(result, toFind, new IndexRange(0, result.size()));
  }

  /**
   * Get the index of the given KeyValue in the given Result.
   *
   * <p>
   *   If the KeyValue is not present, returns the insertion point where the KeyValue would be.
   * </p>
   *
   * @param result Result in which to find the given KeyValue.
   * @param toFind KeyValue to find in the given Result.
   * @param range Inclusive-exclusive index range in which to search.
   * @return the index of the given KeyValue in the given Result.
   */
  private static int kvIndex(
      final Result result,
      final KeyValue toFind,
      final IndexRange range
  ) {
    final int binarySearchResult = Arrays.binarySearch(
        result.raw(), range.mStart, range.mEnd, toFind, KV_COMPARATOR);
    if (binarySearchResult < 0) {
      return -1 - binarySearchResult;  // see documentation for Arrays.binarySearch()
    } else {
      return binarySearchResult;
    }
  }

  /**
   * Get the KeyValue at the given index from the given Result.
   *
   * @param result Result from which to get the KeyValue at the given index.
   * @param index Array index of the KeyValue to retrieve.
   * @return the KeyValue at the given index from the given Result or null if the index is out of
   *     bounds.
   */
  private static KeyValue kvAtIndex(
      final Result result,
      final int index
  ) {
    if ((index < 0) || (index >= result.size())) {
      return null;
    }
    return result.raw()[index];
  }

  /**
   * Invert a sorted list of IndexRanges.
   *
   * <p>
   *   Ranges are assume to be non-overlapping. Ranges are assumed to be inclusive of their start
   *   index and exclusive of their end index. Ranges returned are also inclusive-exclusive. If the
   *   end index of one range and the start index of the next are equal, no range will be included
   *   in the inverted ranges for that interval.
   * </p>
   *
   * @param toInvert List of IndexRanges to invert.
   * @param minimum start index of the first range in the inverted ranges.
   * @param maximum end index of the last range in the inverted ranges.
   * @return a sorted list of ranges representing the inversion of the input list.
   */
  private static List<IndexRange> invertRanges(
      final List<IndexRange> toInvert,
      final int minimum,
      final int maximum
  ) {
    if (toInvert.isEmpty()) {
      return Lists.newArrayList(new IndexRange(minimum, maximum));
    } else {
      final List<IndexRange> inverted = Lists.newArrayList();
      final Iterator<IndexRange> toInvertIterator = toInvert.iterator();
      IndexRange lastRange = toInvertIterator.next();
      if (minimum != lastRange.mStart) {
        inverted.add(new IndexRange(minimum, lastRange.mStart));
      }
      while (toInvertIterator.hasNext()) {
        final IndexRange nextRange = toInvertIterator.next();
        if (lastRange.mEnd != nextRange.mStart) {
          inverted.add(new IndexRange(lastRange.mEnd, nextRange.mStart));
        }
        lastRange = nextRange;
      }
      if (lastRange.mEnd != maximum) {
        inverted.add(new IndexRange(lastRange.mEnd, maximum));
      }
      return inverted;
    }
  }

  // -----------------------------------------------------------------------------------------------
  // State, private (non-static) classes, and private methods
  // -----------------------------------------------------------------------------------------------

  private final EntityId mEntityId;
  private final KijiDataRequest mDataRequest;
  private final Result mUnpagedResult;
  private final KijiColumnNameTranslator mColumnNameTranslator;
  private final CellDecoderProvider mCellDecoderProvider;
  private final HBaseKijiTable mTable;
  private final KeyValueToKijiCell<Object> mKeyValueToKijiCell;

  /**
   * Initialize a new HBaseKijiResult.
   *
   * @param entityId EntityId of the row from which to read cells.
   * @param dataRequest KijiDataRequest defining the values to retrieve.
   * @param unPagedResult {@link org.apache.hadoop.hbase.client.Result} containing all non-paged
   *     values requested by dataRequest. Includes values which should be removed during
   *     post-processing such as the first value from a paged column, or extra versions of a column
   *     with a lower max versions than the highest in the request.
   * @param columnNameTranslator ColumnNameTranslator to use for decoding KeyValues.
   * @param cellDecoderProvider Provider for cell decoders to use for decoding KeyValues.
   * @param table Kiji table from which to retrieve cells.
   */
  public HBaseKijiResult(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final Result unPagedResult,
      final KijiColumnNameTranslator columnNameTranslator,
      final CellDecoderProvider cellDecoderProvider,
      final HBaseKijiTable table
  ) {
    mEntityId = entityId;
    mDataRequest = dataRequest;
    mColumnNameTranslator = columnNameTranslator;
    mCellDecoderProvider = cellDecoderProvider;
    mTable = table;
    mKeyValueToKijiCell = new KeyValueToKijiCell<Object>();
    mUnpagedResult = filterToMatchRequest(unPagedResult);
  }

  /**
   * Function to transform a KeyValue into a KijiCell.
   *
   * @param <T> type of the value in the KijiCell produced by this function.
   */
  private final class KeyValueToKijiCell<T> implements Function<KeyValue, KijiCell<T>> {
    /** {@inheritDoc} */
    @Override
    public KijiCell<T> apply(
        final KeyValue input
    ) {
      return cellFromKeyValue(input);
    }
  }

  /** An iterator across all cells requested by the data request which defines this KijiResult. */
  private final class FullRequestIterator implements Iterator<KijiCell<?>> {

    private final Iterator<KijiCell<Object>> mUnpagedIterator;
    private final Iterator<KijiColumnName> mPagedColumns;
    private Iterator<KijiCell<Object>> mCurrentPagedIterator;
    private KijiCell<Object> mNextCell;

    /** Initialize a new FullRequestIterator. */
    private FullRequestIterator() {
      mUnpagedIterator = Iterators.transform(
          new HBaseResultIterator(mUnpagedResult), mKeyValueToKijiCell);

      final Set<KijiColumnName> pagedColumns = Sets.newHashSet();
      for (KijiDataRequest.Column columnRequest : mDataRequest.getColumns()) {
        if (columnRequest.isPagingEnabled()) {
          pagedColumns.add(columnRequest.getColumnName());
        }
      }
      mPagedColumns = pagedColumns.iterator();
      mCurrentPagedIterator = mPagedColumns.hasNext() ? iterator(mPagedColumns.next()) : null;
      mNextCell = getNextCell();
    }

    /**
     * Get the next cell from the underlying iterators, or null if none exist.
     *
     * @return the next cell from the underlying iterators, or null if none exist.
     */
    private KijiCell<Object> getNextCell() {
      if (mUnpagedIterator.hasNext()) {
        return mUnpagedIterator.next();
      } else if (null != mCurrentPagedIterator && mCurrentPagedIterator.hasNext()) {
        return mCurrentPagedIterator.next();
      } else if (null != mCurrentPagedIterator) {
        mCurrentPagedIterator = mPagedColumns.hasNext() ? iterator(mPagedColumns.next()) : null;
        return getNextCell();
      } else {
        return null;
      }
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
      return (null != mNextCell);
    }

    /** {@inheritDoc} */
    @Override
    public KijiCell<?> next() {
      final KijiCell<?> nextCell = mNextCell;
      if (null == nextCell) {
        throw new NoSuchElementException();
      } else {
        mNextCell = getNextCell();
        return nextCell;
      }
    }

    /** {@inheritDoc} */
    @Override
    public void remove() {
      throw new UnsupportedOperationException("FullRequestIterator does not support remove().");
    }
  }

  /**
   * Get the cell decoder for the given column.
   *
   * @param column Kiji column for which to get a decoder.
   * @param <T> type of the value returned by the decoder.
   * @return the cell decoder for the given column.
   */
  private <T> KijiCellDecoder<T> getDecoder(
      final KijiColumnName column
  ) {
    final ColumnReaderSpec readerSpec = mDataRequest.getRequestForColumn(column).getReaderSpec();
    try {
      if (null != readerSpec) {
        return mCellDecoderProvider.getDecoder(BoundColumnReaderSpec.create(readerSpec, column));
      } else {
        return mCellDecoderProvider.getDecoder(column.getFamily(), column.getQualifier());
      }
    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }
  }

  /**
   * Get the largest max versions from the data request which defines this KijiResult.
   *
   * @return the largest max versions from the data request which defines this KijiResult.
   */
  private int largestMaxVersions() {
    int largest = 1;
    for (KijiDataRequest.Column column : mDataRequest.getColumns()) {
      if (largest < column.getMaxVersions()) {
        largest = column.getMaxVersions();
      }
    }
    return largest;
  }

  /**
   * Filter the given Result to only include values which were requested as part of data request
   * which defines this KijiResult.
   *
   * <p>
   *   This filtering is run once during construction and allows other methods to make strong
   *   assumptions about the contents of the backing Result.
   * </p>
   *
   * @param result Result from which to filter values requested by the data request which defines
   *     this KijiResult.
   * @return a new Result containing only those KeyValues which were requested by the data request
   *     which defines this KijiResult.
   */
  private Result filterToMatchRequest(
      final Result result
  ) {
    final int largestMaxVersions = largestMaxVersions();
    final Set<KijiColumnName> pagedColumns = Sets.newHashSet();
    final Set<KijiColumnName> columnsWithExtraVersions = Sets.newHashSet();
    for (KijiDataRequest.Column columnRequest : mDataRequest.getColumns()) {
      if (columnRequest.isPagingEnabled()) {
        pagedColumns.add(columnRequest.getColumnName());
      } else if (largestMaxVersions > columnRequest.getMaxVersions()) {
        columnsWithExtraVersions.add(columnRequest.getColumnName());
      }
    }

    final List<IndexRange> rangesToRemove = Lists.newArrayList();

    // Paged columns may contain one value which should be removed.
    for (KijiColumnName column : pagedColumns) {
      final int index = kvIndex(result, mostRecentKey(column));
      final KeyValue kv = kvAtIndex(result, index);
      if (null != kv) {
        if (column.isFullyQualified() && Objects.equal(column, kcnFromKeyValue(kv))) {
          rangesToRemove.add(new IndexRange(index, index + 1));
        } else if (!column.isFullyQualified()
            && Objects.equal(column.getFamily(), kcnFromKeyValue(kv).getFamily())) {
          rangesToRemove.add(new IndexRange(index, index + 1));
        }
      }
    }

    // Extra versions of columns with fewer versions requested than the largest max versions should
    // be removed.
    for (KijiColumnName column : columnsWithExtraVersions) {
      if (column.isFullyQualified()) {
        final KeyValue mostRecentKey = mostRecentKey(column);
        final int maxVersionsIndex = kvIndex(result, mostRecentKey) + columnMaxVersions(column);
        final int nextQualifierIndex = kvIndex(result, nextQualifierKV(mostRecentKey));
        if (nextQualifierIndex > maxVersionsIndex) {
          rangesToRemove.add(new IndexRange(maxVersionsIndex, nextQualifierIndex));
        } // Otherwise there are fewer values in this column than the requested max versions so we
          // shouldn't remove any. TODO add a test to ensure this behavior is correct.
      } else {
        // Include the requested number of max versions for each qualifier in a family.
        final int columnMaxVersions = columnMaxVersions(column);
        final KeyValue mostRecentFamilyKey = mostRecentKey(column);
        KeyValue mostRecentQualifierKey = kvAtIndex(result, kvIndex(result, mostRecentFamilyKey));
        while (null != mostRecentQualifierKey && Objects.equal(
            column.getFamily(), kcnFromKeyValue(mostRecentQualifierKey).getFamily())) {
          final int maxVersionsIndex = kvIndex(result, mostRecentQualifierKey) + columnMaxVersions;
          final int nextQualifierIndex = kvIndex(result, nextQualifierKV(mostRecentQualifierKey));
          if (nextQualifierIndex > maxVersionsIndex) {
            rangesToRemove.add(new IndexRange(maxVersionsIndex, nextQualifierIndex));
          } // Otherwise there are fewer values in this column than the requested max versions so we
            // shouldn't remove any. TODO add a test to ensure this behavior is correct
          mostRecentQualifierKey = kvAtIndex(result, nextQualifierIndex);
        }
      }
    }

    if (rangesToRemove.size() == 0) {
      return result;
    } else {
      Collections.sort(rangesToRemove, INDEX_RANGE_COMPARATOR);
      LOG.debug("removing retrieved cells outside data request at indices: {}",
          rangesToRemove.toString());
      final List<IndexRange> rangesToKeep = invertRanges(rangesToRemove, 0, result.size());
      LOG.debug("keeping cells matching request at indices: {}", rangesToKeep.toString());

      final List<KeyValue> kvsToKeep = Lists.newArrayList();
      for (IndexRange range : rangesToKeep) {
        kvsToKeep.addAll(Arrays.asList(
            Arrays.copyOfRange(result.raw(), range.mStart, range.mEnd)));
      }
      return new Result(kvsToKeep);
    }
  }

  /**
   * Get the KeyValue representing the most recent possible Key of a Kiji column.
   *
   * <p>
   *   This KeyValue has no Value and should be used only for finding the location in the underlying
   *   Result at which the value should be found.
   * </p>
   *
   * @param column Kiji column for which the most recent possible Key.
   * @return the KeyValue representing the most recent possible Key of a Kiji column.
   */
  private KeyValue mostRecentKey(
      final KijiColumnName column
  ) {
    return toKey(column, Long.MAX_VALUE);
  }

  /**
   * Get the KeyValue representing the Key of a Kiji column with the given timestamp.
   *
   * <p>
   *   This KeyValue has no Value and should be used only for finding the location in the underlying
   *   Result at which the value should be found.
   * </p>
   *
   * @param column Kiji column for which the Key with the given timestamp.
   * @param timestamp Timestamp of the Key to create.
   * @return the KeyValue representing the Key of a Kiji column with the given timestamp.
   */
  private KeyValue toKey(
      final KijiColumnName column,
      final long timestamp
  ) {
    final HBaseColumnName hBaseColumnName;
    try {
      hBaseColumnName = mColumnNameTranslator.toHBaseColumnName(column);
    } catch (NoSuchColumnException nsce) {
      throw new KijiIOException(nsce);
    }
    return new KeyValue(mEntityId.getHBaseRowKey(),
        hBaseColumnName.getFamily(),
        hBaseColumnName.getQualifier(),
        timestamp,
        new byte[0]);
  }

  /**
   * Get the index at which the given Key may be found in the unpaged Result backing this
   * KijiResult.
   *
   * <p>
   *   Returns the index at which the Key would be inserted if the Result does not contain a
   *   KeyValue with the given Key.
   * </p>
   *
   * @param toFind Key to find in the underlying unpaged Result.
   * @return the index at which the given Key may be found in the unpaged Result backing this
   *     KijiResult.
   */
  private int kvIndex(
      final KeyValue toFind
  ) {
    return kvIndex(mUnpagedResult, toFind);
  }

  /**
   * Get the KeyValue at the given index from the underlying unpaged Result.
   *
   * @param index Array index of the KeyValue to retrieve.
   * @return the KeyValue at the given index from the underlying unpaged Result or null if the index
   *     is out of bounds.
   */
  private KeyValue kvAtIndex(
      final int index
  ) {
    if (index < 0 || index >= mUnpagedResult.size()) {
      return null;
    }
    return mUnpagedResult.raw()[index];
  }

  /**
   * Get the KijiColumnName encoded in the Key of a given KeyValue.
   *
   * @param kv KeyValue from which to get the encoded KijiColumnName.
   * @return the KijiColumnName encoded in the Key of a given KeyValue.
   */
  private KijiColumnName kcnFromKeyValue(
      final KeyValue kv
  ) {
    final HBaseColumnName hBaseColumnName = new HBaseColumnName(kv.getFamily(), kv.getQualifier());
    try {
      return mColumnNameTranslator.toKijiColumnName(hBaseColumnName);
    } catch (NoSuchColumnException nsce) {
      throw new KijiIOException(nsce);
    }
  }

  /**
   * Translate a KeyValue into a KijiCell.
   *
   * @param kv KeyValue from which to get the encoded KijiCell.
   * @param <T> type of the value encoded in the KeyValue.
   * @return the KijiCell encoded in the KeyValue.
   */
  private <T> KijiCell<T> cellFromKeyValue(
      final KeyValue kv
  ) {
    final KijiColumnName column = kcnFromKeyValue(kv);
    final DecodedCell<T> decodedCell;
    try {
      final KijiCellDecoder<T> decoder = getDecoder(column);
      Preconditions.checkNotNull(decoder);
      decodedCell = decoder.decodeCell(kv.getValue());
    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }
    return KijiCell.create(
        column,
        kv.getTimestamp(),
        decodedCell);
  }

  /**
   * Get the max versions for the given column.
   *
   * @param column Kiji column for which to get the max versions.
   * @return the max versions for the given column.
   */
  private int columnMaxVersions(
      final KijiColumnName column
  ) {
    return mDataRequest.getRequestForColumn(column).getMaxVersions();
  }

  /**
   * Get a slice of the underlying unpaged Result containing only KeyValues from the given column.
   *
   * @param column Kiji column or family for which to get a slice from the underlying Result.
   * @return a slice of the underlying Result containing only KeyValues from the given column.
   */
  private IndexRange slice(
      final KijiColumnName column
  ) {
    final KeyValue mostRecentKey = mostRecentKey(column);
    final int startIndex = kvIndex(mostRecentKey);
    final int endIndex = column.isFullyQualified()
        ? kvIndex(nextQualifierKV(mostRecentKey))
        : kvIndex(nextFamilyKV(mostRecentKey));
    return new IndexRange(startIndex, endIndex);
  }

  // -----------------------------------------------------------------------------------------------
  // Public interface
  // -----------------------------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mEntityId;
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    return mDataRequest;
  }

  /** {@inheritDoc} */
  @Override
  public <T> KijiCell<T> getMostRecentCell(
      final KijiColumnName column
  ) {
    Preconditions.checkArgument(column.isFullyQualified(),
        "Can only get a cell from a fully qualified column. Found: %s", column);
    final KijiDataRequest.Column columnRequest = mDataRequest.getRequestForColumn(column);
    Preconditions.checkNotNull(columnRequest, "No request for column: %s in request: %s",
        column, mDataRequest);
    Preconditions.checkArgument(!columnRequest.isPagingEnabled(),
        "Cannot get the most recent version of a paged column. Found column: %s in request: %s",
        column, mDataRequest);
    final KeyValue maybeKv = kvAtIndex(kvIndex(mostRecentKey(column)));
    if ((null != maybeKv) && Objects.equal(column, kcnFromKeyValue(maybeKv))) {
      return cellFromKeyValue(maybeKv);
    } else {
      return null;
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T> KijiCell<T> getCell(
      final KijiColumnName column,
      final long timestamp
  ) {
    Preconditions.checkArgument(column.isFullyQualified(),
        "Can only get a cell from a fully qualified column. Found: %s", column);
    final KijiDataRequest.Column columnRequest = mDataRequest.getRequestForColumn(column);
    Preconditions.checkNotNull(columnRequest, "No request for column: %s in request: %s",
        column, mDataRequest);
    Preconditions.checkArgument(!columnRequest.isPagingEnabled(),
        "Cannot get a cell from a paged column. Found column: %s in request: %s",
        column, mDataRequest);
    final KeyValue kv = toKey(column, timestamp);
    final KeyValue kvAtIndex = kvAtIndex(kvIndex(kv));
    if ((null != kvAtIndex)
        && Objects.equal(column, kcnFromKeyValue(kvAtIndex))
        && Objects.equal(timestamp, kvAtIndex.getTimestamp())
    ) {
      return cellFromKeyValue(kvAtIndex);
    } else {
      return null;
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterator<KijiCell<T>> iterator(
      final KijiColumnName column
  ) {
    final KijiDataRequest.Column columnRequest = mDataRequest.getRequestForColumn(column);
    Preconditions.checkNotNull(columnRequest, "No request for column: %s in request: %s",
        column, mDataRequest);
    Preconditions.checkArgument(column.isFullyQualified()
        || mTable.getLayout().getFamilyMap().get(column.getFamily()).isMapType(),
        "May only iterate over a fully qualified column or a map-type family.");
    final KijiCellDecoder<T> decoder = getDecoder(column);
    if (columnRequest.isPagingEnabled()) {
      final HBaseQualifierIterator optionalQualifierIterator;
      try {
        optionalQualifierIterator = column.isFullyQualified()
            ? null
            : new HBaseQualifierIterator(mEntityId, mDataRequest, mTable, column);
      } catch (KijiColumnPagingNotEnabledException e) {
        throw new InternalKijiError(String.format(
            "Column %s is both paged and not paged in request: %s", column, mDataRequest));
      }
      return new HBasePagedVersionIterator<T>(
          mEntityId,
          mDataRequest,
          column,
          decoder,
          mColumnNameTranslator,
          mColumnNameTranslator.getTableLayout(),
          mTable,
          optionalQualifierIterator);
    } else {
      final IndexRange columnSlice = slice(column);
      return new HBaseNonPagedVersionIterator<T>(
          mColumnNameTranslator,
          decoder,
          mEntityId,
          new HBaseResultIterator(mUnpagedResult, columnSlice.mStart, columnSlice.mEnd));
    }
  }

  /** {@inheritDoc} */
  @Override
  public Iterator<KijiCell<?>> iterator() {
    return new FullRequestIterator();
  }
}
