/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.schema.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiColumnPagingNotEnabledException;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiPager;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.SpecificCellDecoderFactory;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.layout.impl.ColumnNameTranslator;
import org.kiji.schema.util.TimestampComparator;

/**
 * An implementation of KijiRowData that wraps an HBase Result object.
 */
@ApiAudience.Private
public final class HBaseKijiRowData implements KijiRowData {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseKijiRowData.class);

  /** The entity id for the row. */
  private final EntityId mEntityId;

  /** The request used to retrieve this Kiji row data. */
  private final KijiDataRequest mDataRequest;

  /** The HBase KijiTable we are reading from. */
  private final HBaseKijiTable mTable;

  /** The layout for the table this row data came from. */
  private final KijiTableLayout mTableLayout;

  /** The HBase result providing the data of this object. */
  private Result mResult;

  /** Provider for cell decoders. */
  private final CellDecoderProvider mDecoderProvider;

  /** A map from kiji family to kiji qualifier to timestamp to raw encoded cell values. */
  private NavigableMap<String, NavigableMap<String, NavigableMap<Long, byte[]>>> mFilteredMap;

  /**
   * Creates a provider for cell decoders.
   *
   * <p> The provider creates decoders for specific Avro records. </p>
   *
   * @param table HBase KijiTable to create a CellDecoderProvider for.
   * @return a new CellDecoderProvider for the specified HBase KijiTable.
   * @throws IOException on I/O error.
   */
  private static CellDecoderProvider createCellProvider(HBaseKijiTable table) throws IOException {
    return new CellDecoderProvider(
        table,
        SpecificCellDecoderFactory.get(),
        Maps.<KijiColumnName, CellSpec>newHashMap());
  }

  /**
   * Initializes a row data from an HBase Result.
   *
   * <p>
   *   The HBase Result may contain more cells than are requested by the user.
   *   KijiDataRequest is more expressive than HBase Get/Scan requests.
   *   Currently, {@link #getMap()} attempts to complete the filtering to meet the data request
   *   requirements expressed by the user, but this can be inaccurate.
   * </p>
   *
   * @param table Kiji table containing this row.
   * @param dataRequest Data requested for this row.
   * @param entityId This row entity ID.
   * @param result HBase result containing the requested cells (and potentially more).
   * @param decoderProvider Provider for cell decoders.
   *     Null means the row creates its own provider for cell decoders (not recommended).
   * @throws IOException on I/O error.
   */
  public HBaseKijiRowData(
      HBaseKijiTable table,
      KijiDataRequest dataRequest,
      EntityId entityId,
      Result result,
      CellDecoderProvider decoderProvider)
      throws IOException {
    mTable = table;
    mTableLayout = table.getLayout();
    mDataRequest = dataRequest;
    mEntityId = entityId;
    mResult = result;
    mDecoderProvider = (decoderProvider != null) ? decoderProvider : createCellProvider(table);
  }

  /**
   * An iterator for cells in group type column or map type column family.
   *
   * @param <T> The type parameter for the KijiCells being iterated over.
   */
  private static final class KijiCellIterator<T> implements Iterator<KijiCell<T>> {
    /** A KeyValue comparator. */
    private static final KVComparator KV_COMPARATOR = new KVComparator();
    /** The cell decoder for this column. */
    private final KijiCellDecoder<T> mDecoder;
    /** The column name translator for the given table. */
    private final ColumnNameTranslator mColumnNameTranslator;
    /** The maximum number of versions requested. */
    private final int mMaxVersions;
    /** An array of KeyValues returned by HBase. */
    private final KeyValue[] mKVs;
    /** The column or map type family being iterated over. */
    private final KijiColumnName mColumn;
    /** The number of versions returned by the iterator so far. */
    private int mNumVersions;
    /** The current index in the underlying KV array. */
    private int mCurrentIdx;
    /** The next cell to return. */
    private KijiCell<T> mNextCell;

    /**
     * An iterator of KijiCells, for a particular column.
     *
     * @param columnName The Kiji column that is being iterated over.
     * @param rowdata The HBaseKijiRowData instance containing the desired data.
     * @param eId of the rowdata we are iterating over.
     * @throws IOException on I/O error
     */
    protected KijiCellIterator(KijiColumnName columnName, HBaseKijiRowData rowdata, EntityId eId)
        throws IOException {
      mColumn = columnName;
      // Initialize column name translator.
      mColumnNameTranslator = new ColumnNameTranslator(rowdata.mTableLayout);
      // Get cell decoder.
      mDecoder = rowdata.mDecoderProvider.getDecoder(mColumn.getFamily(), mColumn.getQualifier());
      // Get info about the data request for this column.
      KijiDataRequest.Column columnRequest = rowdata.mDataRequest.getColumn(mColumn.getFamily(),
          mColumn.getQualifier());
      mMaxVersions = columnRequest.getMaxVersions();
      mKVs = rowdata.mResult.raw();
      mNumVersions = 0;
      // Find the first index for this column.
      final HBaseColumnName colName = mColumnNameTranslator.toHBaseColumnName(mColumn);
      mCurrentIdx = findInsertionPoint(mKVs, new KeyValue(eId.getHBaseRowKey(), colName.getFamily(),
          colName.getQualifier()));
      mNextCell = getNextCell();
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
      return (null != mNextCell);
    }

    /** {@inheritDoc} */
    @Override
    public KijiCell<T> next() {
      KijiCell<T> cellToReturn = mNextCell;
      if (null == mNextCell) {
        throw new NoSuchElementException();
      } else {
        mNumVersions += 1;
      }
      mCurrentIdx = getNextIndex(mCurrentIdx);
      mNextCell = getNextCell();
      return cellToReturn;
    }

    /** {@inheritDoc} */
    @Override
    public void remove() {
      throw new UnsupportedOperationException("Removing a cell is not a supported operation.");
    }

    /**
     * Constructs the next cell that will be returned by the iterator.
     *
     * @return The next cell in the column we are iterating over, potentially null.
     */
    private KijiCell<T> getNextCell() {
      KijiCell<T> nextCell = null;
      try {
        if (mCurrentIdx < mKVs.length) { // If our index is out of bounds, nextCell is null.
          final KeyValue kv = mKVs[mCurrentIdx];
          // Filter KeyValues by Kiji column family.
          final KijiColumnName colName = mColumnNameTranslator.toKijiColumnName(
              new HBaseColumnName(kv.getFamily(), kv.getQualifier()));
          nextCell = new KijiCell<T>(mColumn.getFamily(), colName.getQualifier(),
              kv.getTimestamp(), mDecoder.decodeCell(kv.getValue()));
        }
      } catch (IOException ex) {
        throw new KijiIOException(ex);
      }
      return nextCell;
    }

    /**
     * Finds the index of the next KeyValue in the underlying KV array that is from the column we
     * are iterating over.
     *
     * @param lastIndex The index of the KV used to construct the last returned cell.
     * @return The index of the next KeyValue from the column we are iterating over.If there are no
     * more cells to iterate over, the returned value will be mKVs.length.
     */
    private int getNextIndex(int lastIndex) {
      int nextIndex = lastIndex;
      if (mColumn.isFullyQualified()) {
        if (mNumVersions < mMaxVersions) {
          nextIndex = lastIndex + 1; // The next element should be the next in the array.
        } else {
          nextIndex = mKVs.length; //There is nothing else to return.
        }
      } else {
        if (mNumVersions <= mMaxVersions) {
          nextIndex = lastIndex + 1; // The next element should be the next in the array.
        } else {
          mNumVersions = 0; // Reset current number of versions.
          nextIndex = findInsertionPoint(mKVs, makePivotKeyValue(mKVs[lastIndex]));
        }
      }
      if (nextIndex < mKVs.length) {
        final KeyValue kv = mKVs[nextIndex];
        // Filter KeyValues by Kiji column family.
        try {
          final KijiColumnName colName = mColumnNameTranslator.toKijiColumnName(
            new HBaseColumnName(kv.getFamily(), kv.getQualifier()));
          if (!colName.getQualifier().equals(mNextCell.getQualifier())) {
            if (mColumn.isFullyQualified()) {
              return mKVs.length;
            } else {
              nextIndex = findInsertionPoint(mKVs, makePivotKeyValue(mKVs[lastIndex]));
              mNumVersions = 0;
            }
          }
          if (!colName.getFamily().equals(mColumn.getFamily())) {
            return mKVs.length; // From the wrong column family.
          }
        } catch (NoSuchColumnException ex) {
          throw new KijiIOException(ex);
        }
      }
      return nextIndex;
    }

   /**
    * Finds the insertion point of the pivot KeyValue in the  KeyValue array and returns the index.
    *
    * @param kvs The KeyValue array to search in.
    * @param pivotKeyValue A KeyValue that is less than or equal to the first KeyValue for our
    *     column, and larger than any KeyValue that may preceed values for our desired column.
    * @return The index of the first KeyValue in the desired map type family.
    */
    private static int findInsertionPoint(KeyValue[] kvs, KeyValue pivotKeyValue) {
      // Now find where the pivotKeyValue would be placed
      int binaryResult = Arrays.<KeyValue>binarySearch(kvs, pivotKeyValue, KV_COMPARATOR);
      if (binaryResult < 0) {
        return -1 - binaryResult; // Algebra on the formula provided in the binary search JavaDoc.
      } else {
        return binaryResult;
      }
    }

   /**
    * Constructs a KeyValue that is strictly greater than every KeyValue associated with baseline
    * KeyValue.
    *
    * @param baselineKV A baseline keyValue from a column that we wish to construct a KeyValue that
    *     is strictly greater than every KeyValue from the same HBase column.
    * @return A KeyValue that is strictly greater than every KeyValue that is in the same column as
    *     the baseline (input) KeyValue, and less than the next column. Note that the ordering is
    *     defined by KVComparator.
    *
    */
    private static KeyValue makePivotKeyValue(KeyValue baselineKV) {
      // Generate a byte[] that is the qualifier of the baseline byte[], with a 0 byte appended.
      byte[] baselineQualifier = baselineKV.getQualifier();
      byte[] smallestStrictlyGreaterQualifier = Arrays.copyOf(baselineQualifier,
          baselineQualifier.length + 1);
      return new KeyValue(baselineKV.getRow(), baselineKV.getFamily(),
          smallestStrictlyGreaterQualifier);
    }
  }

  /**
   * An iterable of cells in a column.
   *
   * @param <T> The type parameter for the KijiCells being iterated over.
   */
  private static final class CellIterable<T> implements Iterable<KijiCell<T>> {
    /** The column family. */
    private final KijiColumnName mColumnName;
    /** The rowdata we are iterating over. */
    private final HBaseKijiRowData mRowData;
    /** The entity id for the row. */
    private final EntityId mEntityId;
    /**
     * An iterable of KijiCells, for a particular column.
     *
     * @param colName The Kiji column family that is being iterated over.
     * @param rowdata The HBaseKijiRowData instance containing the desired data.
     * @param eId of the rowdata we are iterating over.
     */
    protected CellIterable(KijiColumnName colName, HBaseKijiRowData rowdata, EntityId eId) {
      mColumnName = colName;
      mRowData = rowdata;
      mEntityId = eId;
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<KijiCell<T>> iterator() {
      try {
        return new KijiCellIterator<T>(mColumnName , mRowData, mEntityId);
      } catch (IOException ex) {
        throw new KijiIOException(ex);
      }
    }
  }

  /**
   * Gets the HBase result backing this {@link org.kiji.schema.KijiRowData}.
   *
   * @return The HBase result.
   */
  public Result getHBaseResult() {
    return mResult;
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mEntityId;
  }

  /**
   * Gets the layout of the table this row data belongs to.
   *
   * @return The table layout.
   */
  public KijiTableLayout getTableLayout() {
    return mTableLayout;
  }

  /**
   * Gets a map from kiji family to qualifier to timestamp to raw kiji-encoded bytes of a cell.
   *
   * @return The map.
   */
  public synchronized NavigableMap<String, NavigableMap<String, NavigableMap<Long, byte[]>>>
      getMap() {
    if (null != mFilteredMap) {
      return mFilteredMap;
    }

    LOG.debug("Filtering the HBase Result into a map of kiji cells...");
    final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map =
        mResult.getMap();
    mFilteredMap = new TreeMap<String, NavigableMap<String, NavigableMap<Long, byte[]>>>();
    if (null == map) {
      LOG.debug("No result data.");
      return mFilteredMap;
    }

    final ColumnNameTranslator columnNameTranslator = new ColumnNameTranslator(mTableLayout);
    // Loop over the families in the HTable.
    for (NavigableMap.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyEntry
             : map.entrySet()) {
      // Loop over the columns in the family.
      for (NavigableMap.Entry<byte[], NavigableMap<Long, byte[]>> columnEntry
               : familyEntry.getValue().entrySet()) {
        final HBaseColumnName hbaseColumnName =
            new HBaseColumnName(familyEntry.getKey(), columnEntry.getKey());

        // Translate the HBase column name to a Kiji column name.
        KijiColumnName kijiColumnName;
        try {
          kijiColumnName = columnNameTranslator.toKijiColumnName(
              new HBaseColumnName(familyEntry.getKey(), columnEntry.getKey()));
        } catch (NoSuchColumnException e) {
          LOG.info("Ignoring HBase family " + hbaseColumnName
              + " because it doesn't contain Kiji data.");
          continue;
        }
        LOG.debug("Adding family [{}] to getMap() result.", kijiColumnName.getName());

        // First check if all columns were requested.
        KijiDataRequest.Column columnRequest =
            mDataRequest.getColumn(kijiColumnName.getFamily(), null);
        if (null == columnRequest) {
          // Not all columns were requested, so check if this particular column was.
          columnRequest =
              mDataRequest.getColumn(kijiColumnName.getFamily(), kijiColumnName.getQualifier());
        }
        if (null == columnRequest) {
          LOG.debug("Ignoring unrequested data: " + kijiColumnName.getFamily() + ":"
              + kijiColumnName.getQualifier());
          continue;
        }

        // Loop over the versions.
        int numVersions = 0;
        for (NavigableMap.Entry<Long, byte[]> versionEntry : columnEntry.getValue().entrySet()) {
          if (numVersions >= columnRequest.getMaxVersions()) {
            LOG.debug("Skipping remaining cells because we hit max versions requested: "
                + columnRequest.getMaxVersions());
            break;
          }

          // Read the timestamp.
          final long timestamp = versionEntry.getKey();
          if (mDataRequest.isTimestampInRange(timestamp)) {
            // Add the cell to the filtered map.
            if (!mFilteredMap.containsKey(kijiColumnName.getFamily())) {
              mFilteredMap.put(kijiColumnName.getFamily(),
                  new TreeMap<String, NavigableMap<Long, byte[]>>());
            }
            final NavigableMap<String, NavigableMap<Long, byte[]>> columnMap =
                mFilteredMap.get(kijiColumnName.getFamily());
            if (!columnMap.containsKey(kijiColumnName.getQualifier())) {
              columnMap.put(kijiColumnName.getQualifier(),
                  new TreeMap<Long, byte[]>(TimestampComparator.INSTANCE));
            }
            final NavigableMap<Long, byte[]> versionMap =
                columnMap.get(kijiColumnName.getQualifier());
            versionMap.put(versionEntry.getKey(), versionEntry.getValue());
            ++numVersions;
          } else {
            LOG.debug("Excluding cell at timestamp " + timestamp + " because it is out of range ["
                + mDataRequest.getMinTimestamp() + "," + mDataRequest.getMaxTimestamp() + ")");
          }
        }
      }
    }
    return mFilteredMap;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized boolean containsColumn(String family, String qualifier) {
    final NavigableMap<String, NavigableMap<Long, byte[]>> columnMap = getMap().get(family);
    if (null == columnMap) {
      return false;
    }
   final  NavigableMap<Long, byte[]> versionMap = columnMap.get(qualifier);
    if (null == versionMap) {
      return false;
    }
    return !versionMap.isEmpty();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized boolean containsColumn(String family) {

    final NavigableMap<String, NavigableMap<Long, byte[]>> columnMap = getMap().get(family);
        for (Map.Entry<String, NavigableMap<String, NavigableMap<Long, byte[]>>> columnMapEntry
          : getMap().entrySet()) {
          LOG.debug("The result return contains family [{}]", columnMapEntry.getKey());
        }
    if (null == columnMap) {
      return false;
    }
    return !columnMap.isEmpty();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized boolean containsCell(String family, String qualifier, long timestamp) {
    return containsColumn(family, qualifier)
        && getTimestamps(family, qualifier).contains(timestamp);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized NavigableSet<String> getQualifiers(String family) {
    final NavigableMap<String, NavigableMap<Long, byte[]>> qmap = getRawQualifierMap(family);
    if (null == qmap) {
      return Sets.newTreeSet();
    }
    return qmap.navigableKeySet();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized NavigableSet<Long> getTimestamps(String family, String qualifier) {
    final NavigableMap<Long, byte[]> tmap = getRawTimestampMap(family, qualifier);
    if (null == tmap) {
      return Sets.newTreeSet(TimestampComparator.INSTANCE);
    }
    return tmap.navigableKeySet();
  }

  /** {@inheritDoc} */
  @Override
  public Schema getReaderSchema(String family, String qualifier) throws IOException {
    final Schema schema = mTableLayout.getSchema(new KijiColumnName(family, qualifier));
    if (null == schema) {
      throw new NoSuchColumnException(
          "Cannot retrieve schema for non-existent column: " + family + ":" + qualifier);
    }
    return schema;
  }

  /**
   * Reports the encoded map of qualifiers of a given family.
   *
   * @param family Family to look up.
   * @return the encoded map of qualifiers in the specified family, or null.
   */
  private NavigableMap<String, NavigableMap<Long, byte[]>> getRawQualifierMap(String family) {
    return getMap().get(family);
  }

  /**
   * Reports the specified raw encoded time-series of a given column.
   *
   * @param family Family to look up.
   * @param qualifier Qualifier to look up.
   * @return the encoded time-series in the specified family:qualifier column, or null.
   */
  private NavigableMap<Long, byte[]> getRawTimestampMap(String family, String qualifier) {
    final NavigableMap<String, NavigableMap<Long, byte[]>> qmap = getRawQualifierMap(family);
    if (null == qmap) {
      return null;
    }
    return qmap.get(qualifier);
  }

  /**
   * Reports the encoded content of a given cell.
   *
   * @param family Family to look up.
   * @param qualifier Qualifier to look up.
   * @param timestamp Timestamp to look up.
   * @return the encoded cell content, or null.
   */
  private byte[] getRawCell(String family, String qualifier, long timestamp) {
    final NavigableMap<Long, byte[]> tmap = getRawTimestampMap(family, qualifier);
    if (null == tmap) {
      return null;
    }
    return tmap.get(timestamp);
  }

  /** {@inheritDoc} */
  @Override
  public <T> T getValue(String family, String qualifier, long timestamp) throws IOException {
    final KijiCellDecoder<T> decoder = mDecoderProvider.getDecoder(family, qualifier);
    final byte[] bytes = getRawCell(family, qualifier, timestamp);
    return decoder.decodeValue(bytes);
  }

  /** {@inheritDoc} */
  @Override
  public <T> KijiCell<T> getCell(String family, String qualifier, long timestamp)
      throws IOException {
    final KijiCellDecoder<T> decoder = mDecoderProvider.getDecoder(family, qualifier);
    final byte[] bytes = getRawCell(family, qualifier, timestamp);
    return new KijiCell<T>(family, qualifier, timestamp, decoder.decodeCell(bytes));
  }

  /** {@inheritDoc} */
  @Override
  public <T> T getMostRecentValue(String family, String qualifier) throws IOException {
    final KijiCellDecoder<T> decoder = mDecoderProvider.getDecoder(family, qualifier);
    final NavigableMap<Long, byte[]> tmap = getRawTimestampMap(family, qualifier);
    if (null == tmap) {
      return null;
    }
    final byte[] bytes = tmap.values().iterator().next();
    return decoder.decodeValue(bytes);
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, T> getMostRecentValues(String family) throws IOException {
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "getMostRecentValues(String family) is only enabled"
        + " on map type column families. The column family [%s], is a group type column family."
        + " Please use the getMostRecentValues(String family, String qualifier) method.",
        family);
    final NavigableMap<String, T> result = Maps.newTreeMap();
    for (String qualifier : getQualifiers(family)) {
      final T value = getMostRecentValue(family, qualifier);
      result.put(qualifier, value);
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<Long, T> getValues(String family, String qualifier)
      throws IOException {
    final NavigableMap<Long, T> result = Maps.newTreeMap(TimestampComparator.INSTANCE);
    for (Map.Entry<Long, KijiCell<T>> entry : this.<T>getCells(family, qualifier).entrySet()) {
      result.put(entry.getKey(), entry.getValue().getData());
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, NavigableMap<Long, T>> getValues(String family)
      throws IOException {
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "getValues(String family) is only enabled on map "
        + "type column families. The column family [%s], is a group type column family. Please use "
        + "the getValues(String family, String qualifier) method.",
        family);
    final NavigableMap<String, NavigableMap<Long, T>> result = Maps.newTreeMap();
    for (String qualifier : getQualifiers(family)) {
      final NavigableMap<Long, T> timeseries = getValues(family, qualifier);
      result.put(qualifier, timeseries);
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public <T> KijiCell<T> getMostRecentCell(String family, String qualifier) throws IOException {
    final KijiCellDecoder<T> decoder = mDecoderProvider.getDecoder(family,  qualifier);
    final NavigableMap<Long, byte[]> tmap = getRawTimestampMap(family, qualifier);
    if (null == tmap) {
      return null;
    }
    final byte[] bytes = tmap.values().iterator().next();
    final long timestamp = tmap.firstKey();
    return new KijiCell<T>(family, qualifier, timestamp, decoder.decodeCell(bytes));
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, KijiCell<T>> getMostRecentCells(String family)
      throws IOException {
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "getMostRecentCells(String family) is only enabled"
        + " on map type column families. The column family [%s], is a group type column family."
        + " Please use the getMostRecentCells(String family, String qualifier) method.",
        family);
    final NavigableMap<String, KijiCell<T>> result = Maps.newTreeMap();
    for (String qualifier : getQualifiers(family)) {
      final KijiCell<T> cell = getMostRecentCell(family, qualifier);
      result.put(qualifier, cell);
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<Long, KijiCell<T>> getCells(String family, String qualifier)
      throws IOException {
    final KijiCellDecoder<T> decoder = mDecoderProvider.getDecoder(family,  qualifier);

    final NavigableMap<Long, KijiCell<T>> result = Maps.newTreeMap(TimestampComparator.INSTANCE);
    final NavigableMap<Long, byte[]> tmap = getRawTimestampMap(family, qualifier);
    if (tmap != null) {
      for (Map.Entry<Long, byte[]> entry : tmap.entrySet()) {
        final Long timestamp = entry.getKey();
        final byte[] bytes = entry.getValue();
        final KijiCell<T> cell =
            new KijiCell<T>(family, qualifier, timestamp, decoder.decodeCell(bytes));
        result.put(timestamp, cell);
      }
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, NavigableMap<Long, KijiCell<T>>> getCells(String family)
      throws IOException {
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "getCells(String family) is only enabled"
        + " on map type column families. The column family [%s], is a group type column family."
        + " Please use the getCells(String family, String qualifier) method.",
        family);
    final NavigableMap<String, NavigableMap<Long, KijiCell<T>>> result = Maps.newTreeMap();
    for (String qualifier : getQualifiers(family)) {
      final NavigableMap<Long, KijiCell<T>> cells = getCells(family, qualifier);
      result.put(qualifier, cells);
    }
    return result;
  }


  /**  {@inheritDoc} */
  @Override
  public <T> Iterator<KijiCell<T>> iterator(String family, String qualifier) throws
    IOException {
    return new KijiCellIterator<T>(new KijiColumnName(family, qualifier), this, mEntityId);
  }

  /**  {@inheritDoc} */
  @Override
  public <T> Iterator<KijiCell<T>> iterator(String family) throws
    IOException {
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "iterator(String family) is only enabled"
        + " on map type column families. The column family [%s], is a group type column family."
        + " Please use the iterator(String family, String qualifier) method.",
        family);
    return new KijiCellIterator<T>(new KijiColumnName(family, null), this, mEntityId);
  }

  /**  {@inheritDoc} */
  @Override
  public <T> Iterable<KijiCell<T>> asIterable(String family, String qualifier) {
    return new CellIterable<T>(new KijiColumnName(family, qualifier), this, mEntityId);

  }

  /**  {@inheritDoc} */
  @Override
  public <T> Iterable<KijiCell<T>> asIterable(String family) {
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "asIterable(String family) is only enabled"
        + " on map type column families. The column family [%s], is a group type column family."
        + " Please use the asIterable(String family, String qualifier) method.",
        family);
    return new CellIterable<T>(new KijiColumnName(family, null) , this, mEntityId);
  }


  /** {@inheritDoc} */
  @Override
  public KijiPager getPager(String family, String qualifier)
    throws KijiColumnPagingNotEnabledException {
    final KijiColumnName kijiColumnName = new KijiColumnName(family, qualifier);
    return new HBaseVersionPager(
        mEntityId, mDataRequest, mTable,  kijiColumnName, mDecoderProvider);
  }

  /** {@inheritDoc} */
  @Override
  public KijiPager getPager(String family) throws KijiColumnPagingNotEnabledException {
    final KijiColumnName kijiFamily = new KijiColumnName(family, null);
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "getPager(String family) is only enabled on map type column families. "
        + "The column family '%s' is a group type column family. "
        + "Please use the getPager(String family, String qualifier) method.",
        family);
    return new HBaseMapFamilyPager(mEntityId, mDataRequest, mTable, kijiFamily);
  }
}
