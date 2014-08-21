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

package org.kiji.schema.impl.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
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
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTableReaderBuilder;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.impl.BoundColumnReaderSpec;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.platform.SchemaPlatformBridge;
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
  private static CellDecoderProvider createCellProvider(
      final HBaseKijiTable table
  ) throws IOException {
    return CellDecoderProvider.create(
        table.getLayout(),
        ImmutableMap.<KijiColumnName, BoundColumnReaderSpec>of(),
        ImmutableList.<BoundColumnReaderSpec>of(),
        KijiTableReaderBuilder.DEFAULT_CACHE_MISS);
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
      final HBaseKijiTable table,
      final KijiDataRequest dataRequest,
      final EntityId entityId,
      final Result result,
      final CellDecoderProvider decoderProvider
  ) throws IOException {
    mTable = table;
    mTableLayout = table.getLayout();
    mDataRequest = dataRequest;
    mEntityId = entityId;
    mResult = result;
    mDecoderProvider = ((decoderProvider != null) ? decoderProvider : createCellProvider(table))
        .getDecoderProviderForRequest(dataRequest);
  }

  /**
   * An iterator for cells in group type column or map type column family.
   *
   * @param <T> The type parameter for the KijiCells being iterated over.
   */
  private static final class KijiCellIterator<T> implements Iterator<KijiCell<T>> {

    private static final KVComparator KV_COMPARATOR = new KVComparator();

    /**
     * Finds the insertion point of the pivot KeyValue in the KeyValue array and returns the index.
     *
     * @param kvs The KeyValue array to search in.
     * @param pivotKeyValue A KeyValue that is less than or equal to the first KeyValue for our
     *     column, and larger than any KeyValue that may preceed values for our desired column.
     * @return The index of the first KeyValue in the desired map type family.
     */
    private static int findInsertionPoint(final KeyValue[] kvs, final KeyValue pivotKeyValue) {
      // Now find where the pivotKeyValue would be placed
      int binaryResult = Arrays.binarySearch(kvs, pivotKeyValue, KV_COMPARATOR);
      if (binaryResult < 0) {
        return -1 - binaryResult; // Algebra on the formula provided in the binary search JavaDoc.
      } else {
        return binaryResult;
      }
    }

    private final KeyValue[] mKeyValues;
    private final EntityId mEntityId;
    private final KijiColumnName mColumn;
    private final KijiCellDecoder<T> mDecoder;
    private final HBaseColumnNameTranslator mColumnNameTranslator;
    private final int mMaxVersions;
    private int mCurrentVersions = 0;
    private int mNextIndex = 0;
    private KijiCell<T> mNextCell = null;

    /**
     * Create a new KijiCellIterator.
     *
     * @param rowData KijiRowData from which to retrieve cells.
     * @param columnName Column across which to iterate. May be a fully qualified column or map type
     *     family.
     * @param columnNameTranslator HBaseColumnNameTranslator with which to decode KeyValues.
     * @throws IOException In case of an error initializing the Iterator.
     */
    private KijiCellIterator(
        final HBaseKijiRowData rowData,
        final KijiColumnName columnName,
        final HBaseColumnNameTranslator columnNameTranslator
    ) throws IOException {
      mKeyValues = SchemaPlatformBridge.get().keyValuesFromResult(rowData.mResult);
      mEntityId = rowData.mEntityId;
      mColumn = columnName;
      mDecoder = rowData.mDecoderProvider.getDecoder(mColumn);
      mColumnNameTranslator = columnNameTranslator;
      mMaxVersions = rowData.mDataRequest.getRequestForColumn(mColumn).getMaxVersions();

      mNextIndex = findStartIndex();
      mNextCell = getNextCell();
    }

    /**
     * Find the start index of the configured column in the KeyValues of this Iterator.
     *
     * @return the start index of the configured column in the KeyValues of this Iterator.
     * @throws NoSuchColumnException in case the column does not exist in the table.
     */
    private int findStartIndex() throws NoSuchColumnException {
      final HBaseColumnName hBaseColumnName = mColumnNameTranslator.toHBaseColumnName(mColumn);
      final KeyValue kv = new KeyValue(
          mEntityId.getHBaseRowKey(),
          hBaseColumnName.getFamily(),
          hBaseColumnName.getQualifier(),
          Long.MAX_VALUE,
          new byte[0]);
      return findInsertionPoint(mKeyValues, kv);
    }

    /**
     * Get the next cell to be returned by this Iterator. Null indicates the iterator is exhausted.
     *
     * @return the next cell to be returned by this Iterator or null if the iterator is exhausted.
     *
     * @throws IOException in case of an error decoding the cell.
     */
    private KijiCell<T> getNextCell() throws IOException {
      // Ensure that we do not attempt to get KeyValues from out of bounds indices.
      if (mNextIndex >= mKeyValues.length) {
        return null;
      }

      final KeyValue next = mKeyValues[mNextIndex];
      final HBaseColumnName hbaseColumn = new HBaseColumnName(
          next.getFamily(),
          next.getQualifier());
      final KijiColumnName column = mColumnNameTranslator.toKijiColumnName(hbaseColumn);

      // Validates that the column of the next KeyValue should be included in the iterator.
      if (mColumn.isFullyQualified()) {
        if (!Objects.equal(mColumn, column)) {
          // The column of the next cell is not the requested column, do not return it.
          return null;
        }
      } else {
        if (!Objects.equal(column.getFamily(), mColumn.getFamily())) {
          // The column of the next cell is not in the requested family, do not return it.
          return null;
        }
      }

      if ((null != mNextCell) && !Objects.equal(mNextCell.getColumn(), column)) {
        // We've hit the next qualifier before the max versions; reset the current version count.
        mCurrentVersions = 0;
      }

      if (mCurrentVersions < mMaxVersions) {
        // decode the cell and return it.
        mCurrentVersions++;
        mNextIndex++;
        return KijiCell.create(column, next.getTimestamp(), mDecoder.decodeCell(next.getValue()));
      } else {
        // Reset the current versions and try the next qualifier.
        mCurrentVersions = 0;
        final KeyValue nextQualifierKV = new KeyValue(
            mEntityId.getHBaseRowKey(),
            hbaseColumn.getFamily(),
            Arrays.copyOf(hbaseColumn.getQualifier(), hbaseColumn.getQualifier().length + 1),
            Long.MAX_VALUE,
            new byte[0]);
        mNextIndex = findInsertionPoint(mKeyValues, nextQualifierKV);
        return getNextCell();
      }
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
      return (null != mNextCell);
    }

    /** {@inheritDoc} */
    @Override
    public KijiCell<T> next() {
      final KijiCell<T> next = mNextCell;
      if (null == next) {
        throw new NoSuchElementException();
      } else {
        try {
          mNextCell = getNextCell();
        } catch (IOException ioe) {
          throw new KijiIOException(ioe);
        }
        return next;
      }
    }

    /** {@inheritDoc} */
    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          String.format("%s does not support remove().", getClass().getName()));
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
    /** The HBaseColumnNameTranslator for the column. */
    private final HBaseColumnNameTranslator mHBaseColumnNameTranslator;

    /**
     * An iterable of KijiCells, for a particular column.
     *
     * @param colName The Kiji column family that is being iterated over.
     * @param rowdata The HBaseKijiRowData instance containing the desired data.
     * @param columnNameTranslator of the table we are iterating over.
     */
    protected CellIterable(
        final KijiColumnName colName,
        final HBaseKijiRowData rowdata,
        final HBaseColumnNameTranslator columnNameTranslator
    ) {
      mColumnName = colName;
      mRowData = rowdata;
      mHBaseColumnNameTranslator = columnNameTranslator;
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<KijiCell<T>> iterator() {
      try {
        return new KijiCellIterator<T>(
            mRowData,
            mColumnName,
            mHBaseColumnNameTranslator);
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
   * Gets the table this row data belongs to.
   *
   * @return the table this row data belongs to.
   */
  public HBaseKijiTable getTable() {
    return mTable;
  }

  /**
   * Gets the data request used to retrieve this row data.
   *
   * @return the data request used to retrieve this row data.
   */
  public KijiDataRequest getDataRequest() {
    return mDataRequest;
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

    final HBaseColumnNameTranslator columnNameTranslator =
        HBaseColumnNameTranslator.from(mTableLayout);
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
          LOG.info("Ignoring HBase column '{}:{}' because it doesn't contain Kiji data.",
              Bytes.toStringBinary(hbaseColumnName.getFamily()),
              Bytes.toStringBinary(hbaseColumnName.getQualifier()));
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
  public synchronized boolean containsColumn(final String family, final String qualifier) {
    final NavigableMap<String, NavigableMap<Long, byte[]>> columnMap = getMap().get(family);
    if (null == columnMap) {
      return false;
    }
    final NavigableMap<Long, byte[]> versionMap = columnMap.get(qualifier);
    if (null == versionMap) {
      return false;
    }
    return !versionMap.isEmpty();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized boolean containsColumn(final String family) {

    final NavigableMap<String, NavigableMap<Long, byte[]>> columnMap = getMap().get(family);
    if (null == columnMap) {
      return false;
    }
    for (Map.Entry<String, NavigableMap<String, NavigableMap<Long, byte[]>>> columnMapEntry
      : getMap().entrySet()) {
      LOG.debug("The result return contains family [{}]", columnMapEntry.getKey());
    }
    return !columnMap.isEmpty();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized boolean containsCell(
      final String family,
      final String qualifier,
      long timestamp
  ) {
    return containsColumn(family, qualifier)
        && getTimestamps(family, qualifier).contains(timestamp);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized NavigableSet<String> getQualifiers(final String family) {
    final NavigableMap<String, NavigableMap<Long, byte[]>> qmap = getRawQualifierMap(family);
    if (null == qmap) {
      return Sets.newTreeSet();
    }
    return qmap.navigableKeySet();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized NavigableSet<Long> getTimestamps(
      final String family,
      final String qualifier
  ) {
    final NavigableMap<Long, byte[]> tmap = getRawTimestampMap(family, qualifier);
    if (null == tmap) {
      return Sets.newTreeSet(TimestampComparator.INSTANCE);
    }
    return tmap.navigableKeySet();
  }

  /** {@inheritDoc} */
  @Override
  public Schema getReaderSchema(final String family, final String qualifier) throws IOException {
    return mTableLayout.getCellSpec(KijiColumnName.create(family, qualifier)).getAvroSchema();
  }

  /**
   * Reports the encoded map of qualifiers of a given family.
   *
   * @param family Family to look up.
   * @return the encoded map of qualifiers in the specified family, or null.
   */
  private NavigableMap<String, NavigableMap<Long, byte[]>> getRawQualifierMap(final String family) {
    return getMap().get(family);
  }

  /**
   * Reports the specified raw encoded time-series of a given column.
   *
   * @param family Family to look up.
   * @param qualifier Qualifier to look up.
   * @return the encoded time-series in the specified family:qualifier column, or null.
   */
  private NavigableMap<Long, byte[]> getRawTimestampMap(
      final String family,
      final String qualifier
  ) {
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
    final KijiCellDecoder<T> decoder =
        mDecoderProvider.getDecoder(KijiColumnName.create(family, qualifier));
    final byte[] bytes = getRawCell(family, qualifier, timestamp);
    return decoder.decodeValue(bytes);
  }

  /** {@inheritDoc} */
  @Override
  public <T> KijiCell<T> getCell(String family, String qualifier, long timestamp)
      throws IOException {
    final KijiCellDecoder<T> decoder =
        mDecoderProvider.getDecoder(KijiColumnName.create(family, qualifier));
    final byte[] bytes = getRawCell(family, qualifier, timestamp);
    return KijiCell.create(
        KijiColumnName.create(family, qualifier),
        timestamp,
        decoder.decodeCell(bytes));
  }

  /** {@inheritDoc} */
  @Override
  public <T> T getMostRecentValue(String family, String qualifier) throws IOException {
    final KijiCellDecoder<T> decoder =
        mDecoderProvider.getDecoder(KijiColumnName.create(family, qualifier));
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
    final KijiCellDecoder<T> decoder =
        mDecoderProvider.getDecoder(KijiColumnName.create(family, qualifier));
    final NavigableMap<Long, byte[]> tmap = getRawTimestampMap(family, qualifier);
    if (null == tmap) {
      return null;
    }
    final byte[] bytes = tmap.values().iterator().next();
    final long timestamp = tmap.firstKey();
    return KijiCell.create(
        KijiColumnName.create(family, qualifier),
        timestamp,
        decoder.decodeCell(bytes));
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
    final KijiCellDecoder<T> decoder =
        mDecoderProvider.getDecoder(KijiColumnName.create(family, qualifier));

    final NavigableMap<Long, KijiCell<T>> result = Maps.newTreeMap(TimestampComparator.INSTANCE);
    final NavigableMap<Long, byte[]> tmap = getRawTimestampMap(family, qualifier);
    if (tmap != null) {
      for (Map.Entry<Long, byte[]> entry : tmap.entrySet()) {
        final Long timestamp = entry.getKey();
        final byte[] bytes = entry.getValue();
        final KijiCell<T> cell =
            KijiCell.create(
                KijiColumnName.create(family, qualifier),
                timestamp,
                decoder.decodeCell(bytes));
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

  /** {@inheritDoc} */
  @Override
  public <T> Iterator<KijiCell<T>> iterator(String family, String qualifier)
      throws IOException {
    final KijiColumnName column = KijiColumnName.create(family, qualifier);
    Preconditions.checkArgument(
        mDataRequest.getRequestForColumn(column) != null,
        "Column %s has no data request.", column);
    return new KijiCellIterator<T>(this, column, mTable.getColumnNameTranslator());
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterator<KijiCell<T>> iterator(String family)
      throws IOException {
    final KijiColumnName column = KijiColumnName.create(family, null);
    Preconditions.checkArgument(
        mDataRequest.getRequestForColumn(column) != null,
        "Column %s has no data request.", column);
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "iterator(String family) is only enabled"
            + " on map type column families. The column family [%s], is a group type column family."
            + " Please use the iterator(String family, String qualifier) method.",
        family);
    return new KijiCellIterator<T>(this, column, mTable.getColumnNameTranslator());
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterable<KijiCell<T>> asIterable(String family, String qualifier) {
    final KijiColumnName column = KijiColumnName.create(family, qualifier);
    Preconditions.checkArgument(
        mDataRequest.getRequestForColumn(column) != null,
        "Column %s has no data request.", column);
    return new CellIterable<T>(column, this, mTable.getColumnNameTranslator());
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterable<KijiCell<T>> asIterable(String family) {
    final KijiColumnName column = KijiColumnName.create(family, null);
    Preconditions.checkArgument(
        mDataRequest.getRequestForColumn(column) != null,
        "Column %s has no data request.", column);
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "asIterable(String family) is only enabled"
            + " on map type column families. The column family [%s], is a group type column family."
            + " Please use the asIterable(String family, String qualifier) method.",
        family);
    return new CellIterable<T>(column, this, mTable.getColumnNameTranslator());
  }

  /** {@inheritDoc} */
  @Override
  public KijiPager getPager(String family, String qualifier)
      throws KijiColumnPagingNotEnabledException {
    final KijiColumnName kijiColumnName = KijiColumnName.create(family, qualifier);
    return new HBaseVersionPager(
        mEntityId, mDataRequest, mTable,  kijiColumnName, mDecoderProvider);
  }

  /** {@inheritDoc} */
  @Override
  public KijiPager getPager(String family) throws KijiColumnPagingNotEnabledException {
    final KijiColumnName kijiFamily = KijiColumnName.create(family, null);
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "getPager(String family) is only enabled on map type column families. "
        + "The column family '%s' is a group type column family. "
        + "Please use the getPager(String family, String qualifier) method.",
        family);
    return new HBaseMapFamilyPager(mEntityId, mDataRequest, mTable, kijiFamily);
  }

  /**
   * Get a KijiResult corresponding to the same data as this KjiRowData.
   *
   * <p>
   *   This method allows the caller to specify a type-bound on the values of the {@code KijiCell}s
   *   of the returned {@code KijiResult}. The caller should be careful to only specify an
   *   appropriate type. If the type is too specific (or wrong), a runtime
   *   {@link java.lang.ClassCastException} will be thrown when the returned {@code KijiResult} is
   *   used. See the 'Type Safety' section of {@link KijiResult}'s documentation for more details.
   * </p>
   *
   * @return A KijiResult corresponding to the same data as this KijiRowData.
   * @param <T> The type of {@code KijiCell} values in the returned {@code KijiResult}.
   * @throws IOException if error while decoding cells.
   */
  public <T> KijiResult<T> asKijiResult() throws IOException {
    return HBaseKijiResult.create(
        mEntityId,
        mDataRequest,
        mResult,
        mTable,
        mTableLayout,
        HBaseColumnNameTranslator.from(mTableLayout),
        mDecoderProvider);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(HBaseKijiRowData.class)
        .add("table", mTable.getURI())
        .add("entityId", getEntityId())
        .add("dataRequest", mDataRequest)
        .add("resultSize", mResult.size())
        .add("result", mResult)
        .add("map", getMap())
        .toString();
  }
}
