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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.HBaseColumnName;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.KijiCellDecoderFactory;
import org.kiji.schema.KijiCellFormat;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiColumnPager;
import org.kiji.schema.KijiCounter;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.NoCellDataException;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.layout.ColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.TimestampComparator;


/**
 * An implementation of KijiRowData that wraps an HBase Result object.
 */
@ApiAudience.Private
public class HBaseKijiRowData extends AbstractKijiRowData {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseKijiRowData.class);

  /** The entity id for the row. */
  private final EntityId mEntityId;

  /** The request used to retrieve this Kiji row data. */
  private final KijiDataRequest mDataRequest;

  /** The layout for the table this row data came from. */
  private final KijiTableLayout mTableLayout;

  /** The HBase result providing the data of this object. */
  private Result mResult;

  /** A Kiji cell decoder factory. */
  private final KijiCellDecoderFactory mCellDecoderFactory;

  /** An optional HTable instance used for fetching the more results from paged columns. */
  private final HTableInterface mHTable;

  /** A column pager (will only be used if paging is enabled, otherwise set to null). */
  private final KijiColumnPager mColumnPager;

  /** A map from kiji family to kiji qualifier to timestamp to raw encoded cell values. */
  private NavigableMap<String, NavigableMap<String, NavigableMap<Long, byte[]>>> mFilteredMap;

  /** Options for constructing an HBaseKijiRowData instance. */
  public static class Options {
    /** The entity the data in this object came from. */
    private EntityId mEntityId;

    /** The data request that populated this KijiRowData. */
    private KijiDataRequest mDataRequest;

    /** The layout of the table the data came from. */
    private KijiTableLayout mTableLayout;

    /** The HBase Result that contains the row data. */
    private Result mResult;

    /** A factory for creating Kiji cell decoders. */
    private KijiCellDecoderFactory mCellDecoderFactory;

    /** An optional HTable instance, required for implementing nextPage() RPCs. */
    private HTableInterface mHTable;

    /**
     * If this is not called, the EntityId is read from the HBase Result. Therefore, if
     * the HBase Result is empty you need to specify the EntityId.
     *
     * @param entityId The entity id.
     * @return This options instance.
     */
    public Options withEntityId(EntityId entityId) {
      mEntityId = entityId;
      return this;
    }

    /**
     * Sets the data request used to fetch the data in this row.
     *
     * @param dataRequest The data request.
     * @return This options instance.
     */
    public Options withDataRequest(KijiDataRequest dataRequest) {
      mDataRequest = dataRequest;
      return this;
    }

    /**
     * Sets the layout of the table this data comes from.
     *
     * @param tableLayout The table layout.
     * @return This options instance.
     */
    public Options withTableLayout(KijiTableLayout tableLayout) {
      mTableLayout = tableLayout;
      return this;
    }

    /**
     * The HBase Result object containing the row data.
     *
     * @param hbaseResult The hbase result.
     * @return This options instance.
     */
    public Options withHBaseResult(Result hbaseResult) {
      mResult = hbaseResult;
      return this;
    }

    /**
     * Sets the cell decoder factory to use.
     *
     * @param factory A cell decoder factory.
     * @return This options instance.
     */
    public Options withCellDecoderFactory(KijiCellDecoderFactory factory) {
      mCellDecoderFactory = factory;
      return this;
    }

    /**
     * Sets the HTable object from which the data came, used to fetch more data if
     * nextPage() is called on a column with paging enabled.
     *
     * @param htable An HTable instance.
     * @return This options instance.
     */
    public Options withHTable(HTableInterface htable) {
      mHTable = htable;
      return this;
    }

    /**
     * Gets the entity id for the row.
     *
     * @return The entity id.
     */
    public EntityId getEntityId() {
      return mEntityId;
    }

    /**
     * Gets the data request.
     *
     * @return The data request.
     */
    public KijiDataRequest getDataRequest() {
      return mDataRequest;
    }

    /**
     * Gets the table layout.
     *
     * @return The table layout.
     */
    public KijiTableLayout getTableLayout() {
      return mTableLayout;
    }

    /**
     * Gets the HBase result.
     *
     * @return The HBase result.
     */
    public Result getHBaseResult() {
      return mResult;
    }

    /**
     * Gets the cell decoder factory.
     *
     * @return The cell decoder factory.
     */
    public KijiCellDecoderFactory getCellDecoderFactory() {
      return mCellDecoderFactory;
    }

    /**
     * Gets the HTable to use for fetching paged data.
     *
     * @return The HTable instance.
     */
    public HTableInterface getHTable() {
      return mHTable;
    }
  }

  /**
   * Constructor.
   *
   * @param options The options for the HBaseKijiRowData instance.
   */
  public HBaseKijiRowData(Options options) {
    EntityId entityId = options.getEntityId();
    if (null == entityId) {
      // Read the entity id from the HBase result if not provided.
      final EntityIdFactory entityIdFactory =
          EntityIdFactory.create(options.getTableLayout().getDesc().getKeysFormat());
      entityId = entityIdFactory.fromHBaseRowKey(options.getHBaseResult().getRow());
    }
    mEntityId = entityId;
    mDataRequest = options.getDataRequest();
    mTableLayout = options.getTableLayout();
    mResult = options.getHBaseResult();
    mCellDecoderFactory = options.getCellDecoderFactory();
    mHTable = options.getHTable();
    mColumnPager = (null != mHTable)
        ? new KijiColumnPager(mEntityId, mDataRequest, mTableLayout, mHTable)
        : null;

    // Compute this lazily.
    mFilteredMap = null;
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
   * Gets the schema table this kiji row data uses for decoding.
   *
   * @return The schema table.
   */
  public KijiSchemaTable getSchemaTable() {
    return mCellDecoderFactory.getSchemaTable();
  }

  /**
   * Merges in the data from another HBaseKijiRowData instance.
   *
   * @param kijiRowData The data to merge in.
   */
  public synchronized void merge(HBaseKijiRowData kijiRowData) {
    merge(kijiRowData.mResult.list());
  }

  /**
   * Merges in the data an HBase Put object.
   *
   * @param put The data to merge in.
   */
  public synchronized void merge(Put put) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Merging in fresh data from put: " + put.toString());
    }
    List<KeyValue> keyValues = new ArrayList<KeyValue>();
    for (List<KeyValue> newKeyValues : put.getFamilyMap().values()) {
      keyValues.addAll(newKeyValues);
    }
    merge(keyValues);
  }

  /**
   * Merges in the data from a collection of KeyValues.
   *
   * @param keyValues The data to merge in.
   */
  public synchronized void merge(Collection<KeyValue> keyValues) {
    // All we have to do is put the KeyValues into the Result.
    List<KeyValue> existingKvs = mResult.list();
    List<KeyValue> merged
        = existingKvs != null ? new ArrayList<KeyValue>(existingKvs) : new ArrayList<KeyValue>();
    merged.addAll(keyValues);
    mResult = new Result(merged);

    // Invalidate the cached filtered map.
    mFilteredMap = null;
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
    if (null == columnMap) {
      return false;
    }
    return !columnMap.isEmpty();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized NavigableSet<String> getQualifiers(String family) {
    final NavigableMap<String, NavigableMap<Long, byte[]>> columnMap = getMap().get(family);
    return null == columnMap ? new TreeSet<String>() : columnMap.navigableKeySet();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized NavigableSet<Long> getTimestamps(String family, String qualifier) {
    final NavigableMap<String, NavigableMap<Long, byte[]>> columnMap = getMap().get(family);
    if (null == columnMap) {
      return new TreeSet<Long>(TimestampComparator.INSTANCE);
    }
    final NavigableMap<Long, byte[]> versionMap = columnMap.get(qualifier);
    if (null == versionMap) {
      return new TreeSet<Long>(TimestampComparator.INSTANCE);
    }
    return versionMap.navigableKeySet();
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
   * Gets the raw kiji-cell-encoded values.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @return Map of the raw bytes for each version of the cell.
   * @throws NoCellDataException If there is no data there.
   */
  private NavigableMap<Long, byte[]> getRawValues(String family, String qualifier)
      throws NoCellDataException {

    final NavigableMap<String, NavigableMap<Long, byte[]>> columnMap = getMap().get(family);
    if (null == columnMap) {
      throw new NoCellDataException("No family: " + family);
    }
    final NavigableMap<Long, byte[]> versionMap = columnMap.get(qualifier);
    if (null == versionMap) {
      throw new NoCellDataException("No column: " + family + ":" + qualifier);
    }
    if (versionMap.isEmpty()) {
      throw new NoCellDataException("No versions: " + family + ":" + qualifier);
    }
    return versionMap;
  }

  /**
   * Gets the latest version of a raw kiji-cell-encoded value.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @return The raw bytes of the cell.
   * @throws NoCellDataException If there is no data there.
   */
  private byte[] getMostRecentRawValue(String family, String qualifier)
      throws NoCellDataException {
    final NavigableMap<Long, byte[]> versionMap = getRawValues(family, qualifier);
    return versionMap.firstEntry().getValue();
  }

  /**
   * Gets the specified version of a raw kiji-cell-encoded value.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param timestamp The version of the cell.
   * @return The raw bytes of the cell.
   * @throws NoCellDataException If there is no data there.
   */
  private byte[] getRawValue(String family, String qualifier, long timestamp)
      throws NoCellDataException {
    final NavigableMap<Long, byte[]> versionMap = getRawValues(family, qualifier);
    final byte[] rawBytes = versionMap.get(timestamp);
    if (null == rawBytes) {
      throw new NoCellDataException(
          "No cell version in " + family + ":" + qualifier + " with " + timestamp);
    }
    return rawBytes;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized <T> KijiCell<T> getCell(String family, String qualifier, Schema readerSchema)
      throws IOException {
    final KijiCellDecoder<T> decoder =
        mCellDecoderFactory.create(readerSchema, getCellFormat(family, qualifier));
    try {
      return decoder.decode(getMostRecentRawValue(family, qualifier));
    } catch (NoCellDataException e) {
      return null;
    }
  }

  /** {@inheritDoc} */
  @Override
  public synchronized <T> KijiCell<T> getCell(String family, String qualifier,
      long timestamp, Schema readerSchema) throws IOException {
    final KijiCellDecoder<T> decoder =
        mCellDecoderFactory.create(readerSchema, getCellFormat(family, qualifier));
    try {
      return decoder.decode(getRawValue(family, qualifier, timestamp));
    } catch (NoCellDataException e) {
      return null;
    }
  }

  /** {@inheritDoc} */
  @Override
  public synchronized <T extends SpecificRecord> KijiCell<T> getCell(
      String family, String qualifier, Class<T> type) throws IOException {
    try {
      return mCellDecoderFactory.create(type, getCellFormat(family, qualifier))
          .decode(getMostRecentRawValue(family, qualifier));
    } catch (NoCellDataException e) {
      return null;
    }
  }

  /** {@inheritDoc} */
  @Override
  public synchronized <T extends SpecificRecord> KijiCell<T> getCell(
      String family, String qualifier, long timestamp, Class<T> type) throws IOException {
    try {
      return mCellDecoderFactory.create(type, getCellFormat(family, qualifier))
          .decode(getRawValue(family, qualifier, timestamp));
    } catch (NoCellDataException e) {
      return null;
    }
  }

  /** {@inheritDoc} */
  @Override
  public synchronized KijiCounter getCounter(String family, String qualifier) throws IOException {
    try {
      final Map.Entry<Long, byte[]> rawValue = getRawValues(family, qualifier).firstEntry();
      return new DefaultKijiCounter(rawValue.getKey(), Bytes.toLong(rawValue.getValue()));
    } catch (NoCellDataException e) {
      return null;
    }
  }

  /** {@inheritDoc} */
  @Override
  public synchronized KijiCounter getCounter(String family, String qualifier, long timestamp)
      throws IOException {
    try {
      final NavigableMap<Long, byte[]> counterValues = getRawValues(family, qualifier);
      final Map.Entry<Long, byte[]> counterEntry = counterValues.floorEntry(timestamp);
      if (null == counterEntry) {
        return null;
      }
      return new DefaultKijiCounter(counterEntry.getKey(), Bytes.toLong(counterEntry.getValue()));
    } catch (NoCellDataException e) {
      return null;
    }
  }

  /** {@inheritDoc} */
  @Override
  public synchronized <T> NavigableMap<String, NavigableMap<Long, T>> getValues(
      String family, Schema readerSchema) throws IOException {
    final NavigableMap<String, NavigableMap<Long, byte[]>> columnMap = getMap().get(family);
    if (null == columnMap) {
      return new TreeMap<String, NavigableMap<Long, T>>();
    }

    final NavigableMap<String, NavigableMap<Long, T>> result =
        new TreeMap<String, NavigableMap<Long, T>>();

    for (Map.Entry<String, NavigableMap<Long, byte[]>> columnEntry : columnMap.entrySet()) {
      final NavigableMap<Long, byte[]> inVersionMap = columnEntry.getValue();
      if (inVersionMap.isEmpty()) {
        // No data here.
        continue;
      }
      final KijiCellDecoder<T> decoder =
          mCellDecoderFactory.create(readerSchema, getCellFormat(family, columnEntry.getKey()));

      final NavigableMap<Long, T> outVersionMap =
          new TreeMap<Long, T>(TimestampComparator.INSTANCE);
      for (Map.Entry<Long, byte[]> inEntry : inVersionMap.entrySet()) {
        final Long ts = inEntry.getKey();
        final byte[] valBytes = inEntry.getValue();
        outVersionMap.put(ts, decoder.decode(valBytes).getData());
      }

      result.put(columnEntry.getKey(), outVersionMap);
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized <T> NavigableMap<String, T> getRecentValues(
      String family, Schema readerSchema) throws IOException {
    final NavigableMap<String, NavigableMap<Long, byte[]>> columnMap = getMap().get(family);
    if (null == columnMap) {
      return new TreeMap<String, T>();
    }
    final NavigableMap<String, T> result = new TreeMap<String, T>();
    for (Map.Entry<String, NavigableMap<Long, byte[]>> columnEntry : columnMap.entrySet()) {
      final NavigableMap<Long, byte[]> versionMap = columnEntry.getValue();
      if (versionMap.isEmpty()) {
        // No data here.
        continue;
      }
      final KijiCellDecoder<T> decoder =
          mCellDecoderFactory.create(readerSchema, getCellFormat(family, columnEntry.getKey()));
      result.put(columnEntry.getKey(),
          decoder.decode(versionMap.firstEntry().getValue()).getData());
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized <T> NavigableMap<Long, T> getValues(
      String family, String qualifier, Schema readerSchema) throws IOException {
    if (!containsColumn(family, qualifier)) {
      return new TreeMap<Long, T>(TimestampComparator.INSTANCE);
    }
    final NavigableMap<Long, byte[]> versionMap = getMap().get(family).get(qualifier);
    assert null != versionMap;
    assert !versionMap.isEmpty();

    final KijiCellDecoder<T> decoder =
        mCellDecoderFactory.create(readerSchema, getCellFormat(family, qualifier));
    final NavigableMap<Long, T> result = new TreeMap<Long, T>(TimestampComparator.INSTANCE);
    for (NavigableMap.Entry<Long, byte[]> versionEntry : versionMap.entrySet()) {
      result.put(versionEntry.getKey(), decoder.decode(versionEntry.getValue()).getData());
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized <T extends SpecificRecord> NavigableMap<String, NavigableMap<Long, T>>
      getValues(String family, Class<T> type) throws IOException {
    final NavigableMap<String, NavigableMap<Long, byte[]>> columnMap = getMap().get(family);

    if (null == columnMap) {
      return new TreeMap<String, NavigableMap<Long, T>>();
    }

    final KijiCellDecoder<T> decoder = mCellDecoderFactory.create(type, getCellFormat(family));
    final NavigableMap<String, NavigableMap<Long, T>> result =
        new TreeMap<String, NavigableMap<Long, T>>();

    for (Map.Entry<String, NavigableMap<Long, byte[]>> columnEntry : columnMap.entrySet()) {
      final NavigableMap<Long, byte[]> inVersionMap = columnEntry.getValue();
      if (inVersionMap.isEmpty()) {
        // No data here.
        continue;
      }

      final NavigableMap<Long, T> outVersionMap =
          new TreeMap<Long, T>(TimestampComparator.INSTANCE);
      for (Map.Entry<Long, byte[]> inEntry : inVersionMap.entrySet()) {
        final Long ts = inEntry.getKey();
        final byte[] valBytes = inEntry.getValue();
        outVersionMap.put(ts, decoder.decode(valBytes).getData());
      }

      result.put(columnEntry.getKey(), outVersionMap);
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized <T extends SpecificRecord> NavigableMap<String, T> getRecentValues(
      String family, Class<T> type) throws IOException {
    final NavigableMap<String, NavigableMap<Long, byte[]>> columnMap = getMap().get(family);
    final NavigableMap<String, T> result = new TreeMap<String, T>();
    if (null == columnMap) {
      return result;
    }
    for (NavigableMap.Entry<String, NavigableMap<Long, byte[]>> columnEntry
             : columnMap.entrySet()) {
      if (columnEntry.getValue().isEmpty()) {
        // No versions of cells.
        continue;
      }
      final KijiCellDecoder<T> decoder =
          mCellDecoderFactory.create(type, getCellFormat(family, columnEntry.getKey()));
      result.put(columnEntry.getKey(),
          decoder.decode(columnEntry.getValue().firstEntry().getValue()).getData());
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized <T extends SpecificRecord> NavigableMap<Long, T> getValues(
      String family, String qualifier, Class<T> type) throws IOException {
    final NavigableMap<Long, T> result = new TreeMap<Long, T>(TimestampComparator.INSTANCE);
    if (!containsColumn(family, qualifier)) {
      return result;
    }

    final NavigableMap<Long, byte[]> versionMap = getMap().get(family).get(qualifier);
    final KijiCellDecoder<T> decoder =
        mCellDecoderFactory.create(type, getCellFormat(family, qualifier));
    for (NavigableMap.Entry<Long, byte[]> versionEntry : versionMap.entrySet()) {
      result.put(versionEntry.getKey(), decoder.decode(versionEntry.getValue()).getData());
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized boolean nextPage(String family, String qualifier) throws IOException {
    if (null == mColumnPager) {
      LOG.warn("Paging is not available in unit tests.\n"
          + "The KijiRowData instance given to your produce/gather() method will contain "
          + "all data regardless of whether a page size was specified in its KijiDataRequest. "
          + "Calling KijiRowData.nextPage() will always return false.");
      return false;
    }

    // Fetch the next page of results.
    final NavigableMap<Long, byte[]> nextPage = mColumnPager.getNextPage(family, qualifier);

    // Clear the current page of results.
    final NavigableMap<String, NavigableMap<Long, byte[]>> qualifierMap = mFilteredMap.get(family);
    qualifierMap.remove(qualifier);

    if (null == nextPage) {
      // No more pages.
      return false;
    }

    // Populate the map with the new page.
    qualifierMap.put(qualifier, nextPage);
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized boolean nextPage(String family) throws IOException {
    if (null == mColumnPager) {
      LOG.warn("Paging is not available in unit tests.\n"
          + "The KijiRowData instance given to your produce/gather() method will contain "
          + "all data regardless of whether a page size was specified in its KijiDataRequest. "
          + "Calling KijiRowData.nextPage() will always return false.");
      return false;
    }

    // Fetch the next page of results.
    final NavigableMap<String, NavigableMap<Long, byte[]>> nextPage =
        mColumnPager.getNextPage(family);

    // Clear the current page of results.
    mFilteredMap.remove(family);
    if (null == nextPage) {
      // No more pages.
      return false;
    }

    // Populate the map with the new page.
    mFilteredMap.put(family, nextPage);
    return true;
  }

  /**
   * Determines the cell encoding format from the column layout.
   *
   * @param family Column family name.
   * @param qualifier Column qualifier name.
   * @return the cell encoding format.
   * @throws NoSuchColumnException if the column does not exist.
   */
  private KijiCellFormat getCellFormat(String family, String qualifier)
      throws NoSuchColumnException {
    return mTableLayout.getCellFormat(new KijiColumnName(family, qualifier));
  }

  /**
   * Determines the cell encoding format from the map-type column layout.
   *
   * @param family Column family name.
   * @return the cell encoding format.
   * @throws NoSuchColumnException if the column does not exist.
   */
  private KijiCellFormat getCellFormat(String family) throws NoSuchColumnException {
    return mTableLayout.getCellFormat(new KijiColumnName(family, null));
  }
}
