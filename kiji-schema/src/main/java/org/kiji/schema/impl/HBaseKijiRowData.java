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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.KijiCellDecoderFactory;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiPager;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.SpecificCellDecoderFactory;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellSpec;
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

  /** A Kiji cell decoder factory. */
  private final KijiCellDecoderFactory mCellDecoderFactory;

  /** Schema table to resolve schema hashes or IDs. */
  private final KijiSchemaTable mSchemaTable;

  /** A map from kiji family to kiji qualifier to timestamp to raw encoded cell values. */
  private NavigableMap<String, NavigableMap<String, NavigableMap<Long, byte[]>>> mFilteredMap;

  /**
   * Initializes a row data.
   *
   * @param entityId Entity ID of the row.
   * @param request Data request to build the row.
   * @param decoderFactory Factory for cell decoders.
   * @param layout Layout of the table the row belongs to.
   * @param result HTable result with the encoded row content.
   * @param schemaTable Schema table.
   */
  // TODO: Delete after tests are refactored.
  @Deprecated
  public HBaseKijiRowData(
      EntityId entityId,
      KijiDataRequest request,
      KijiCellDecoderFactory decoderFactory,
      KijiTableLayout layout,
      Result result,
      KijiSchemaTable schemaTable) {
    mEntityId = entityId;
    mDataRequest = request;
    mTableLayout = layout;
    mResult = result;
    mCellDecoderFactory = decoderFactory;
    mTable = null;
    mSchemaTable = schemaTable;
  }

  /**
   * Initializes a row data.
   *
   * The entity ID is constructed from the HTable encoded result.
   * This may fail if the table uses hashed row keys.
   *
   * @param request Data request to build the row.
   * @param decoderFactory Factory for cell decoders.
   * @param layout Layout of the table the row belongs to.
   * @param result HTable result with the encoded row content.
   * @param schemaTable Schema table.
   */
  // TODO: Delete after tests are refactored.
  @Deprecated
  public HBaseKijiRowData(
      KijiDataRequest request,
      KijiCellDecoderFactory decoderFactory,
      KijiTableLayout layout,
      Result result,
      KijiSchemaTable schemaTable) {
    this(
        EntityIdFactory.getFactory(layout).getEntityIdFromHBaseRowKey(result.getRow()),
        request,
        decoderFactory,
        layout,
        result,
        schemaTable);
  }

  /**
   * Initializes a row data.
   *
   * @param entityId The entityId of the row.
   * @param request The requested data.
   * @param table The Kiji table that this row belongs to.
   * @param result The HBase result containing the row data.
   * @throws IOException on I/O error.
   */
  public HBaseKijiRowData(
      EntityId entityId,
      KijiDataRequest request,
      HBaseKijiTable table,
      Result result)
      throws IOException {
    mEntityId = entityId;
    mDataRequest = request;
    mTableLayout = table.getLayout();
    mResult = result;
    mCellDecoderFactory = SpecificCellDecoderFactory.get();
    mSchemaTable = table.getKiji().getSchemaTable();
    mTable = table;

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
    return mSchemaTable;
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
    final KijiCellDecoder<T> decoder = getDecoder(family, qualifier);
    final byte[] bytes = getRawCell(family, qualifier, timestamp);
    return decoder.decodeValue(bytes);
  }

  /** {@inheritDoc} */
  @Override
  public <T> KijiCell<T> getCell(String family, String qualifier, long timestamp)
      throws IOException {
    final KijiCellDecoder<T> decoder = getDecoder(family, qualifier);
    final byte[] bytes = getRawCell(family, qualifier, timestamp);
    return new KijiCell<T>(family, qualifier, timestamp, decoder.decodeCell(bytes));
  }

  /** {@inheritDoc} */
  @Override
  public <T> T getMostRecentValue(String family, String qualifier) throws IOException {
    final KijiCellDecoder<T> decoder = getDecoder(family, qualifier);
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
        String.format("getMostRecentValues(String family) is only enabled"
        + " on map type column families. The column family [%s], is a group type column family."
        + " Please use the getMostRecentValues(String family, String qualifier) method.",
        family));
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
        String.format("getValues(String family) is only enabled on map "
        + "type column families. The column family [%s], is a group type column family. Please use "
        + "getValues(String family, String qualifier) method.",
        family));
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
    final KijiCellDecoder<T> decoder = getDecoder(family,  qualifier);
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
        String.format("getMostRecentCells(String family) is only enabled"
        + " on map type column families. The column family [%s], is a group type column family."
        + " Please use the getMostRecentCells(String family, String qualifier) method.",
        family));
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
    final KijiCellDecoder<T> decoder = getDecoder(family,  qualifier);

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
        String.format("getCells(String family) is only enabled"
        + " on map type column families. The column family [%s], is a group type column family."
        + " Please use the getCells(String family, String qualifier) method.",
        family));
    final NavigableMap<String, NavigableMap<Long, KijiCell<T>>> result = Maps.newTreeMap();
    for (String qualifier : getQualifiers(family)) {
      final NavigableMap<Long, KijiCell<T>> cells = getCells(family, qualifier);
      result.put(qualifier, cells);
    }
    return result;
  }

  /**
   * Creates a decoder for the specified column.
   *
   * @param family Name of the column family.
   * @param qualifier Column qualifier.
   * @return a decoder for the specific column.
   * @throws IOException on I/O error.
   *
   * @param <T> type of the value to decode.
   */
  private <T> KijiCellDecoder<T> getDecoder(String family, String qualifier)
      throws IOException {
    // TODO: there is a need for caching decoders, or at least cell specs, as building the cell
    //     spec causes parsing a JSON schema or looking up a class by name.
    final CellSpec cellSpec = mTableLayout.getCellSpec(new KijiColumnName(family, qualifier))
        .setSchemaTable(mSchemaTable);
    return mCellDecoderFactory.create(cellSpec);
  }

  /** {@inheritDoc} */
  @Override
  public KijiPager getPager(String family, String qualifier)
    throws KijiColumnPagingNotEnabledException {
    final KijiColumnName kijiColumnName = new KijiColumnName(family, qualifier);
    return new HBaseKijiPager(mEntityId, mDataRequest, mTableLayout, mTable,  kijiColumnName);
  }

    /** {@inheritDoc} */
  @Override
  public KijiPager getPager(String family) throws KijiColumnPagingNotEnabledException {
        Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        String.format("getPager(String family) is only enabled on map"
        + " type column families. The column family [%s], is a group type column family. Please use"
        + " the getPager(String family, String qualifier) method.",
        family));
    final KijiColumnName kijiFamily = new KijiColumnName(family);
    if (kijiFamily.isFullyQualified()) {
      throw new IllegalArgumentException("Family name (" + family + ") had a colon ':' in it");
    }
    return new HBaseKijiPager(mEntityId, mDataRequest, mTableLayout, mTable, kijiFamily);
  }
}
