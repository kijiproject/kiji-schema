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

package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.cassandra.utils.ByteBufferUtil;
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
import org.kiji.schema.KijiTableReaderBuilder;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.cassandra.CassandraColumnName;
import org.kiji.schema.impl.BoundColumnReaderSpec;
import org.kiji.schema.layout.ColumnReaderSpec;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.layout.impl.cassandra.ShortColumnNameTranslator;
import org.kiji.schema.util.TimestampComparator;

/**
 * An implementation of KijiRowData that wraps an Cassandra Result object.
 */
@ApiAudience.Private
public final class CassandraKijiRowData implements KijiRowData {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiRowData.class);

  /** The entity id for the row. */
  private final EntityId mEntityId;

  /** The request used to retrieve this Kiji row data. */
  private final KijiDataRequest mDataRequest;

  /** The Cassandra KijiTable we are reading from. */
  private final CassandraKijiTable mTable;

  /** The layout for the table this row data came from. */
  private final KijiTableLayout mTableLayout;

  /** The Cassandra result providing the data of this object. */
  private Collection<Row> mRows;

  /** Provider for cell decoders. */
  private final CellDecoderProvider mDecoderProvider;

  /** A map from kiji family to kiji qualifier to timestamp to raw encoded cell values. */
  private NavigableMap<String, NavigableMap<String, NavigableMap<Long, byte[]>>> mFilteredMap;

  /**
   * Creates a provider for cell decoders.
   *
   * <p> The provider creates decoders for specific Avro records. </p>
   *
   * @param table Cassandra KijiTable to create a CellDecoderProvider for.
   * @return a new CellDecoderProvider for the specified Cassandra KijiTable.
   * @throws java.io.IOException on I/O error.
   */
  private static CellDecoderProvider createCellProvider(
      CassandraKijiTable table) throws IOException {
    return new CellDecoderProvider(
        table.getLayout(),
        Maps.<KijiColumnName, BoundColumnReaderSpec>newHashMap(),
        Sets.<BoundColumnReaderSpec>newHashSet(),
        KijiTableReaderBuilder.DEFAULT_CACHE_MISS);
  }

  /**
   * Initializes a row data from an Cassandra Result.
   *
   * <p>
   *   The Cassandra Result may contain more cells than are requested by the user.
   *   KijiDataRequest is more expressive than Cassandra Get/Scan requests.
   *   Currently, {@link #getMap()} attempts to complete the filtering to meet the data request
   *   requirements expressed by the user, but this can be inaccurate.
   * </p>
   *
   * @param table Kiji table containing this row.
   * @param dataRequest Data requested for this row.
   * @param entityId This row entity ID.
   * @param rows Cassandra result containing the requested cells (and potentially more).
   * @param decoderProvider Provider for cell decoders.
   *     Null means the row creates its own provider for cell decoders (not recommended).
   * @throws java.io.IOException on I/O error.
   */
  public CassandraKijiRowData(
      CassandraKijiTable table,
      KijiDataRequest dataRequest,
      EntityId entityId,
      Collection<Row> rows,
      CellDecoderProvider decoderProvider)
      throws IOException {
    mTable = table;
    mTableLayout = table.getLayout();
    mDataRequest = dataRequest;
    mEntityId = entityId;
    mRows = rows;
    mDecoderProvider = (decoderProvider != null) ? decoderProvider : createCellProvider(table);
  }

  /**
   * Package-private constructor, useful for creating a KijiRowData from a previously-created map.
   *
   * @param table The table to which the row data belongs.
   * @param dataRequest The data request from which the row data was derived.
   * @param entityId The entity ID for this row.
   * @param map of family to qualifier to version to value.
   * @param decoderProvider for this row data.
   * @throws java.io.IOException if there is a problem creating the row data.
   */
  CassandraKijiRowData(
      CassandraKijiTable table,
      KijiDataRequest dataRequest,
      EntityId entityId,
      NavigableMap<String, NavigableMap<String, NavigableMap<Long, byte[]>>> map,
      CellDecoderProvider decoderProvider) throws IOException {
    mTable = table;
    mTableLayout = table.getLayout();
    mDataRequest = dataRequest;
    mEntityId = entityId;
    mRows = null;
    mFilteredMap = map;
    mDecoderProvider = (decoderProvider != null) ? decoderProvider : createCellProvider(table);
  }

  /**
   * Get the decoder for the given column from the {@link
   * org.kiji.schema.layout.impl.CellDecoderProvider}.
   *
   * @param column column for which to get a cell decoder.
   * @param <T> the type of the value encoded in the cell.
   * @return a cell decoder which can read the given column.
   * @throws java.io.IOException in case of an error getting the cell decoder.
   */
  private <T> KijiCellDecoder<T> getDecoder(KijiColumnName column) throws IOException {
    final KijiDataRequest.Column requestColumn = mDataRequest.getRequestForColumn(column);
    if (null != requestColumn) {
      final ColumnReaderSpec spec = requestColumn.getReaderSpec();
      if (null != spec) {
        // If there is a spec override in the data request, use it to get the decoder.
        return mDecoderProvider.getDecoder(BoundColumnReaderSpec.create(spec, column));
      }
    }
    // If the column is not in the request, or there is no spec override, get the decoder for the
    // column by name.
    return mDecoderProvider.getDecoder(column.getFamily(), column.getQualifier());
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mEntityId;
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

    // TODO: Add only cell values that are valid with respect to the data request (filters, etc.)

    LOG.info(String.format(
        "Filtering the Cassandra results into a map of kiji cells.  There are a total of %d rows",
        mRows.size()));

    mFilteredMap = new TreeMap<String, NavigableMap<String, NavigableMap<Long, byte[]>>>();

    // Need to translate from short names in Cassandra table into longer names in Kiji table.
    final ShortColumnNameTranslator columnNameTranslator =
        new ShortColumnNameTranslator(mTableLayout);

    // Go through every column in the result set and add the data to the filtered map.
    for (Row row : mRows) {

      LOG.info("Reading a row back...");

      // Get the Cassandra key (entity Id), qualifier, timestamp, and value.
      Long timestamp = row.getLong(CQLUtils.VERSION_COL);

      final CassandraColumnName cassandraColumn =
          new CassandraColumnName(
              row.getString(CQLUtils.LOCALITY_GROUP_COL),
              row.getBytes(CQLUtils.FAMILY_COL),
              row.getBytes(CQLUtils.QUALIFIER_COL));

      KijiColumnName kijiColumnName;
      try {
        kijiColumnName = columnNameTranslator.toKijiColumnName(cassandraColumn);

      } catch (NoSuchColumnException e) {
        LOG.info(String.format("Ignoring Cassandra column %s because it doesn't contain Kiji data.",
                cassandraColumn));
        continue;
      }

      String family = kijiColumnName.getFamily();
      String qualifier = kijiColumnName.getQualifier();


      LOG.info(String.format(
          "Got back data from table for family:qualifier %s:%s, timestamp %s",
          family,
          qualifier,
          timestamp
      ));

      // Most values will not be from counters, so we use getBytes.
      ByteBuffer value;
      try {
        value = row.getBytes(CQLUtils.VALUE_COL);
      } catch (InvalidTypeException e) {
        if (e.getMessage().equals("Column value is of type counter")) {
          long counter =  row.getLong(CQLUtils.VALUE_COL);
          value = ByteBufferUtil.bytes(counter);
        } else {
          throw new KijiIOException(e);
        }
      }

      checkDataRequestAndInsertValueIntoMap(family, qualifier, timestamp, value);
    }

    updateMapForMaxValues();

    return mFilteredMap;
  }

  /**
   * Insert this value into the map if it is valid with respect to the data request.
   *
   * @param family of the value to insert.
   * @param qualifier of the value to insert.
   * @param timestamp of the value to insert.
   * @param value to insert.
   */
  private void checkDataRequestAndInsertValueIntoMap(
      String family,
      String qualifier,
      Long timestamp,
      ByteBuffer value
  ) {
    LOG.info(String.format(
        "Considering whether to add (%s, %s, %s, -)...",
        family,
        qualifier,
        timestamp));

    // Check whether there is a request for this column or for this column family.
    KijiDataRequest.Column columnRequest = mDataRequest.getRequestForColumn(family, qualifier);

    // This column is not part of the data request, do not add it to the map.
    if (null == columnRequest) {
      return;
    }

    // TODO: Do something with the filter for this data request.
    //KijiColumnFilter filter = columnRequest.getFilter();

    long maxTimestamp = mDataRequest.getMaxTimestamp();
    if (timestamp < mDataRequest.getMinTimestamp() || timestamp >= maxTimestamp) {
      return;
    }

    // Get a reference to the map for the family.
    NavigableMap<String, NavigableMap<Long, byte[]>> familyMap;
    if (mFilteredMap.containsKey(family)) {
      familyMap = mFilteredMap.get(family);
    } else {
      familyMap = new TreeMap<String, NavigableMap<Long, byte[]>>();
      mFilteredMap.put(family, familyMap);
    }

    // Get a reference to the map for the qualifier.
    NavigableMap<Long, byte[]> qualifierMap;
    if (familyMap.containsKey(qualifier)) {
      qualifierMap = familyMap.get(qualifier);
    } else {
      // Need to use TimestampComparator here to sort timestamps such that lowest timestamp is
      // first in order.
      qualifierMap =  new TreeMap<Long, byte[]>(TimestampComparator.INSTANCE);
      familyMap.put(qualifier, qualifierMap);
    }

    // Should not already have an entry for this timestamp!
    assert(!qualifierMap.containsKey(timestamp));

    // Insert the data into the map if we have not already exceeded the maximum number of
    // versions.  Also check that the timestamp for the current cell we are considering is older
    // than any of the versions currently in the map.

    // TODO: Guarantee order of data here is same as C* SELECT and do max-versions filtering here.
    // (Instead of at the end of building the entire map.)
    /*
    // Already exceeded the maximum number of versions for this column.
    int maxVersions = columnRequest.getMaxVersions();
    LOG.info(String.format(
        "...max versions = %d, already have %d values.",
        maxVersions,
        qualifierMap.size()));

    // Sanity check that this timestamp is older than any we have seen so far.
    // Recall that we are using a comparator for timestamps that sorts timestamps such that the
    // highest (most recent) values come first.
    assert(null == qualifierMap.ceilingKey(timestamp)) :
        "Should be getting data back in reverse-timestamp order. " +
        "Currently looking at datum with timestamp " + timestamp + ", but there exists a datum " +
        "already with timestamp " + qualifierMap.ceilingKey(timestamp) + "!";

    if (qualifierMap.size() >= maxVersions) {
      LOG.info("...Already have max versions, not adding.");
      return;
    }
    */

    LOG.info("...Adding the cell!");
    // Finally insert the data into the map!
    qualifierMap.put(timestamp, CassandraByteUtil.byteBuffertoBytes(value));
  }

  /**
   * Delete values from the internal map based on the max values constraints for various columns.
   */
  private void updateMapForMaxValues() {
    for (String family : mFilteredMap.keySet()) {

      NavigableMap<String, NavigableMap<Long, byte[]>> familyMap = mFilteredMap.get(family);

      for (String qualifier : familyMap.keySet()) {

        // Check whether there is a request for this column or for this column family.
        KijiDataRequest.Column columnRequest = mDataRequest.getRequestForColumn(family, qualifier);
        // If this column request does not exist, then there shouldn't be any data present in the
        // map because we check for this earlier.
        assert(columnRequest != null);
        int maxVersions = columnRequest.getMaxVersions();

        NavigableMap<Long, byte[]> columnMap = familyMap.get(qualifier);

        if (maxVersions >= columnMap.size()) {
          continue;
        }

        // TODO: Optimize this code.

        // Trim the size of the map...
        int versionCount = 0;
        long firstKeyToDrop = -1;

        for (Long key: columnMap.keySet()) {
          if (versionCount >= maxVersions) {
            // This is the first key that is too old to include here.
            firstKeyToDrop = key;
            break;
          }
          versionCount++;
        }

        NavigableMap<Long, byte[]> trimmedColumnMap = columnMap.headMap(firstKeyToDrop, false);
        assert (maxVersions == trimmedColumnMap.size());

        familyMap.put(qualifier, trimmedColumnMap);
      }
    }
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
    for (Map.Entry<String, NavigableMap<String, NavigableMap<Long, byte[]>>> columnMapEntry
      : getMap().entrySet()) {
      LOG.debug("The result return contains family [{}]", columnMapEntry.getKey());
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
    return mTableLayout.getCellSpec(new KijiColumnName(family, qualifier)).getAvroSchema();
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
    final KijiCellDecoder<T> decoder = getDecoder(new KijiColumnName(family, qualifier));
    final byte[] bytes = getRawCell(family, qualifier, timestamp);
    if (null == bytes) {
      return null;
    }
    return decoder.decodeValue(bytes);
  }

  /** {@inheritDoc} */
  @Override
  public <T> KijiCell<T> getCell(String family, String qualifier, long timestamp)
      throws IOException {
    final KijiCellDecoder<T> decoder = getDecoder(new KijiColumnName(family, qualifier));
    final byte[] bytes = getRawCell(family, qualifier, timestamp);
    return new KijiCell<T>(family, qualifier, timestamp, decoder.decodeCell(bytes));
  }

  /** {@inheritDoc} */
  @Override
  public <T> T getMostRecentValue(String family, String qualifier) throws IOException {
    final KijiCellDecoder<T> decoder = getDecoder(new KijiColumnName(family, qualifier));
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
    final KijiCellDecoder<T> decoder = getDecoder(new KijiColumnName(family, qualifier));
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
    final KijiCellDecoder<T> decoder = getDecoder(new KijiColumnName(family, qualifier));

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

  /** {@inheritDoc} */
  @Override
  public <T> Iterator<KijiCell<T>> iterator(String family, String qualifier)
      throws IOException {
    final KijiColumnName column = new KijiColumnName(family, qualifier);
    Preconditions.checkArgument(
        mDataRequest.getRequestForColumn(column) != null,
        "Column %s has no data request.", column);
    return new KijiCellIterator<T>(column, this, mEntityId);
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterator<KijiCell<T>> iterator(String family)
      throws IOException {
    final KijiColumnName column = new KijiColumnName(family, null);
    Preconditions.checkArgument(
        mDataRequest.getRequestForColumn(column) != null,
        "Column %s has no data request.", column);
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "iterator(String family) is only enabled"
        + " on map type column families. The column family [%s], is a group type column family."
        + " Please use the iterator(String family, String qualifier) method.",
        family);
    return new KijiCellIterator<T>(column, this, mEntityId);
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterable<KijiCell<T>> asIterable(String family, String qualifier) {
    final KijiColumnName column = new KijiColumnName(family, qualifier);
    Preconditions.checkArgument(
        mDataRequest.getRequestForColumn(column) != null,
        "Column %s has no data request.", column);
    return new CellIterable<T>(column, this, mEntityId);
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterable<KijiCell<T>> asIterable(String family) {
    final KijiColumnName column = new KijiColumnName(family, null);
    Preconditions.checkArgument(
        mDataRequest.getRequestForColumn(column) != null,
        "Column %s has no data request.", column);
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "asIterable(String family) is only enabled"
        + " on map type column families. The column family [%s], is a group type column family."
        + " Please use the asIterable(String family, String qualifier) method.",
        family);
    return new CellIterable<T>(column, this, mEntityId);
  }

  /** {@inheritDoc} */
  @Override
  public KijiPager getPager(String family, String qualifier)
      throws KijiColumnPagingNotEnabledException {
    final KijiColumnName kijiColumnName = new KijiColumnName(family, qualifier);
    return new CassandraVersionPager(
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
    return new CassandraMapFamilyPager(mEntityId, mDataRequest, mTable, kijiFamily);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(CassandraKijiRowData.class)
        .add("table", mTable.getURI())
        .add("entityId", getEntityId())
        .add("dataRequest", mDataRequest)
        //.add("resultSize", mResult.size())
        //.add("result", mResult)
        .add("map", getMap())
        .toString();
  }

  /**
   * An iterator for cells in group type column or map type column family.
   *
   * @param <T> The type parameter for the KijiCells being iterated over.
   */
  private static final class KijiCellIterator<T> implements Iterator<KijiCell<T>> {
    // This is a much less-sophisticated (and probably less-optimal) implementation of this
    // interface than that present in HBaseKijiRowData.

    /** The maximum number of versions requested. */
    //private final int mMaxVersions;

    /** An iterator over all of the columns. */
    private Iterator<Map.Entry<String, NavigableMap<Long, byte[]>>> mQualifierIterator;

    /** An iterator over values in the current qualified column. */
    private Iterator<Map.Entry<Long, byte[]>> mVersionIterator;

    private String mCurrentQualifier;

    /** The column or map type family being iterated over. */
    private final KijiColumnName mColumn;

    /** The number of versions returned by the iterator so far. */
    private int mNumVersions;

    /** The cell decoder for this column. */
    private final KijiCellDecoder<T> mDecoder;

    /** The next cell to return. */
    private KijiCell<T> mNextCell;

    /**
     * An iterator of KijiCells, for a particular column.
     *
     * @param columnName The Kiji column that is being iterated over.
     * @param rowdata The CassandraKijiRowData instance containing the desired data.
     * @param eId of the rowdata we are iterating over.
     * @throws java.io.IOException on I/O error
     */
    protected KijiCellIterator(
        KijiColumnName columnName,
        CassandraKijiRowData rowdata,
        EntityId eId) throws IOException {
      mColumn = columnName;
      // Get info about the data request for this column.
      //KijiDataRequest.Column columnRequest = rowdata.mDataRequest.getRequestForColumn(mColumn);
      //mMaxVersions = columnRequest.getMaxVersions();
      mNumVersions = 0;
      mDecoder = rowdata.getDecoder(columnName);

      mNextCell = null;

      LOG.info("------ Creating KijiCellIterator ------");

      // Initialize the qualifier iterator.
      NavigableMap<String, NavigableMap<Long, byte[]>> columnMap;
      if (mColumn.isFullyQualified()) {
        LOG.info("Creating iterator for full-qualified column (" + mColumn + ").");

        if (!rowdata.getMap().containsKey(mColumn.getFamily())) {
          // This family is not present.
          LOG.info("Don't even have any results for this family!");
          return;
        }
        if (!rowdata.getMap().get(mColumn.getFamily()).containsKey(mColumn.getQualifier())) {
          // This qualified column is not present (although the family is).
          LOG.info("Don't have any results for this qualifier!");
          return;
        }

        // Create a single-entry map with just this one qualifier in it.  Doing so keeps the rest of
        // the code for this class agnostic to whether the column is fully-qualified or not.
        NavigableMap<Long, byte[]> qualifierMap =
            rowdata.getMap().get(mColumn.getFamily()).get(mColumn.getQualifier());
        columnMap = new TreeMap<String, NavigableMap<Long, byte[]>>();
        columnMap.put(mColumn.getQualifier(), qualifierMap);
      } else {
        LOG.info("Creating iterator for entire column family (" + mColumn + ").");
        columnMap = rowdata.getMap().get(mColumn.getFamily());
        if (null == columnMap) {
          return;
        }
      }

      mQualifierIterator = columnMap.entrySet().iterator();
      assert(mQualifierIterator.hasNext());

      Map.Entry<String, NavigableMap<Long, byte[]>> qualifierAndValues = mQualifierIterator.next();

      mCurrentQualifier = qualifierAndValues.getKey();
      LOG.info("Current qualifier for iterator = " + mCurrentQualifier);

      mVersionIterator = qualifierAndValues.getValue().entrySet().iterator();

      mNextCell = getNextCell();
    }

    /**
     * Constructs the next cell that will be returned by the iterator.
     *
     * @return The next cell in the column we are iterating over, potentially null.
     */
    private KijiCell<T> getNextCell() {
      KijiCell<T> nextCell = null;

      LOG.info("Getting next cell for iterator.");

      if (mVersionIterator.hasNext()) {
        // If the current version-to-value iterator has another value, then create a cell from that
        // and return it.
        Map.Entry<Long, byte[]> nextVersionAndValue = mVersionIterator.next();
        assert(nextVersionAndValue != null);

        try {
          nextCell = new KijiCell<T>(
              mColumn.getFamily(),
              mCurrentQualifier,
              nextVersionAndValue.getKey(),
              mDecoder.decodeCell(nextVersionAndValue.getValue())
          );
          LOG.info("Got the next cell!");
        } catch (IOException ex) {
          throw new KijiIOException(ex);
        }
        return nextCell;
      } else {
        LOG.info("The iterator for the current qualifier is empty!");
        // If the current version-to-value iterator is empty, then try to get another
        // version-to-value iterator and repeat.
        if (!mQualifierIterator.hasNext()) {
          // We are done with this entire iterator.
          LOG.info("This entire iterator is empty.");
          return null;
        }

        // Start an iterator for a new column and repeat.
        Map.Entry<String, NavigableMap<Long, byte[]>> qualifierAndValues
            = mQualifierIterator.next();

        mCurrentQualifier = qualifierAndValues.getKey();
        mVersionIterator = qualifierAndValues.getValue().entrySet().iterator();

        LOG.info("Got a new qualifier (" + mCurrentQualifier + ")");

        nextCell = getNextCell();
        assert(nextCell != null);
        return nextCell;
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
      KijiCell<T> cellToReturn = mNextCell;
      if (null == mNextCell) {
        throw new NoSuchElementException();
      } else {
        mNumVersions += 1;
      }
      mNextCell = getNextCell();
      return cellToReturn;
    }

    /** {@inheritDoc} */
    @Override
    public void remove() {
      throw new UnsupportedOperationException("Removing a cell is not a supported operation.");
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
    private final CassandraKijiRowData mRowData;
    /** The entity id for the row. */
    private final EntityId mEntityId;
    /**
     * An iterable of KijiCells, for a particular column.
     *
     * @param colName The Kiji column family that is being iterated over.
     * @param rowdata The CassandraKijiRowData instance containing the desired data.
     * @param eId of the rowdata we are iterating over.
     */
    protected CellIterable(KijiColumnName colName, CassandraKijiRowData rowdata, EntityId eId) {
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
}
