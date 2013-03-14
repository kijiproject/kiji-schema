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
import java.util.Map;
import java.util.NavigableMap;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.layout.impl.CellSpec;
import org.kiji.schema.layout.impl.ColumnNameTranslator;
import org.kiji.schema.util.ResourceUtils;

/**
 * Makes modifications to a Kiji table by sending requests directly to HBase from the local client.
 */
@ApiAudience.Private
public final class HBaseKijiTableWriter implements KijiTableWriter {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseKijiTableWriter.class);

  /** The kiji table instance. */
  private final HBaseKijiTable mTable;

  /** The column name translator to use. */
  private final ColumnNameTranslator mTranslator;

  /**
   * Creates a non-buffered kiji table writer that sends modifications directly to Kiji.
   *
   * @param table A kiji table.
   */
  public HBaseKijiTableWriter(HBaseKijiTable table) {
    mTable = table;
    mTable.retain();
    mTranslator = new ColumnNameTranslator(mTable.getLayout());
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(EntityId entityId, String family, String qualifier, T value)
      throws IOException {
    put(entityId, family, qualifier, HConstants.LATEST_TIMESTAMP, value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(EntityId entityId, String family, String qualifier, long timestamp, T value)
      throws IOException {
    final KijiColumnName columnName = new KijiColumnName(family, qualifier);
    final HBaseColumnName hbaseColumnName = mTranslator.toHBaseColumnName(columnName);

    final CellSpec cellSpec = mTable.getLayout().getCellSpec(columnName)
        .setSchemaTable(mTable.getKiji().getSchemaTable());
    final KijiCellEncoder cellEncoder = DefaultKijiCellEncoderFactory.get().create(cellSpec);
    final byte[] encoded = cellEncoder.encode(value);

    final Put put = new Put(entityId.getHBaseRowKey())
        .add(hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(), timestamp, encoded);
    mTable.getHTable().put(put);
  }

  // ----------------------------------------------------------------------------------------------
  // Counter increment

  /** {@inheritDoc} */
  @Override
  public KijiCell<Long> increment(EntityId entityId, String family, String qualifier, long amount)
      throws IOException {

    verifyIsCounter(family, qualifier);

    // Translate the Kiji column name to an HBase column name.
    final HBaseColumnName hbaseColumnName =
        mTranslator.toHBaseColumnName(new KijiColumnName(family, qualifier));

    // Send the increment to the HBase HTable.
    final Increment increment = new Increment(entityId.getHBaseRowKey());
    increment.addColumn(
        hbaseColumnName.getFamily(),
        hbaseColumnName.getQualifier(),
        amount);
    final Result result = mTable.getHTable().increment(increment);
    final NavigableMap<Long, byte[]> counterEntries =
        result.getMap().get(hbaseColumnName.getFamily()).get(hbaseColumnName.getQualifier());
    assert null != counterEntries;
    assert 1 == counterEntries.size();

    final Map.Entry<Long, byte[]> counterEntry = counterEntries.firstEntry();
    final DecodedCell<Long> counter =
        new DecodedCell<Long>(null, Bytes.toLong(counterEntry.getValue()));
    return new KijiCell<Long>(family, qualifier, counterEntry.getKey(), counter);
  }

  /**
   * Verifies that a column is a counter.
   *
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @throws IOException If the column is not a counter, or it does not exist.
   */
  private void verifyIsCounter(String family, String qualifier) throws IOException {
    final KijiColumnName column = new KijiColumnName(family, qualifier);
    if (mTable.getLayout().getCellSchema(column).getType() != SchemaType.COUNTER) {
      throw new IOException(String.format("Column '%s' is not a counter", column));
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Deletes

  /** {@inheritDoc} */
  @Override
  public void deleteRow(EntityId entityId) throws IOException {
    deleteRow(entityId, HConstants.LATEST_TIMESTAMP);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteRow(EntityId entityId, long upToTimestamp) throws IOException {
    final Delete delete = new Delete(entityId.getHBaseRowKey(), upToTimestamp, null);
    mTable.getHTable().delete(delete);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(EntityId entityId, String family) throws IOException {
    deleteFamily(entityId, family, HConstants.LATEST_TIMESTAMP);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(EntityId entityId, String family, long upToTimestamp)
      throws IOException {

    final FamilyLayout familyLayout = mTable.getLayout().getFamilyMap().get(family);
    if (null == familyLayout) {
      throw new NoSuchColumnException(String.format("Family '%s' not found.", family));
    }

    if (familyLayout.getLocalityGroup().getFamilyMap().size() > 1) {
      // There are multiple families within the locality group, so we need to be clever.
      if (familyLayout.isGroupType()) {
        deleteGroupFamily(entityId, familyLayout, upToTimestamp);
      } else if (familyLayout.isMapType()) {
        deleteMapFamily(entityId, familyLayout, upToTimestamp);
      } else {
        throw new RuntimeException("Internal error: family is neither map-type nor group-type.");
      }
      return;
    }

    // The only data in this HBase family is the one Kiji family, so we can delete everything.
    final HBaseColumnName hbaseColumnName =
        mTranslator.toHBaseColumnName(new KijiColumnName(family));
    final Delete delete = new Delete(entityId.getHBaseRowKey());
    delete.deleteFamily(hbaseColumnName.getFamily(), upToTimestamp);

    // Send the delete to the HBase HTable.
    mTable.getHTable().delete(delete);
  }

  /**
   * Deletes all cells from a group-type family with a timestamp less than or equal to a
   * specified timestamp.
   *
   * @param entityId The entity (row) to delete from.
   * @param familyLayout The family layout.
   * @param upToTimestamp A timestamp.
   * @throws IOException If there is an IO error.
   */
  private void deleteGroupFamily(
      EntityId entityId,
      FamilyLayout familyLayout,
      long upToTimestamp)
      throws IOException {
    final String familyName = Preconditions.checkNotNull(familyLayout.getName());
    // Delete each column in the group according to the layout.
    final Delete delete = new Delete(entityId.getHBaseRowKey());
    for (ColumnLayout columnLayout : familyLayout.getColumnMap().values()) {
      final String qualifier = columnLayout.getName();
      final KijiColumnName column = new KijiColumnName(familyName, qualifier);
      final HBaseColumnName hbaseColumnName = mTranslator.toHBaseColumnName(column);
      delete.deleteColumns(
          hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(), upToTimestamp);
    }

    // Send the delete to the HBase HTable.
    mTable.getHTable().delete(delete);
  }

  /**
   * Deletes all cells from a map-type family with a timestamp less than or equal to a
   * specified timestamp.
   *
   * <p>This call requires an HBase row lock, so it should be used with care.</p>
   *
   * @param entityId The entity (row) to delete from.
   * @param familyLayout A family layout.
   * @param upToTimestamp A timestamp.
   * @throws IOException If there is an IO error.
   */
  private void deleteMapFamily(EntityId entityId, FamilyLayout familyLayout, long upToTimestamp)
      throws IOException {
    // Since multiple Kiji column families are mapped into a single HBase column family,
    // we have to do this delete in a two-step transaction:
    //
    // 1. Send a get() to retrieve the names of all HBase qualifiers within the HBase
    //    family that belong to the Kiji column family.
    // 2. Send a delete() for each of the HBase qualifiers found in the previous step.

    final String familyName = familyLayout.getName();
    final HBaseColumnName hbaseColumnName =
        mTranslator.toHBaseColumnName(new KijiColumnName(familyName));
    final byte[] hbaseRow = entityId.getHBaseRowKey();

    // Lock the row.
    final RowLock rowLock = mTable.getHTable().lockRow(hbaseRow);
    try {
      // Step 1.
      final Get get = new Get(hbaseRow, rowLock);
      get.addFamily(hbaseColumnName.getFamily());

      final FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
      filter.addFilter(new KeyOnlyFilter());
      filter.addFilter(new ColumnPrefixFilter(hbaseColumnName.getQualifier()));
      get.setFilter(filter);

      final Result result = mTable.getHTable().get(get);

      // Step 2.
      if (result.isEmpty()) {
        LOG.debug("No qualifiers to delete in map family: " + familyName);
      } else {
        final Delete delete = new Delete(hbaseRow, HConstants.LATEST_TIMESTAMP, rowLock);
        for (byte[] hbaseQualifier
                 : result.getFamilyMap(hbaseColumnName.getFamily()).keySet()) {
          LOG.debug("Deleting HBase column " + hbaseColumnName.getFamilyAsString()
              + ":" + Bytes.toString(hbaseQualifier));
          delete.deleteColumns(hbaseColumnName.getFamily(), hbaseQualifier, upToTimestamp);
        }
        mTable.getHTable().delete(delete);
      }
    } finally {
      // Make sure to unlock the row!
      mTable.getHTable().unlockRow(rowLock);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(EntityId entityId, String family, String qualifier) throws IOException {
    deleteColumn(entityId, family, qualifier, HConstants.LATEST_TIMESTAMP);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(EntityId entityId, String family, String qualifier, long upToTimestamp)
      throws IOException {
    final HBaseColumnName hbaseColumnName =
        mTranslator.toHBaseColumnName(new KijiColumnName(family, qualifier));
    final Delete delete = new Delete(entityId.getHBaseRowKey())
        .deleteColumns(hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(), upToTimestamp);
    mTable.getHTable().delete(delete);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(EntityId entityId, String family, String qualifier) throws IOException {
    deleteCell(entityId, family, qualifier, HConstants.LATEST_TIMESTAMP);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(EntityId entityId, String family, String qualifier, long timestamp)
      throws IOException {
    final HBaseColumnName hbaseColumnName =
        mTranslator.toHBaseColumnName(new KijiColumnName(family, qualifier));
    final Delete delete = new Delete(entityId.getHBaseRowKey())
        .deleteColumn(hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(), timestamp);
    mTable.getHTable().delete(delete);
  }

  // ----------------------------------------------------------------------------------------------

  @Override
  public void flush() throws IOException {
    mTable.getHTable().flushCommits();
  }

  @Override
  public void close() throws IOException {
    flush();

    ResourceUtils.releaseOrLog(mTable);
  }
}
