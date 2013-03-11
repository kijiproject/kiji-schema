/**
 * (c) Copyright 2013 WibiData, Inc.
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

import com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiBufferedWriter;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.layout.impl.CellSpec;
import org.kiji.schema.layout.impl.ColumnNameTranslator;
import org.kiji.schema.util.ResourceUtils;

/**
 * HBase implementation of a batch KijiTableWriter.  Contains its own HTable connection to optimize
 * performance.  Buffer is stored locally and the underlying HTableInterface buffer is ignored.
 * Default buffer size is 2,000,000 bytes.
 */
@ApiAudience.Public
@Inheritance.Sealed
public class HBaseKijiBufferedWriter implements KijiBufferedWriter {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseKijiBufferedWriter.class);

  /** Underlying HTableInterface used by this writer. */
  private final HTableInterface mHTable;
  /** KijiTable this writer is attached to. */
  private final HBaseKijiTable mTable;
  /** Column name translator to use. */
  private final ColumnNameTranslator mTranslator;
  /** Local write buffers. */
  private ArrayList<Put> mPutBuffer = new ArrayList<Put>();
  private ArrayList<Delete> mDeleteBuffer = new ArrayList<Delete>();
  /** Local write buffer size. */
  private long mWriteBufferMaxSize = 2000000L;
  private long mCurrentWriteBufferSize = 0L;

  /**
   * Creates a buffered kiji table writer that stores modifications to be sent on command
   * or when the buffer overflows.
   *
   * @param table A kiji table.
   * @throws KijiTableNotFoundException in case of an invalid table parameter
   * @throws IOException in case of IO errors.
   */
  public HBaseKijiBufferedWriter(HBaseKijiTable table) throws IOException {
    table.retain();
    mTable = table;
    try {
      mHTable = HBaseKijiTable.createHTableInterface(table);
    } catch (TableNotFoundException e) {
      close();
      throw new KijiTableNotFoundException(table.getName());
    }
    mTranslator = new ColumnNameTranslator(mTable.getLayout());
  }

  /**
   * Size (in bytes) of a delete in the buffer.
   *
   * @param d the delete to be measured.
   * @return The size (in bytes) of the delete to buffer.
   */
  private long deleteSize(Delete d) {
    long heapsize = ClassSize.align(
        ClassSize.OBJECT + 2 * ClassSize.REFERENCE
        + 2 * Bytes.SIZEOF_LONG + Bytes.SIZEOF_BOOLEAN
        + ClassSize.REFERENCE + ClassSize.TREEMAP);
    heapsize += ClassSize.align(ClassSize.ARRAY + d.getRow().length);
    return heapsize;
  }

  // ----------------------------------------------------------------------------------------------
  // Puts

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
    mPutBuffer.add(put);
    mWriteBufferMaxSize += put.heapSize();
    if (mCurrentWriteBufferSize > mWriteBufferMaxSize) {
      flush();
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
    mDeleteBuffer.add(delete);
    mWriteBufferMaxSize += deleteSize(delete);
    if (mCurrentWriteBufferSize > mWriteBufferMaxSize) {
      flush();
    }
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
    mDeleteBuffer.add(delete);
    mWriteBufferMaxSize += deleteSize(delete);
    if (mCurrentWriteBufferSize > mWriteBufferMaxSize) {
      flush();
    }
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
    mDeleteBuffer.add(delete);
    mWriteBufferMaxSize += deleteSize(delete);
    if (mCurrentWriteBufferSize > mWriteBufferMaxSize) {
      flush();
    }
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
    final RowLock rowLock = mHTable.lockRow(hbaseRow);
    try {
      // Step 1.
      final Get get = new Get(hbaseRow, rowLock);
      get.addFamily(hbaseColumnName.getFamily());

      final FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
      filter.addFilter(new KeyOnlyFilter());
      filter.addFilter(new ColumnPrefixFilter(hbaseColumnName.getQualifier()));
      get.setFilter(filter);

      final Result result = mHTable.get(get);

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
        mDeleteBuffer.add(delete);
        mWriteBufferMaxSize += deleteSize(delete);
        if (mCurrentWriteBufferSize > mWriteBufferMaxSize) {
          flush();
        }
      }
    } finally {
      // Make sure to unlock the row!
      mHTable.unlockRow(rowLock);
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
    mDeleteBuffer.add(delete);
    mWriteBufferMaxSize += deleteSize(delete);
    if (mCurrentWriteBufferSize > mWriteBufferMaxSize) {
      flush();
    }
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
    mDeleteBuffer.add(delete);
    mWriteBufferMaxSize += deleteSize(delete);
    if (mCurrentWriteBufferSize > mWriteBufferMaxSize) {
      flush();
    }
  }

  // ----------------------------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public void setBufferSize(long bufferSize) {
    mWriteBufferMaxSize = bufferSize;
  }

  /** {@inheritDoc} */
  @Override
  public void flush() throws IOException {
    mHTable.put(mPutBuffer);
    mHTable.delete(mDeleteBuffer);
    mHTable.flushCommits();
    mCurrentWriteBufferSize = 0L;
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    flush();
    if (mHTable != null) {
      mHTable.close();
    }
    ResourceUtils.releaseOrLog(mTable);
  }
}
