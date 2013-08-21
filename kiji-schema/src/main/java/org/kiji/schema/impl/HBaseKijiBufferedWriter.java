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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
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
import org.apache.hadoop.util.StringUtils;
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
import org.kiji.schema.impl.HBaseKijiTable.LayoutCapsule;
import org.kiji.schema.impl.HBaseKijiTableWriter.WriterLayoutCapsule;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.layout.impl.CellEncoderProvider;
import org.kiji.schema.platform.SchemaPlatformBridge;

/**
 * <p>
 * HBase implementation of a batch KijiTableWriter.  Contains its own HTable connection to optimize
 * performance.  Buffer is stored locally and the underlying HTableInterface buffer is ignored.
 * Default buffer size is 2,097,152 bytes.
 * </p>
 *
 * <p>
 * Access to this Writer is threadsafe.  All internal state mutations must synchronize against
 * mInternalLock.
 * </p>
 */
@ApiAudience.Private
@Inheritance.Sealed
public class HBaseKijiBufferedWriter implements KijiBufferedWriter {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseKijiBufferedWriter.class);

  /** Underlying dedicated HTableInterface used by this writer. Owned by this writer. */
  private final HTableInterface mHTable;

  /** KijiTable this writer is attached to. */
  private final HBaseKijiTable mTable;

  /** Object which processes layout update from the KijiTable to which this Writer writes. */
  private final InnerLayoutUpdater mInnerLayoutUpdater = new InnerLayoutUpdater();

  /** Monitor against which all internal state mutations must be synchronized. */
  private final Object mInternalLock = new Object();

  /**
   * All state which should be modified atomically to reflect an update to the underlying table's
   * layout.
   */
  private volatile WriterLayoutCapsule mWriterLayoutCapsule = null;

  /** Local write buffers. */
  private Map<EntityId, Put> mPutBuffer = new HashMap<EntityId, Put>();
  private ArrayList<Delete> mDeleteBuffer = Lists.newArrayList();

  /** Local write buffer size. */
  private long mMaxWriteBufferSize = 1024L * 1024L * 2L;
  private long mCurrentWriteBufferSize = 0L;

  /** Static overhead size of a Delete. */
  private final long mDeleteSize = ClassSize.align(
      ClassSize.OBJECT + 2 * ClassSize.REFERENCE
      + 2 * Bytes.SIZEOF_LONG + Bytes.SIZEOF_BOOLEAN
      + ClassSize.REFERENCE + ClassSize.TREEMAP);

  /** False before instance construction is complete.  True any time after construction. */
  private final AtomicBoolean mIsOpen = new AtomicBoolean(false);

  /** False before {@link #close()} completes.  True any time after close(). */
  private final AtomicBoolean mIsClosed = new AtomicBoolean(false);

  /** Provides for the updating of this Writer in response to a table layout update. */
  private final class InnerLayoutUpdater implements LayoutConsumer {
    /** {@inheritDoc} */
    @Override
    public void update(final LayoutCapsule capsule) throws IOException {
      synchronized (mInternalLock) {
        if (mIsOpen.get()) {
          Preconditions.checkState(!mIsClosed.get(),
              "Cannot update layout for a closed KijiBufferedWriter.");
          LOG.info("Flushing buffer from BufferedWriter for table: {} in preparation for layout "
              + "update.", mTable.getURI());
          flush();
        }
        final CellEncoderProvider provider = new CellEncoderProvider(
            mTable.getURI(),
            capsule.getLayout(),
            mTable.getKiji().getSchemaTable(),
            DefaultKijiCellEncoderFactory.get());
        // If the capsule is null this is the initial setup and we do not need a log message.
        if (mWriterLayoutCapsule != null) {
          LOG.debug(
              "Updating layout used by KijiBufferedWriter: "
              + "{} for table: {} from version: {} to: {}",
              this,
              mTable.getURI(),
              mWriterLayoutCapsule.getLayout().getDesc().getLayoutId(),
              capsule.getLayout().getDesc().getLayoutId());
        } else {
          LOG.debug(
              "Initializing KijiBufferedWriter: {} for table: {} with table layout version: {}",
              this,
              mTable.getURI(),
              capsule.getLayout().getDesc().getLayoutId());
        }
        mWriterLayoutCapsule = new WriterLayoutCapsule(
            provider,
            capsule.getLayout(),
            capsule.getColumnNameTranslator());
      }
    }
  }

  /**
   * Creates a buffered kiji table writer that stores modifications to be sent on command
   * or when the buffer overflows.
   *
   * @param table A kiji table.
   * @throws KijiTableNotFoundException in case of an invalid table parameter
   * @throws IOException in case of IO errors.
   */
  public HBaseKijiBufferedWriter(HBaseKijiTable table) throws IOException {
    mTable = table;
    try {
      mHTable = mTable.openHTableConnection();
    } catch (TableNotFoundException e) {
      throw new KijiTableNotFoundException(table.getName());
    }
    mTable.registerLayoutConsumer(mInnerLayoutUpdater);
    Preconditions.checkState(mWriterLayoutCapsule != null,
        "KijiBufferedWriter for table: %s failed to initialize.", mTable.getURI());

    SchemaPlatformBridge.get().setAutoFlush(mHTable, false);
    // Retain the table only after everything else succeeded:
    mTable.retain();
    mIsOpen.set(true);
  }

  // ----------------------------------------------------------------------------------------------
  // Puts

  /**
   * Add a Put to the buffer and update the current buffer size.
   *
   * @param entityId the EntityId of the row to put into.
   * @param family the byte[] representation of the hbase family to write into.
   * @param qualifier the byte[] representation of the hbase qualifier to write into.
   * @param timestamp the timestamp at which to write the value.
   * @param value the byte[] representation of the value to write.
   * @throws IOException in case of an error on flush.
   */
  private void updateBuffer(EntityId entityId, byte[] family, byte[] qualifier,
      long timestamp, byte[] value) throws IOException {
    synchronized (mInternalLock) {
      if (mPutBuffer.containsKey(entityId)) {
        mCurrentWriteBufferSize -= mPutBuffer.get(entityId).heapSize();
        mPutBuffer.get(entityId).add(family, qualifier,
            timestamp, value);
        mCurrentWriteBufferSize += mPutBuffer.get(entityId).heapSize();
      } else {
        final Put put = new Put(entityId.getHBaseRowKey())
            .add(family, qualifier, timestamp, value);
        mPutBuffer.put(entityId, put);
        mCurrentWriteBufferSize += put.heapSize();
      }
      if (mCurrentWriteBufferSize > mMaxWriteBufferSize) {
        flush();
      }
    }
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
    Preconditions.checkState(mIsOpen.get(),
        "Cannot add to buffer of a KijiBufferedWriter which has not yet completed construction");
    Preconditions.checkState(!mIsClosed.get(),
        "Cannot add to buffer of a closed KijiBufferedWriter.");

    final KijiColumnName columnName = new KijiColumnName(family, qualifier);
    final WriterLayoutCapsule capsule = mWriterLayoutCapsule;
    final HBaseColumnName hbaseColumnName =
        capsule.getColumnNameTranslator().toHBaseColumnName(columnName);

    final KijiCellEncoder cellEncoder =
        capsule.getCellEncoderProvider().getEncoder(family, qualifier);
    final byte[] encoded = cellEncoder.encode(value);

    updateBuffer(entityId, hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(), timestamp,
        encoded);
  }

  // ----------------------------------------------------------------------------------------------
  // Deletes

  /**
   * Add a Delete to the buffer and update the current buffer size.
   *
   * @param d A delete to add to the buffer.
   * @throws IOException in case of an error on flush.
   */
  private void updateBuffer(Delete d) throws IOException {
    synchronized (mInternalLock) {
      mDeleteBuffer.add(d);
      long heapSize = mDeleteSize;
      heapSize += ClassSize.align(ClassSize.ARRAY + d.getRow().length);
      mCurrentWriteBufferSize += heapSize;
      if (mCurrentWriteBufferSize > mMaxWriteBufferSize) {
        flush();
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteRow(EntityId entityId) throws IOException {
    deleteRow(entityId, HConstants.LATEST_TIMESTAMP);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteRow(EntityId entityId, long upToTimestamp) throws IOException {
    Preconditions.checkState(mIsOpen.get(),
        "Cannot add to buffer of a KijiBufferedWriter which has not yet completed construction");
    Preconditions.checkState(!mIsClosed.get(),
        "Cannot add to buffer of a closed KijiBufferedWriter.");
    final Delete delete = new Delete(entityId.getHBaseRowKey(), upToTimestamp, null);
    updateBuffer(delete);
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
    Preconditions.checkState(mIsOpen.get(),
        "Cannot add to buffer of a KijiBufferedWriter which has not yet completed construction");
    Preconditions.checkState(!mIsClosed.get(),
        "Cannot add to buffer of a closed KijiBufferedWriter.");

    final WriterLayoutCapsule capsule = mWriterLayoutCapsule;
    final FamilyLayout familyLayout = capsule.getLayout().getFamilyMap().get(family);
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
    final HBaseColumnName hbaseColumnName = capsule.getColumnNameTranslator()
        .toHBaseColumnName(new KijiColumnName(family));
    final Delete delete = new Delete(entityId.getHBaseRowKey());
    delete.deleteFamily(hbaseColumnName.getFamily(), upToTimestamp);

    // Buffer the delete.
    updateBuffer(delete);
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
    Preconditions.checkState(mIsOpen.get(),
        "Cannot add to buffer of a KijiBufferedWriter which has not yet completed construction");
    Preconditions.checkState(!mIsClosed.get(),
        "Cannot add to buffer of a closed KijiBufferedWriter.");
    final String familyName = Preconditions.checkNotNull(familyLayout.getName());
    // Delete each column in the group according to the layout.
    final Delete delete = new Delete(entityId.getHBaseRowKey());
    for (ColumnLayout columnLayout : familyLayout.getColumnMap().values()) {
      final String qualifier = columnLayout.getName();
      final KijiColumnName column = new KijiColumnName(familyName, qualifier);
      final HBaseColumnName hbaseColumnName =
          mWriterLayoutCapsule.getColumnNameTranslator().toHBaseColumnName(column);
      delete.deleteColumns(
          hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(), upToTimestamp);
    }

    // Buffer the delete.
    updateBuffer(delete);
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
    Preconditions.checkState(mIsOpen.get(),
        "Cannot add to buffer of a KijiBufferedWriter which has not yet completed construction");
    Preconditions.checkState(!mIsClosed.get(),
        "Cannot add to buffer of a closed KijiBufferedWriter.");
    // Since multiple Kiji column families are mapped into a single HBase column family,
    // we have to do this delete in a two-step transaction:
    //
    // 1. Send a get() to retrieve the names of all HBase qualifiers within the HBase
    //    family that belong to the Kiji column family.
    // 2. Send a delete() for each of the HBase qualifiers found in the previous step.

    final String familyName = familyLayout.getName();
    final HBaseColumnName hbaseColumnName = mWriterLayoutCapsule.getColumnNameTranslator()
        .toHBaseColumnName(new KijiColumnName(familyName));
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
        updateBuffer(delete);
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
    Preconditions.checkState(mIsOpen.get(),
        "Cannot add to buffer of a KijiBufferedWriter which has not yet completed construction");
    Preconditions.checkState(!mIsClosed.get(),
        "Cannot add to buffer of a closed KijiBufferedWriter.");
    final HBaseColumnName hbaseColumnName = mWriterLayoutCapsule.getColumnNameTranslator()
        .toHBaseColumnName(new KijiColumnName(family, qualifier));
    final Delete delete = new Delete(entityId.getHBaseRowKey())
        .deleteColumns(hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(), upToTimestamp);
    updateBuffer(delete);
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
    Preconditions.checkState(mIsOpen.get(),
        "Cannot add to buffer of a KijiBufferedWriter which has not yet completed construction");
    Preconditions.checkState(!mIsClosed.get(),
        "Cannot add to buffer of a closed KijiBufferedWriter.");
    final HBaseColumnName hbaseColumnName =
        mTable.getColumnNameTranslator().toHBaseColumnName(new KijiColumnName(family, qualifier));
    final Delete delete = new Delete(entityId.getHBaseRowKey())
        .deleteColumn(hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(), timestamp);
    updateBuffer(delete);
  }

  // ----------------------------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public void setBufferSize(long bufferSize) throws IOException {
    Preconditions.checkState(mIsOpen.get(),
        "Cannot set buffer size of a KijiBufferedWriter which has not yet completed construction");
    Preconditions.checkState(!mIsClosed.get(),
        "Cannot set buffer size of a closed KijiBufferedWriter.");
    Preconditions.checkArgument(bufferSize > 0,
        "Buffer size cannot be negative, got %s.", bufferSize);
    mMaxWriteBufferSize = bufferSize;
    if (mCurrentWriteBufferSize > mMaxWriteBufferSize) {
      flush();
    }
    SchemaPlatformBridge.get().setWriteBufferSize(mHTable, bufferSize);
  }

  /** {@inheritDoc} */
  @Override
  public void flush() throws IOException {
    synchronized (mInternalLock) {
      Preconditions.checkState(mIsOpen.get(),
          "Cannot flush a KijiBufferedWriter which has not yet completed construction");
      Preconditions.checkState(!mIsClosed.get(),
          "Cannot flush buffer of a closed KijiBufferedWriter.");
      if (mDeleteBuffer.size() > 0) {
        mHTable.delete(mDeleteBuffer);
        mDeleteBuffer.clear();
      }
      if (mPutBuffer.size() > 0) {
        for (EntityId eid : mPutBuffer.keySet()) {
          mHTable.put(mPutBuffer.get(eid));
        }
        mHTable.flushCommits();
        mPutBuffer.clear();
      }
      mCurrentWriteBufferSize = 0L;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    flush();
    Preconditions.checkState(mIsOpen.get(),
        "Cannot close a KijiBufferedWriter which has not yet completed construction");
    Preconditions.checkState(!mIsClosed.get(),
        "Cannot close an already closed KijiBufferedWriter.");
    mTable.unregisterLayoutConsumer(mInnerLayoutUpdater);
    mHTable.close();
    mTable.release();
    mIsClosed.set(true);
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    try {
      if (mIsOpen.get()) {
        if (!mIsClosed.get()) {
          LOG.warn("Closing KijiBufferedWriter in finalize.  Always close KijiBufferedWriters.");
          close();
        }
      }
    } catch (Throwable thr) {
      LOG.warn("Throwable thrown by close() in finalize of KijiBufferedWriter: {}\n{}",
          thr.getMessage(), StringUtils.stringifyException(thr));
    } finally {
      super.finalize();
    }
  }
}
