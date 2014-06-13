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

package org.kiji.schema.impl.async;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.util.StringUtils;
import org.hbase.async.ColumnPrefixFilter;
import org.hbase.async.HBaseClient;
import org.hbase.async.DeleteRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.RowLock;
import org.hbase.async.RowLockRequest;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiBufferedWriter;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.impl.LayoutConsumer;
import org.kiji.schema.layout.KijiColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.layout.impl.CellEncoderProvider;
import org.kiji.schema.layout.impl.LayoutCapsule;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

/**
 * <p>
 * AsyncHBase implementation of a batch KijiTableWriter. Buffer is stored locally and the underlying
 * AsyncHbase time-based buffer is ignored.
 * Default buffer size is 2,097,152 bytes.
 * </p>
 *
 * <p>
 * Access to this Writer is threadsafe.  All internal state mutations must synchronize against
 * mInternalLock.
 * </p>
 */
@ApiAudience.Private
public final class AsyncKijiBufferedWriter implements KijiBufferedWriter {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncKijiBufferedWriter.class);

  /** Shared HBaseClient connection. */
  private final HBaseClient mHBClient;

  /** HBase table name */
  private final byte[] mTableName;

  /** KijiTable this writer is attached to. */
  private final AsyncKijiTable mTable;

  /** Layout consumer registration resource. */
  private final LayoutConsumer.Registration mLayoutConsumerRegistration;

  /** Monitor against which all internal state mutations must be synchronized. */
  private final Object mInternalLock = new Object();

  /**
   * All state which should be modified atomically to reflect an update to the underlying table's
   * layout.
   */
  private volatile AsyncKijiTableWriter.WriterLayoutCapsule mWriterLayoutCapsule = null;

  /** Local write buffers. */
  private Map<EntityId, PutRequest> mPutBuffer = new HashMap<EntityId, PutRequest>();
  private ArrayList<DeleteRequest> mDeleteBuffer = Lists.newArrayList();

  /** Local write buffer size. */
  private long mMaxWriteBufferSize = 1024L * 1024L * 2L;
  private long mCurrentWriteBufferSize = 0L;

  /** Static overhead size of a BatchableRPC (both DeleteRequest and PutRequest. */
  private static final long batchableRpcSize = ClassSize.align(
      //From HBaseRpc
      ClassSize.REFERENCE         // Deferred<Object> deferred;
      + ClassSize.REFERENCE       // byte[] table
      + ClassSize.REFERENCE       // byte[] key
      + ClassSize.REFERENCE       // RegionInfo region
      + 2 * Bytes.SIZEOF_BYTE     // byte attempt, boolean failfast
      //From BatchableRpc
      + ClassSize.REFERENCE       // byte[] family
      + 2 * Bytes.SIZEOF_LONG     // long timestamp, long lockid
      + 2 * Bytes.SIZEOF_BOOLEAN);  // boolean bufferable, boolean durable

  /** Static overhead size of a DeleteRequest. */
  private static final long mDeleteRequestSize = ClassSize.align(
      batchableRpcSize
      + ClassSize.OBJECT        // Object
      + ClassSize.REFERENCE     // byte[][] qualifiers
      + Bytes.SIZEOF_BOOLEAN);  // boolean at_timestamp_only

  /** Static overhead size of a PutRequest. */
  private static final long mPutRequestSize = ClassSize.align(
      batchableRpcSize
      + ClassSize.OBJECT          // Object
      + ClassSize.REFERENCE       // byte[][] qualifiers
      + ClassSize.REFERENCE);     // byte[][] values

  /** Static overhead size of a Deferred. */
  private static long mDeferredSize = ClassSize.align(
      ClassSize.OBJECT
      + ClassSize.INTEGER         // int state
      + ClassSize.REFERENCE       // Object result
      + ClassSize.REFERENCE       // Callback[] callbacks
      + 2 * Bytes.SIZEOF_SHORT);  // short next_callback, last_callback

  /** States of a buffered writer instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /**
   * Tracks the state of this buffered writer.
   * Reads and writes to mState must by synchronized by mInternalLock.
   */
  private State mState = State.UNINITIALIZED;

  /** Provides for the updating of this Writer in response to a table layout update. */
  private final class InnerLayoutUpdater implements LayoutConsumer {
    /** {@inheritDoc} */
    @Override
    public void update(final LayoutCapsule capsule) throws IOException {
      synchronized (mInternalLock) {
        if (mState == State.CLOSED) {
          LOG.debug("BufferedWriter instance is closed; ignoring layout update.");
          return;
        }
        if (mState == State.OPEN) {
          LOG.info("Flushing buffer from AsyncKijiBufferedWriter for table: {} in preparation for"
              + " layout update.", mTable.getURI());
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
              "Updating layout used by HBaseKijiBufferedWriter: "
              + "{} for table: {} from version: {} to: {}",
              this,
              mTable.getURI(),
              mWriterLayoutCapsule.getLayout().getDesc().getLayoutId(),
              capsule.getLayout().getDesc().getLayoutId());
        } else {
          LOG.debug(
              "Initializing HBaseKijiBufferedWriter: {} for table: "
                  + "{} with table layout version: {}",
              this,
              mTable.getURI(),
              capsule.getLayout().getDesc().getLayoutId());
        }
        mWriterLayoutCapsule = new AsyncKijiTableWriter.WriterLayoutCapsule(
            provider,
            capsule.getLayout(),
            capsule.getKijiColumnNameTranslator());
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
  public AsyncKijiBufferedWriter(AsyncKijiTable table) throws IOException {
    mTable = table;
    mHBClient = table.getHBClient();
    mTableName = KijiManagedHBaseTableName
        .getKijiTableName(mTable.getURI().getInstance(), mTable.getURI().getTable()).toBytes();
    try {
      mHBClient.ensureTableExists(mTableName).join();
    } catch (TableNotFoundException e) {
      throw new KijiTableNotFoundException(table.getURI());
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }
    mLayoutConsumerRegistration = mTable.registerLayoutConsumer(new InnerLayoutUpdater());
    Preconditions.checkState(
        mWriterLayoutCapsule != null,
        "HBaseKijiBufferedWriter for table: %s failed to initialize.", mTable.getURI());

    // Set the flushInterval to max value to disable auto-flushing
    mHBClient.setFlushInterval(Short.MAX_VALUE);
    // Retain the table only after everything else succeeded:
    mTable.retain();
    synchronized (mInternalLock) {
      Preconditions.checkState(mState == State.UNINITIALIZED,
          "Cannot open HBaseKijiBufferedWriter instance in state %s.", mState);
      mState = State.OPEN;
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Puts

  /**
   * Calculate the estimated heap size for a given HBaseRpc.
   *
   * @param table the table of the HBaseRpc to estimate heap size for.
   * @param key the key of the HBaseRpc to estimate heap size for.
   * @param family the family of the HBaseRpc to estimate heap size for.
   * @param qualifiers the qualifiers of the HBaseRpc to estimate heap size for.
   */
  private long heapSizeForBatchableRpc(
      byte[] table, byte[] key, byte[] family, byte[][] qualifiers) {
    long heapsize = mDeferredSize;

    //Adding table, key, and family
    heapsize += ClassSize.align(
        3 * ClassSize.ARRAY
            + table.length
            + key.length
            + family.length);

    //Adding qualifiers
    for (int i = 0; i < qualifiers.length; i++) {
      heapsize += ClassSize.align(ClassSize.ARRAY + qualifiers[i].length);
    }
    return ClassSize.align((int)heapsize);
  }

  /**
   * Calculate the estimated heap size for a given PutRequest.
   *
   * @param put the PutRequest to estimate heap size for.
   */
  private long heapSize(PutRequest put) {
    long heapsize = mPutRequestSize;
    heapsize += heapSizeForBatchableRpc(
        put.table(),put.key(),put.family(),put.qualifiers());

    //Adding values
    for (int i = 0; i < put.values().length; i++) {
      heapsize += ClassSize.align(ClassSize.ARRAY + put.values()[i].length);
    }
    return ClassSize.align((int)heapsize);
  }

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
      Preconditions.checkState(mState == State.OPEN,
          "Cannot write to BufferedWriter instance in state %s.", mState);

      final PutRequest put = new PutRequest(mTableName, entityId.getHBaseRowKey(),
          family, qualifier, value, timestamp);
      mPutBuffer.put(entityId, put);
      mCurrentWriteBufferSize += heapSize(put);
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
    final KijiColumnName columnName = new KijiColumnName(family, qualifier);
    final AsyncKijiTableWriter.WriterLayoutCapsule capsule = mWriterLayoutCapsule;
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
   * Calculate the estimated heap size for a given DeleteRequest.
   *
   * @param delete the DeleteRequest to estimate heap size for.
   */
  private long heapSize(DeleteRequest delete) {
    long heapsize = mDeleteRequestSize;
    heapsize += heapSizeForBatchableRpc(
        delete.table(), delete.key(), delete.family(), delete.qualifiers());

    return ClassSize.align((int)heapsize);
  }

  /**
   * Add a Delete to the buffer and update the current buffer size.
   *
   * @param d A delete to add to the buffer.
   * @throws IOException in case of an error on flush.
   */
  private void updateBuffer(DeleteRequest d) throws IOException {
    synchronized (mInternalLock) {
      mDeleteBuffer.add(d);
      long heapSize = heapSize(d);
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
    final DeleteRequest delete = new DeleteRequest(mTableName, entityId.getHBaseRowKey(), upToTimestamp);
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
    final AsyncKijiTableWriter.WriterLayoutCapsule capsule = mWriterLayoutCapsule;
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
    final DeleteRequest delete = new DeleteRequest(
        entityId.getHBaseRowKey(),
        hbaseColumnName.getFamily(),
        upToTimestamp);

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
    final String familyName = Preconditions.checkNotNull(familyLayout.getName());
    final KijiColumnNameTranslator colNameTranslator = mWriterLayoutCapsule.getColumnNameTranslator();
    int i = 0;
    final int numColumnLayouts = familyLayout.getColumnMap().size();
    byte[][] qualifiers = new byte[numColumnLayouts][];
    for (ColumnLayout columnLayout : familyLayout.getColumnMap().values()) {
      final String qualifier = columnLayout.getName();
      final KijiColumnName column = new KijiColumnName(familyName, qualifier);
      final HBaseColumnName hbaseColumnName = colNameTranslator.toHBaseColumnName(column);
      qualifiers[i] = hbaseColumnName.getQualifier();
      i ++;
    }
    final byte[] hbaseFamilyName = mWriterLayoutCapsule.getColumnNameTranslator().
        toHBaseFamilyName(familyLayout.getLocalityGroup());
    final DeleteRequest delete = new DeleteRequest(
        mTableName,
        entityId.getHBaseRowKey(),
        hbaseFamilyName,
        qualifiers,
        upToTimestamp);

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
    // Since multiple Kiji column families are mapped into a single HBase column family,
    // we have to do this delete in a two-step transaction:
    //
    // 1. Use a Scanner to retrieve the names of all HBase qualifiers within the HBase
    //    family that belong to the Kiji column family.
    // 2. Send a delete() for each of the HBase qualifiers found in the previous step.

    final String familyName = familyLayout.getName();
    final HBaseColumnName hbaseColumnName = mWriterLayoutCapsule.getColumnNameTranslator()
        .toHBaseColumnName(new KijiColumnName(familyName));
    final byte[] hbaseRow = entityId.getHBaseRowKey();

    // Lock the row.
    // Row locks are no longer supported since HBase 0.96?
    final RowLock rowLock;
    try {
      rowLock = mHBClient.lockRow(new RowLockRequest(mTableName, hbaseRow)).join();
      // Step 1.
      // TODO: Reimplement this code with a GetRequest once AsyncHBase has
      // been updated to allow GetRequest's to have the ability to use filters
      final Scanner scanner = mHBClient.newScanner(mTable.getName());
      scanner.setFilter(new ColumnPrefixFilter(hbaseColumnName.getQualifier()));
      scanner.setStartKey(hbaseRow);
      scanner.setStopKey(hbaseRow);
      final ArrayList<KeyValue> results;
      results = scanner.nextRows(1).join().get(0);
      scanner.close().join();

      // Step 2.
      if (results.isEmpty()) {
        LOG.debug("No qualifiers to delete in map family: " + familyName);
      } else {

        for (KeyValue keyValue : results) {
          final DeleteRequest delete = new DeleteRequest(
              mTableName,
              hbaseRow,
              keyValue.family(),
              keyValue.qualifier(),
              upToTimestamp);
          LOG.debug("Deleting HBase column " + hbaseColumnName.getFamilyAsString()
                  + ":" + Bytes.toString(keyValue.qualifier()));
          updateBuffer(delete);
        }
      }


    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
      throw new InternalKijiError(e);
    }
    try {
      // Make sure to unlock the row!
      mHBClient.unlockRow(rowLock).join();
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
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
    final HBaseColumnName hbaseColumnName = mWriterLayoutCapsule.getColumnNameTranslator()
        .toHBaseColumnName(new KijiColumnName(family, qualifier));
    final DeleteRequest delete = new DeleteRequest(
        mTableName, entityId.getHBaseRowKey(),
        hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(), upToTimestamp);
    delete.setDeleteAtTimestampOnly(false);
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
    final HBaseColumnName hbaseColumnName = mWriterLayoutCapsule.getColumnNameTranslator()
        .toHBaseColumnName(new KijiColumnName(family, qualifier));
    final DeleteRequest delete = new DeleteRequest(
        mTableName, entityId.getHBaseRowKey(),
        hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(), timestamp);
    delete.setDeleteAtTimestampOnly(true);
    updateBuffer(delete);
  }

  // ----------------------------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public void setBufferSize(long bufferSize) throws IOException {
    synchronized (mInternalLock) {
      Preconditions.checkState(mState == State.OPEN,
          "Cannot set buffer size of BufferedWriter instance %s in state %s.", this, mState);
      Preconditions.checkArgument(bufferSize > 0,
          "Buffer size cannot be negative, got %s.", bufferSize);
      mMaxWriteBufferSize = bufferSize;
      if (mCurrentWriteBufferSize > mMaxWriteBufferSize) {
        flush();
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void flush() throws IOException {
    synchronized (mInternalLock) {
      Preconditions.checkState(mState == State.OPEN,
          "Cannot flush BufferedWriter instance %s in state %s.", this, mState);
      final ArrayList<Deferred<Object>> workers = new ArrayList<Deferred<Object>>();
      if (mDeleteBuffer.size() > 0) {
        for (DeleteRequest delete : mDeleteBuffer) {
          Deferred<Object> d = mHBClient.delete(delete);
          workers.add(d);
        }
        mDeleteBuffer.clear();
      }
      if (mPutBuffer.size() > 0) {
        for (EntityId eid : mPutBuffer.keySet()) {
          Deferred<Object> d = mHBClient.put(mPutBuffer.get(eid));
          workers.add(d);
        }
        mPutBuffer.clear();
      }
      try {
        mHBClient.flush().join();
        Deferred.group(workers).join();
      } catch (Exception e) {
        ZooKeeperUtils.wrapAndRethrow(e);
      }
      mCurrentWriteBufferSize = 0L;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    synchronized (mInternalLock) {
      flush();
      Preconditions.checkState(mState == State.OPEN,
          "Cannot close BufferedWriter instance %s in state %s.", this, mState);
      mState = State.CLOSED;
      mLayoutConsumerRegistration.close();
      mTable.release();
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    try {
      if (mState != State.CLOSED) {
        LOG.warn("Finalizing unclosed HBaseKijiBufferedWriter {} in state {}.", this, mState);
        close();
      }
    } catch (Throwable thr) {
      LOG.warn("Throwable thrown by close() in finalize of HBaseKijiBufferedWriter: {}\n{}",
          thr.getMessage(), StringUtils.stringifyException(thr));
    } finally {
      super.finalize();
    }
  }
}
