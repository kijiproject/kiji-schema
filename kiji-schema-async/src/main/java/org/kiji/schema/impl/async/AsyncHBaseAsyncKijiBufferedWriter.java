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
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.util.StringUtils;
import org.hbase.async.ColumnPrefixFilter;
import org.hbase.async.DeleteRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.RowLock;
import org.hbase.async.RowLockRequest;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.AsyncKijiBufferedWriter;
import org.kiji.schema.EntityId;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiFuture;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.impl.LayoutConsumer;
import org.kiji.schema.impl.async.AsyncHBaseKijiTableWriter.WriterLayoutCapsule;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.layout.impl.CellEncoderProvider;
import org.kiji.schema.util.DebugResourceTracker;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

/**
 * <p>
 * AsyncHBase implementation of a batch AsyncKijiTableWriter. AsyncHBase's internal
 * time-based buffer is used. To set the flush interval, call <code>setFlushInterval()</code> on the
 * AsyncHBaseKiji object.
 * </p>
 *
 * <p>
 * Access to this Writer is threadsafe.  All internal state mutations must synchronize against
 * mInternalLock.
 * </p>
 */
@ApiAudience.Private
public final class AsyncHBaseAsyncKijiBufferedWriter implements AsyncKijiBufferedWriter {
  private static final Logger LOG =
      LoggerFactory.getLogger(AsyncHBaseAsyncKijiBufferedWriter.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger("cleanup." + AsyncHBaseAsyncKijiBufferedWriter.class.getName());

  /** Shared HBaseClient connection. */
  private final HBaseClient mHBClient;

  /** HBase table name */
  private final byte[] mTableName;

  /** KijiTable this writer is attached to. */
  private final AsyncHBaseKijiTable mTable;

  /** Layout consumer registration resource. */
  private final LayoutConsumer.Registration mLayoutConsumerRegistration;

  /** Monitor against which all internal state mutations must be synchronized. */
  private final Object mInternalLock = new Object();

  /**
   * All state which should be modified atomically to reflect an update to the underlying table's
   * layout.
   */
  private volatile AsyncHBaseKijiTableWriter.WriterLayoutCapsule mWriterLayoutCapsule = null;

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
    public void update(final KijiTableLayout layout) throws IOException {
      synchronized (mInternalLock) {
        if (mState == State.CLOSED) {
          LOG.debug("BufferedWriter instance is closed; ignoring layout update.");
          return;
        }
        if (mState == State.OPEN) {
          LOG.info("Flushing buffer from AsyncHBaseKijiBufferedWriter for table: {} in preparation"
              + " for layout update.", mTable.getURI());
          flush();
        }

        final CellEncoderProvider provider = new CellEncoderProvider(
            mTable.getURI(),
            layout,
            mTable.getKiji().getSchemaTable(),
            DefaultKijiCellEncoderFactory.get());
        // If the capsule is null this is the initial setup otherwise we are updating the layout.
        if (mWriterLayoutCapsule != null) {
          LOG.debug(
              "Updating layout used by AsyncHBaseKijiBufferedWriter: "
              + "{} for table: {} from version: {} to: {}",
              this,
              mTable.getURI(),
              mWriterLayoutCapsule.getLayout().getDesc().getLayoutId(),
              layout.getDesc().getLayoutId());
        } else {
          LOG.debug(
              "Initializing AsyncHBaseKijiBufferedWriter: {} for table: "
                  + "{} with table layout version: {}",
              this,
              mTable.getURI(),
              layout.getDesc().getLayoutId());
        }
        mWriterLayoutCapsule = new AsyncHBaseKijiTableWriter.WriterLayoutCapsule(
            provider,
            layout,
            HBaseColumnNameTranslator.from(layout));
      }
    }
  }

  /**
   * Creates a buffered kiji table writer that uses AsyncHBase's internal time-based buffer.
   *
   * @param table A kiji table.
   * @throws org.kiji.schema.KijiTableNotFoundException in case of an invalid table parameter
   * @throws java.io.IOException in case of IO errors.
   */
  public static AsyncHBaseAsyncKijiBufferedWriter create(AsyncHBaseKijiTable table)
      throws IOException {
    return new AsyncHBaseAsyncKijiBufferedWriter(table);
  }

  /**
   * Creates a buffered kiji table writer that uses AsyncHBase's internal time-based buffer.
   *
   * @param table A kiji table.
   * @throws org.kiji.schema.KijiTableNotFoundException in case of an invalid table parameter
   * @throws java.io.IOException in case of IO errors.
   */
  private AsyncHBaseAsyncKijiBufferedWriter(AsyncHBaseKijiTable table) throws IOException {
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
        "AsyncHBaseKijiBufferedWriter for table: %s failed to initialize.", mTable.getURI());

    // Retain the table only after everything else succeeded:
    mTable.retain();
    synchronized (mInternalLock) {
      Preconditions.checkState(mState == State.UNINITIALIZED,
          "Cannot open AsyncHBaseKijiBufferedWriter instance in state %s.", mState);
      mState = State.OPEN;
    }
    DebugResourceTracker.get().registerResource(this);
  }

  // ----------------------------------------------------------------------------------------------
  // Puts

  /** {@inheritDoc} */
  @Override
  public <T> KijiFuture<Object> put(
      final EntityId entityId,
      final KijiColumnName columnName,
      final T value)
      throws IOException {
    return put(entityId, columnName, HConstants.LATEST_TIMESTAMP, value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> KijiFuture<Object> put(
      final EntityId entityId,
      final KijiColumnName columnName,
      final long timestamp,
      final T value
  ) throws IOException {
    final AsyncHBaseKijiTableWriter.WriterLayoutCapsule capsule = mWriterLayoutCapsule;
    final HBaseColumnName hbaseColumnName =
        capsule.getColumnNameTranslator().toHBaseColumnName(columnName);

    final KijiCellEncoder cellEncoder =
        capsule.getCellEncoderProvider().getEncoder(
            columnName.getFamily(), columnName.getQualifier());
    final byte[] encoded = cellEncoder.encode(value);
    final PutRequest put = new PutRequest(
        mTableName,
        entityId.getHBaseRowKey(),
        hbaseColumnName.getFamily(),
        hbaseColumnName.getQualifier(),
        encoded,
        timestamp);
    Deferred<Object> d = mHBClient.put(put);
    return AsyncHBaseKijiFuture.create(d);
  }

  // ----------------------------------------------------------------------------------------------
  // Deletes

  /** {@inheritDoc} */
  @Override
  public KijiFuture<Object> deleteRow(final EntityId entityId) throws IOException {
    return deleteRow(entityId, HConstants.LATEST_TIMESTAMP);
  }

  /** {@inheritDoc} */
  @Override
  public KijiFuture<Object> deleteRow(final EntityId entityId, final long upToTimestamp)
      throws IOException {
    final DeleteRequest delete = new DeleteRequest(
        mTableName,
        entityId.getHBaseRowKey(),
        upToTimestamp);
    Deferred<Object> deferred = mHBClient.delete(delete);
    return AsyncHBaseKijiFuture.create(deferred);
  }

  /** {@inheritDoc} */
  @Override
  public KijiFuture<Object> deleteColumn(final EntityId entityId, final KijiColumnName columnName)
      throws IOException {
    return deleteColumn(entityId, columnName , HConstants.LATEST_TIMESTAMP);
  }

  /** {@inheritDoc} */
  @Override
  public KijiFuture<Object> deleteColumn(
      final EntityId entityId,
      final KijiColumnName columnName,
      final long upToTimestamp
  ) throws IOException {
    final AsyncHBaseKijiTableWriter.WriterLayoutCapsule capsule = mWriterLayoutCapsule;
    if (columnName.isFullyQualified()) {
      final HBaseColumnName hbaseColumnName = mWriterLayoutCapsule.getColumnNameTranslator()
          .toHBaseColumnName(columnName);
      final DeleteRequest delete = new DeleteRequest(
          mTableName, entityId.getHBaseRowKey(),
          hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(), upToTimestamp);
      final Deferred<Object> deferred =  mHBClient.delete(delete);
      return AsyncHBaseKijiFuture.create(deferred);
    } else {
      final FamilyLayout familyLayout =
          capsule.getLayout().getFamilyMap().get(columnName.getFamily());
      if (null == familyLayout) {
        throw new NoSuchColumnException(
            String.format("Family '%s' not found.", columnName.getFamily()));
      }

      if (familyLayout.getLocalityGroup().getFamilyMap().size() > 1) {
        // There are multiple families within the locality group, so we need to be clever.
        if (familyLayout.isGroupType()) {
          return deleteGroupFamily(entityId, familyLayout, upToTimestamp);
        } else if (familyLayout.isMapType()) {
          return deleteMapFamily(entityId, familyLayout, upToTimestamp);
        } else {
          throw new RuntimeException("Internal error: family is neither map-type nor group-type.");
        }
      } else {
        // The only data in this HBase family is the one Kiji family, so we can delete everything.
        final HBaseColumnName hbaseColumnName = capsule.getColumnNameTranslator()
            .toHBaseColumnName(columnName);
        final DeleteRequest delete = new DeleteRequest(
            entityId.getHBaseRowKey(),
            hbaseColumnName.getFamily(),
            upToTimestamp);
        Deferred<Object> deferred = mHBClient.delete(delete);
        return AsyncHBaseKijiFuture.create(deferred);
      }
    }
  }

  /**
   * Deletes all cells from a group-type family with a timestamp less than or equal to a
   * specified timestamp.
   *
   * @param entityId The entity (row) to delete from.
   * @param familyLayout The family layout.
   * @param upToTimestamp A timestamp.
   * @throws java.io.IOException If there is an IO error.
   */
  private KijiFuture<Object> deleteGroupFamily(
      final EntityId entityId,
      final FamilyLayout familyLayout,
      final long upToTimestamp
  ) throws IOException {
    final String familyName = familyLayout.getName();
    final HBaseColumnNameTranslator colNameTranslator =
        mWriterLayoutCapsule.getColumnNameTranslator();
    int i = 0;
    final int numColumnLayouts = familyLayout.getColumnMap().size();
    byte[][] qualifiers = new byte[numColumnLayouts][];
    for (ColumnLayout columnLayout : familyLayout.getColumnMap().values()) {
      final String qualifier = columnLayout.getName();
      final KijiColumnName column = KijiColumnName.create(familyName, qualifier);
      final HBaseColumnName hbaseColumnName = colNameTranslator.toHBaseColumnName(column);
      qualifiers[i] = hbaseColumnName.getQualifier();
      i++;
    }
    final byte[] hbaseFamilyName = mWriterLayoutCapsule.getColumnNameTranslator().
        toHBaseFamilyName(familyLayout.getLocalityGroup());
    final DeleteRequest delete = new DeleteRequest(
        mTableName,
        entityId.getHBaseRowKey(),
        hbaseFamilyName,
        qualifiers,
        upToTimestamp);
    Deferred<Object> deferred = mHBClient.delete(delete);
    return AsyncHBaseKijiFuture.create(deferred);
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
   * @throws java.io.IOException If there is an IO error.
   */
  private KijiFuture<Object> deleteMapFamily(
      final EntityId entityId,
      final FamilyLayout familyLayout,
      final long upToTimestamp
  ) throws IOException {
    // Since multiple Kiji column families are mapped into a single HBase column family,
    // we have to do this delete in a two-step transaction:
    //
    // 1. Use a Scanner to retrieve the names of all HBase qualifiers within the HBase
    //    family that belong to the Kiji column family.
    // 2. Send a delete() for each of the HBase qualifiers found in the previous step.
    final WriterLayoutCapsule writerLayoutCapsule = mWriterLayoutCapsule;
    final String familyName = familyLayout.getName();
    final HBaseColumnName hbaseColumnName = writerLayoutCapsule.getColumnNameTranslator()
        .toHBaseColumnName(KijiColumnName.create(familyName));
    final byte[] hbaseRow = entityId.getHBaseRowKey();
    List<Deferred<Object>> workers = Lists.newArrayList();

    // Lock the row.
    // Row locks are no longer supported since HBase 0.96
    // TODO(SCHEMA-899): Remove RowLock from *KijiBufferedWriter#deleteMapFamily
    final RowLock rowLock;
    Deferred<Object> deferred = null;
    try {
      rowLock = mHBClient.lockRow(new RowLockRequest(mTableName, hbaseRow)).join();
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
      throw new InternalKijiError(e);
    }
    try {
      // Step 1.
      // TODO(SCHEMA-900): Reimplement this code with a GetRequest once AsyncHBase has
      // been updated to allow GetRequest's to have the ability to use filters
      final Scanner scanner = mHBClient.newScanner(mTableName);
      scanner.setFilter(new ColumnPrefixFilter(hbaseColumnName.getQualifier()));
      scanner.setMaxVersions(1);
      scanner.setStartKey(hbaseRow);
      scanner.setStopKey(hbaseRow);
      final ArrayList<KeyValue> results;
      results = scanner.nextRows(1).join().get(0);
      scanner.close().join();

      // Step 2.
      if (results.isEmpty()) {
        LOG.debug("No qualifiers to delete in map family: " + familyName);
      } else {
        int i = 0;
        final byte[][] qualifiers = new byte[results.size()][];
        for (KeyValue keyValue : results) {
          qualifiers[i] = keyValue.qualifier();
          i++;
          LOG.debug("Deleting HBase column {}", hbaseColumnName);
        }
        DeleteRequest delete = new DeleteRequest(
            mTableName,
            hbaseRow,
            hbaseColumnName.getFamily(),
            qualifiers,
            upToTimestamp);
        deferred = mHBClient.delete(delete);
      }
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    } finally {
      try {
        // Make sure to unlock the row!
        mHBClient.unlockRow(rowLock).join();
      } catch (Exception e) {
        ZooKeeperUtils.wrapAndRethrow(e);
      }
    }
    return AsyncHBaseKijiFuture.create(deferred);
  }

  /** {@inheritDoc} */
  @Override
  public KijiFuture<Object> deleteCell(
      final EntityId entityId,
      final KijiColumnName columnName
  ) throws IOException {
    return deleteCell(entityId, columnName, HConstants.LATEST_TIMESTAMP);
  }

  /** {@inheritDoc} */
  @Override
  public KijiFuture<Object> deleteCell(
      final EntityId entityId,
      final KijiColumnName columnName,
      final long timestamp
  ) throws IOException {
    final HBaseColumnName hbaseColumnName = mWriterLayoutCapsule.getColumnNameTranslator()
        .toHBaseColumnName(columnName);
    final DeleteRequest delete = new DeleteRequest(
        mTableName,
        entityId.getHBaseRowKey(),
        hbaseColumnName.getFamily(),
        hbaseColumnName.getQualifier(),
        timestamp);
    delete.setDeleteAtTimestampOnly(true);
    final Deferred<Object> deferred = mHBClient.delete(delete);
    return AsyncHBaseKijiFuture.create(deferred);
  }

  // ----------------------------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public void setBufferSize(long bufferSize) throws IOException {
    LOG.warn("AsyncHBaseKijiBufferedWriter does not support setting the buffer size.");
  }

  /** {@inheritDoc} */
  @Override
  public KijiFuture<Object> flush() throws IOException {
    final Deferred<Object> deferred = mHBClient.flush();
    return AsyncHBaseKijiFuture.create(deferred);
}

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    synchronized (mInternalLock) {
      flush();
      Preconditions.checkState(mState == State.OPEN,
          "Cannot close BufferedWriter instance %s in state %s.", this, mState);
      mState = State.CLOSED;
      DebugResourceTracker.get().unregisterResource(this);
      mLayoutConsumerRegistration.close();
      mTable.release();
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    try {
      if (mState != State.CLOSED) {
        CLEANUP_LOG.warn(
            "Finalizing unclosed AsyncHBaseKijiBufferedWriter {} in state {}.", this, mState);
        close();
      }
    } catch (Throwable thr) {
      LOG.warn("Throwable thrown by close() in finalize of AsyncHBaseKijiBufferedWriter: {}\n{}",
          thr.getMessage(), StringUtils.stringifyException(thr));
    } finally {
      super.finalize();
    }
  }
}
