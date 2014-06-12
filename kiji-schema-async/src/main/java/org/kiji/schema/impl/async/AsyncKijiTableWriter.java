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
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.HConstants;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.ColumnPrefixFilter;
import org.hbase.async.DeleteRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.EntityId;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.impl.LayoutConsumer;
import org.kiji.schema.layout.KijiColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.layout.impl.CellEncoderProvider;
import org.kiji.schema.layout.impl.LayoutCapsule;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

/**
 * Makes modifications to a Kiji table by sending requests directly to HBase from the local client.
 *
 * <p> This writer flushes immediately to HBase, so there is no need to call flush() explicitly.
 * All put, increment, delete, and verify operations will cause a synchronous RPC call to HBase.
 * </p>
 * <p> This class is not thread-safe and must be synchronized externally. </p>
 */
@ApiAudience.Private
public final class AsyncKijiTableWriter implements KijiTableWriter {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncKijiTableWriter.class);

  /** The kiji table instance. */
  private final AsyncKijiTable mTable;

  /** States of a writer instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this writer. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** Layout consumer registration resource. */
  private final LayoutConsumer.Registration mLayoutConsumerRegistration;

  /** Shared HBaseClient connection. */
  private final HBaseClient mHBClient;

  /** HBase table name */
  private final byte[] mTableName;

  /**
   * All state which should be modified atomically to reflect an update to the underlying table's
   * layout.
   */
  private volatile WriterLayoutCapsule mWriterLayoutCapsule = null;

  /**
   * A container for all writer state which should be modified atomically to reflect an update to
   * the underlying table's layout.
   */
  public static final class WriterLayoutCapsule {
    private final CellEncoderProvider mCellEncoderProvider;
    private final KijiTableLayout mLayout;
    private final KijiColumnNameTranslator mTranslator;

    /**
     * Default constructor.
     *
     * @param cellEncoderProvider the encoder provider to store in this container.
     * @param layout the table layout to store in this container.
     * @param translator the column name translator to store in this container.
     */
    public WriterLayoutCapsule(
        final CellEncoderProvider cellEncoderProvider,
        final KijiTableLayout layout,
        final KijiColumnNameTranslator translator) {
      mCellEncoderProvider = cellEncoderProvider;
      mLayout = layout;
      mTranslator = translator;
    }

    /**
     * Get the column name translator from this container.
     *
     * @return the column name translator from this container.
     */
    public KijiColumnNameTranslator getColumnNameTranslator() {
      return mTranslator;
    }

    /**
     * Get the table layout from this container.
     *
     * @return the table layout from this container.
     */
    public KijiTableLayout getLayout() {
      return mLayout;
    }

    /**
     * get the encoder provider from this container.
     *
     * @return the encoder provider from this container.
     */
    public CellEncoderProvider getCellEncoderProvider() {
      return mCellEncoderProvider;
    }
  }

  /** Provides for the updating of this Writer in response to a table layout update. */
  private final class InnerLayoutUpdater implements LayoutConsumer {
    /** {@inheritDoc} */
    @Override
    public void update(final LayoutCapsule capsule) throws IOException {
      final State state = mState.get();
      if (state == State.CLOSED) {
        LOG.debug("Writer is closed: ignoring layout update.");
        return;
      }
      final CellEncoderProvider provider = new CellEncoderProvider(
          mTable.getURI(),
          capsule.getLayout(),
          mTable.getKiji().getSchemaTable(),
          DefaultKijiCellEncoderFactory.get());
      // If the capsule is null this is the initial setup and we do not need a log message.
      if (mWriterLayoutCapsule != null) {
        LOG.debug(
            "Updating layout used by KijiTableWriter: {} for table: {} from version: {} to: {}",
            this,
            mTable.getURI(),
            mWriterLayoutCapsule.getLayout().getDesc().getLayoutId(),
            capsule.getLayout().getDesc().getLayoutId());
      } else {
        LOG.debug("Initializing KijiTableWriter: {} for table: {} with table layout version: {}",
            this,
            mTable.getURI(),
            capsule.getLayout().getDesc().getLayoutId());
      }
      // Normally we would atomically flush and update mWriterLayoutCapsule here,
      // but since this writer is unbuffered, the flush is unnecessary
      mWriterLayoutCapsule = new WriterLayoutCapsule(
          provider,
          capsule.getLayout(),
          capsule.getKijiColumnNameTranslator());
    }
  }

  /**
   * Creates a non-buffered kiji table writer that sends modifications directly to Kiji.
   *
   * @param table A kiji table.
   * @throws IOException on I/O error.
   */
  public AsyncKijiTableWriter(AsyncKijiTable table) throws IOException {

    mTable = table;
    mLayoutConsumerRegistration = mTable.registerLayoutConsumer(new InnerLayoutUpdater());
    Preconditions.checkState(mWriterLayoutCapsule != null,
        "KijiTableWriter for table: %s failed to initialize.", mTable.getURI());
    mHBClient = table.getHBClient();
    mHBClient.setFlushInterval((short) 0);
    mTableName = KijiManagedHBaseTableName
        .getKijiTableName(mTable.getURI().getInstance(), mTable.getURI().getTable()).toBytes();

    // Retain the table only when everything succeeds.
    mTable.retain();
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open KijiTableWriter instance in state %s.", oldState);
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
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot put cell to KijiTableWriter instance %s in state %s.", this, state);

    final KijiColumnName columnName = new KijiColumnName(family, qualifier);
    final WriterLayoutCapsule capsule = mWriterLayoutCapsule;
    final HBaseColumnName hbaseColumnName =
        capsule.getColumnNameTranslator().toHBaseColumnName(columnName);

    final KijiCellEncoder cellEncoder =
        capsule.getCellEncoderProvider().getEncoder(family, qualifier);
    final byte[] encoded = cellEncoder.encode(value);

    final PutRequest put = new PutRequest(
        mTableName,
        entityId.getHBaseRowKey(),
        hbaseColumnName.getFamily(),
        hbaseColumnName.getQualifier(),
        encoded,
        timestamp);
    try {
      mHBClient.put(put).join();
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Counter increment


  /**
   * {@inheritDoc}
   *
   * In the AsyncHBase implementation the returned KijiCell always has a timestamp of -1.
   */
  @Override
  public KijiCell<Long> increment(EntityId entityId, String family, String qualifier, long amount)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot increment cell to KijiTableWriter instance %s in state %s.", this, state);

    verifyIsCounter(family, qualifier);

    // Translate the Kiji column name to an HBase column name.
    final HBaseColumnName hbaseColumnName = mWriterLayoutCapsule.getColumnNameTranslator().
        toHBaseColumnName(new KijiColumnName(family, qualifier));

    // Send the increment to the HBaseClient
    final AtomicIncrementRequest increment = new AtomicIncrementRequest(
        mTableName, entityId.getHBaseRowKey(),
        hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(), amount);
    final Long incrementedVal;
    try {
       incrementedVal = mHBClient.atomicIncrement(increment).join();
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
      throw new RuntimeException(e);
    }
    final DecodedCell<Long> counter = new DecodedCell<Long>(
        DecodedCell.NO_SCHEMA,
        incrementedVal);
    return new KijiCell<Long>(family, qualifier, -1, counter);
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
    if (mWriterLayoutCapsule.getLayout().getCellSchema(column).getType() != SchemaType.COUNTER) {
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

    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete row while KijiTableWriter %s is in state %s.", this, state);
    final DeleteRequest delete = new DeleteRequest(
        mTableName,
        entityId.getHBaseRowKey(),
        upToTimestamp);
    try {
      mHBClient.delete(delete).join();
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
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
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete family while KijiTableWriter %s is in state %s.", this, state);

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
    final DeleteRequest delete = new DeleteRequest(
        entityId.getHBaseRowKey(),
        hbaseColumnName.getFamily(),
        upToTimestamp);

    //Send the delete to the HBaseClient
    try {
      mHBClient.delete(delete).join();
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
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
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete family group while KijiTableWriter %s is in state %s.", this, state);
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
    final byte[] hbaseFamilyName =
        mWriterLayoutCapsule.getColumnNameTranslator()
            .toHBaseFamilyName(familyLayout.getLocalityGroup());
    final DeleteRequest delete = new DeleteRequest(
        mTableName,
        entityId.getHBaseRowKey(),
        hbaseFamilyName,
        qualifiers,
        upToTimestamp);
    try {
      mHBClient.delete(delete).join();
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }
  }

  /**
   * Deletes all cells from a map-type family with a timestamp less than or equal to a
   * specified timestamp.
   *
   * <p>No longer uses a rowlock, so it may miss new columns which are written as it runs.</p>
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
    // 1. Use a scanner to retrieve the names of all HBase qualifiers within the HBase
    //    family that belong to the Kiji column family.
    // 2. Send a delete() for each of the HBase qualifiers found in the previous step.

    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete map family while KijiTableWriter %s is in state %s.", this, state);
    final String familyName = familyLayout.getName();
    final HBaseColumnName hbaseColumnName = mWriterLayoutCapsule.getColumnNameTranslator()
        .toHBaseColumnName(new KijiColumnName(familyName));
    final byte[] hbaseRow = entityId.getHBaseRowKey();

    // Step 1.
    // TODO: Reimplement this code with a GetRequest once AsyncHBase has
    // been updated to allow GetRequest's to have the ability to use filters
    final Scanner scanner = mHBClient.newScanner(mTable.getName());

    scanner.setFilter(new ColumnPrefixFilter(hbaseColumnName.getQualifier()));
    scanner.setStartKey(hbaseRow);
    scanner.setStopKey(hbaseRow);
    final ArrayList<KeyValue> results;
    try {
      results = scanner.nextRows(1).join().get(0);
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
      throw new InternalKijiError(e);//Should never happen
    }
    scanner.close();

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
        try {
          mHBClient.delete(delete).join();
        } catch (Exception e) {
          ZooKeeperUtils.wrapAndRethrow(e);
        }
      }
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
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete column while KijiTableWriter %s is in state %s.", this, state);

    final HBaseColumnName hbaseColumnName = mWriterLayoutCapsule.getColumnNameTranslator()
        .toHBaseColumnName(new KijiColumnName(family, qualifier));
    final DeleteRequest delete = new DeleteRequest(
        mTableName, entityId.getHBaseRowKey(),
        hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(), upToTimestamp);
    delete.setDeleteAtTimestampOnly(false);
    try {
      mHBClient.delete(delete).join();
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
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
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete cell while KijiTableWriter %s is in state %s.", this, state);

    final HBaseColumnName hbaseColumnName = mWriterLayoutCapsule.getColumnNameTranslator()
        .toHBaseColumnName(new KijiColumnName(family, qualifier));
    final DeleteRequest delete = new DeleteRequest(
        mTableName, entityId.getHBaseRowKey(),
        hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(), timestamp);
    delete.setDeleteAtTimestampOnly(true);
    try {
      mHBClient.delete(delete).join();
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }
  }

  // ----------------------------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public void flush() throws IOException {
    LOG.debug("KijiTableWriter does not need to be flushed.");
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close KijiTableWriter instance %s in state %s.", this, oldState);
    mLayoutConsumerRegistration.close();
    mTable.release();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(AsyncKijiTableWriter.class)
        .add("id", System.identityHashCode(this))
        .add("table", mTable.getURI())
        .add("layout-version", mWriterLayoutCapsule.getLayout().getDesc().getLayoutId())
        .add("state", mState)
        .toString();
  }
}
