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
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.AtomicKijiPutter;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.impl.HBaseKijiTable.LayoutCapsule;
import org.kiji.schema.impl.HBaseKijiTableWriter.WriterLayoutCapsule;
import org.kiji.schema.layout.LayoutUpdatedException;
import org.kiji.schema.layout.impl.CellEncoderProvider;

/**
 * HBase implementation of AtomicKijiPutter.
 *
 * Access via HBaseKijiWriterFactory.openAtomicKijiPutter(), facilitates guaranteed atomic
 * puts in batch on a single row.
 *
 * Use <code>begin(EntityId)</code> to open a new transaction,
 * <code>put(family, qualifier, value)</code> to stage a put in the transaction,
 * and <code>commit()</code> or <code>checkAndCommit(family, qualifier, value)</code>
 * to write all staged puts atomically.
 *
 * This class is not thread-safe.  It is the user's responsibility to protect against
 * concurrent access to a writer while a transaction is being constructed.
 */
@ApiAudience.Private
public final class HBaseAtomicKijiPutter implements AtomicKijiPutter {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseAtomicKijiPutter.class);

  /** The Kiji table instance. */
  private final HBaseKijiTable mTable;

  /** The HTableInterface associated with the KijiTable. */
  private final HTableInterface mHTable;

  /** False before instance construction completes. True any time after construction. */
  private final AtomicBoolean mIsOpen = new AtomicBoolean(false);

  /** False before {@link #close()}.  True after close(). */
  private final AtomicBoolean mIsClosed = new AtomicBoolean(false);

  /** Object which processes layout update from the KijiTable to which this Writer writes. */
  private final InnerLayoutUpdater mInnerLayoutUpdater = new InnerLayoutUpdater();

  /** Lock for synchronizing layout update mutations. */
  private final Object mLock = new Object();

  /** EntityId of the row to mutate atomically. */
  private EntityId mEntityId;

  /** HBaseRowKey of the row to mutate. */
  private byte[] mId;

  /** List of HBase KeyValue objects to be written. */
  private ArrayList<KeyValue> mHopper = null;

  /**
   * All state which should be modified atomically to reflect an update to the underlying table's
   * layout.
   */
  private volatile WriterLayoutCapsule mWriterLayoutCapsule = null;

  /**
   * Composite Put containing batch puts.
   * mPut is null outside of a begin() — commit()/rollback() transaction.
   * mPut is non null inside of a begin() — commit()/rollback() transaction.
   */
  private Put mPut = null;

  /**
   * <p>
   *   Set to true when the table calls
   *   {@link InnerLayoutUpdater#update(org.kiji.schema.impl.HBaseKijiTable.LayoutCapsule)} to
   *   indicate a table layout update.  Set to false when a user calls
   *   {@link #begin(org.kiji.schema.EntityId)}.  If this becomes true while a transaction is in
   *   progress all methods which would advance the transaction will instead call
   *   {@link #rollback()} and throw a {@link LayoutUpdatedException}.
   * </p>
   * <p>
   *   Access to this variable must be protected by synchronizing on mLock.
   * </p>
   */
  private boolean mLayoutOutOfDate = false;

  /** Provides for the updating of this Writer in response to a table layout update. */
  private final class InnerLayoutUpdater implements LayoutConsumer {
    /** {@inheritDoc} */
    @Override
    public void update(final LayoutCapsule capsule) throws IOException {
      Preconditions.checkState(!mIsClosed.get(), "Cannot update a closed AtomicKijiPutter.");
      synchronized (mLock) {
        mLayoutOutOfDate = true;
        // Update the state of the writer.
        final CellEncoderProvider provider = new CellEncoderProvider(
            capsule.getLayout(),
            mTable.getKiji().getSchemaTable(),
            DefaultKijiCellEncoderFactory.get());
        // If the capsule is null this is the initial setup and we do not need a log message.
        if (mWriterLayoutCapsule != null) {
          LOG.debug(
              "Updating layout used by AtomicKijiPutter: {} for table: {} from version: {} to: {}",
              this,
              mTable.getURI(),
              mWriterLayoutCapsule.getLayout().getDesc().getLayoutId(),
              capsule.getLayout().getDesc().getLayoutId());
        } else {
          LOG.debug("Initializing AtomicKijiPutter: {} for table: {} with table layout version: {}",
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
   * Constructor for this AtomicKijiPutter.
   *
   * @param table The HBaseKijiTable to which this writer writes.
   * @throws IOException in case of an error.
   */
  public HBaseAtomicKijiPutter(HBaseKijiTable table) throws IOException {
    mTable = table;
    mHTable = mTable.openHTableConnection();
    mTable.registerLayoutConsumer(mInnerLayoutUpdater);
    Preconditions.checkState(mWriterLayoutCapsule != null,
        "AtomicKijiPutter for table: {} failed to initialize.", mTable.getURI());

    // Retain the table only when everything succeeds.
    table.retain();
    mIsOpen.set(true);
  }

  /** Resets the current transaction. */
  private void reset() {
    mPut = null;
    mEntityId = null;
    mHopper = null;
    mId = null;
  }

  /** {@inheritDoc} */
  @Override
  public void begin(EntityId eid) {
    // Preconditions.checkArgument() cannot be used here because mEntityId is null between calls to
    // begin().
    Preconditions.checkState(mIsOpen.get(), "begin() called on an AtomicKijiPutter before "
        + "construction is complete.");
    Preconditions.checkState(!mIsClosed.get(), "Cannot begin a transaction on a closed "
        + "AtomicKijiPutter.");
    if (mPut != null) {
      throw new IllegalStateException(String.format("There is already a transaction in progress on "
          + "row: %s. Call commit(), checkAndCommit(), or rollback() to clear the Put.",
          mEntityId.toShellString()));
    }
    synchronized (mLock) {
      mLayoutOutOfDate = false;
    }
    mEntityId = eid;
    mId = eid.getHBaseRowKey();
    mHopper = new ArrayList<KeyValue>();
    mPut = new Put(mId);
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mEntityId;
  }

  /** {@inheritDoc} */
  @Override
  public void commit() throws IOException {
    Preconditions.checkState(mPut != null, "commit() must be paired with a call to begin()");
    Preconditions.checkState(mIsOpen.get(), "commit() called on an AtomicKijiPutter before "
        + "construction is complete.");
    Preconditions.checkState(!mIsClosed.get(), "Cannot commit a transaction on a closed "
        + "AtomicKijiPutter.");
    // We don't actually need the writer layout capsule here, but we want the layout update check.
    getWriterLayoutCapsule();
    for (KeyValue kv : mHopper) {
      mPut.add(kv);
    }

    mHTable.put(mPut);
    if (!mHTable.isAutoFlush()) {
      mHTable.flushCommits();
    }
    reset();
  }

  /** {@inheritDoc} */
  @Override
  public <T> boolean checkAndCommit(String family, String qualifier, T value) throws IOException {
    Preconditions.checkState(mPut != null,
        "checkAndCommit() must be paired with a call to begin()");
    Preconditions.checkState(mIsOpen.get(), "checkAndCommit() called on an AtomicKijiPutter before "
        + "construction is complete.");
    Preconditions.checkState(!mIsClosed.get(), "Cannot checkAndCommit a transaction on a closed "
        + "AtomicKijiPutter.");
    final WriterLayoutCapsule capsule = getWriterLayoutCapsule();
    final KijiColumnName kijiColumnName = new KijiColumnName(family, qualifier);
    final HBaseColumnName columnName =
        capsule.getColumnNameTranslator().toHBaseColumnName(kijiColumnName);

    final KijiCellEncoder cellEncoder =
        capsule.getCellEncoderProvider().getEncoder(family, qualifier);
    final byte[] encoded = cellEncoder.encode(value);

    for (KeyValue kv : mHopper) {
      mPut.add(kv);
    }
    boolean retVal = mHTable.checkAndPut(
        mId, columnName.getFamily(), columnName.getQualifier(), encoded, mPut);
    if (retVal) {
      if (!mHTable.isAutoFlush()) {
        mHTable.flushCommits();
      }
      reset();
    }
    return retVal;
  }

  /** {@inheritDoc} */
  @Override
  public void rollback() {
    Preconditions.checkState(mPut != null, "rollback() must be paired with a call to begin()");
    Preconditions.checkState(mIsOpen.get(), "rollback() called on an AtomicKijiPutter before "
        + "construction is complete.");
    Preconditions.checkState(!mIsClosed.get(), "Cannot rollback a transaction on a closed "
        + "AtomicKijiPutter.");
    reset();
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(String family, String qualifier, T value) throws IOException {
    put(family, qualifier, HConstants.LATEST_TIMESTAMP, value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(String family, String qualifier, long timestamp, T value) throws IOException {
    Preconditions.checkState(mPut != null, "calls to put() must be between calls to begin() and "
        + "commit(), checkAndCommit(), or rollback()");
    Preconditions.checkState(mIsOpen.get(), "put() called on an AtomciKijiPutter before "
        + "construction is complete.");
    Preconditions.checkState(!mIsClosed.get(), "Cannot add to a transaction on a closed "
        + "AtomicKijiPutter.");
    final WriterLayoutCapsule capsule = getWriterLayoutCapsule();
    final KijiColumnName kijiColumnName = new KijiColumnName(family, qualifier);
    final HBaseColumnName columnName =
        capsule.getColumnNameTranslator().toHBaseColumnName(kijiColumnName);

    final KijiCellEncoder cellEncoder =
        capsule.getCellEncoderProvider().getEncoder(family, qualifier);
    final byte[] encoded = cellEncoder.encode(value);

    mHopper.add(new KeyValue(
        mId, columnName.getFamily(), columnName.getQualifier(), timestamp, encoded));
  }

  /**
   * Get the writer layout capsule ensuring that the layout has not been updated while a transaction
   * is in progress.
   *
   * @return the WriterLayoutCapsule for this writer.
   * @throws LayoutUpdatedException in case the table layout has been updated while a transaction is
   * in progress
   */
  private WriterLayoutCapsule getWriterLayoutCapsule() throws LayoutUpdatedException {
    synchronized (mLock) {
      if (mLayoutOutOfDate) {
        // If the layout was updated, roll back the transaction and throw an Exception to indicate
        // the need to retry.
        rollback();
        // TODO: SCHEMA-468 improve error message for LayoutUpdatedException.
        throw new LayoutUpdatedException(
            "Table layout was updated during a transaction, please retry.");
      } else {
        return mWriterLayoutCapsule;
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    Preconditions.checkState(mIsOpen.get(), "Cannot close an AtomicKijiPutter which has not been "
        + "fully constructed.");
    if (mPut != null) {
      LOG.warn("Closing HBaseAtomicKijiPutter "
          + "while a transaction on table {} on entity ID {} is in progress. "
          + "Rolling back transaction.",
          mEntityId);
      rollback();
    }
    mTable.unregisterLayoutConsumer(mInnerLayoutUpdater);
    Preconditions.checkState(
        !mIsClosed.getAndSet(true), "Cannot close an already closed AtomicKijiPutter.");
    mHTable.close();
    mTable.release();
  }
}
