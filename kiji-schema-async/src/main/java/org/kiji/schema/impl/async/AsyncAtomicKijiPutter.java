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
import java.lang.UnsupportedOperationException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.AtomicKijiPutter;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.impl.LayoutConsumer;
import org.kiji.schema.impl.hbase.HBaseKijiTableWriter.WriterLayoutCapsule;
import org.kiji.schema.layout.LayoutUpdatedException;
import org.kiji.schema.layout.impl.CellEncoderProvider;
import org.kiji.schema.layout.impl.LayoutCapsule;

/**
 * HBase implementation of AtomicKijiPutter.
 *
 * Access via AsyncKijiWriterFactory.openAtomicKijiPutter(), facilitates guaranteed atomic
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
public final class AsyncAtomicKijiPutter implements AtomicKijiPutter {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncAtomicKijiPutter.class);

  /** The Kiji table instance. */
  private final AsyncKijiTable mTable;

  /** The HTableInterface associated with the KijiTable. */
  // TODO(gabe): Replace all mHTable use with asynchbase implementation
  /* private final HTableInterface mHTable; */

  /** States of an atomic kiji putter instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this atomic kiji putter. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

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
  // TODO(gabe): Replace this with asynchbase
  /* private Put mPut = null; */

  /**
   * <p>
   *   Set to true when the table calls {@link InnerLayoutUpdater#update} to indicate a table layout
   *   update.  Set to false when a user calls {@link #begin(org.kiji.schema.EntityId)}.  If this
   *   becomes true while a transaction is in progress all methods which would advance the
   *   transaction will instead call {@link #rollback()} and throw a {@link LayoutUpdatedException}.
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
      // TODO(gabe): Replace this with asynchbase
      throw new UnsupportedOperationException("Not yet implemented to work with AsyncHBase");

      /*
      if (mState.get() == State.CLOSED) {
        LOG.debug("AtomicKijiPutter instance is closed; ignoring layout update.");
        return;
      }
      synchronized (mLock) {
        mLayoutOutOfDate = true;
        // Update the state of the writer.
        final CellEncoderProvider provider = new CellEncoderProvider(
            mTable.getURI(),
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
            capsule.getKijiColumnNameTranslator());
      }
      */
    }
  }

  /**
   * Constructor for this AtomicKijiPutter.
   *
   * @param table The HBaseKijiTable to which this writer writes.
   * @throws IOException in case of an error.
   */
  public AsyncAtomicKijiPutter(AsyncKijiTable table) throws IOException {
    // TODO(gabe): Replace this with asynchbase
    throw new UnsupportedOperationException("Not yet implemented to work with AsyncHBase");

    /*
    mTable = table;

    mHTable = mTable.openHTableConnection();
    mTable.registerLayoutConsumer(mInnerLayoutUpdater);
    Preconditions.checkState(mWriterLayoutCapsule != null,
        "AtomicKijiPutter for table: %s failed to initialize.", mTable.getURI());

    // Retain the table only when everything succeeds.
    table.retain();
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open AtomicKijiPutter instance in state %s.", oldState);
    */
  }

  /** Resets the current transaction. */
  private void reset() {
    // TODO(gabe): Replace this with asynchbase
    /* mPut = null; */
    mEntityId = null;
    mHopper = null;
    mId = null;
  }

  /** {@inheritDoc} */
  @Override
  public void begin(EntityId eid) {

    // TODO(gabe): Replace this with asynchbase
    throw new UnsupportedOperationException("Not yet implemented to work with AsyncHBase");

    /*
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot begin a transaction on an AtomicKijiPutter instance in state %s.", state);
    // Preconditions.checkArgument() cannot be used here because mEntityId is null between calls to
    // begin().
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
    */
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mEntityId;
  }

  /** {@inheritDoc} */
  @Override
  public void commit() throws IOException {
    // TODO(gabe): Replace this with asynchbase
    throw new UnsupportedOperationException("Not yet implemented to work with AsyncHBase");

    /*
    Preconditions.checkState(mPut != null, "commit() must be paired with a call to begin()");
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot commit a transaction on an AtomicKijiPutter instance in state %s.", state);
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
    */
  }

  /** {@inheritDoc} */
  @Override
  public <T> boolean checkAndCommit(String family, String qualifier, T value) throws IOException {
    // TODO(gabe): Replace this with asynchbase
    throw new UnsupportedOperationException("Not yet implemented to work with AsyncHBase");

    /*
    Preconditions.checkState(mPut != null,
        "checkAndCommit() must be paired with a call to begin()");
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot checkAndCommit a transaction on an AtomicKijiPutter instance in state %s.", state);
    final WriterLayoutCapsule capsule = getWriterLayoutCapsule();
    final KijiColumnName kijiColumnName = new KijiColumnName(family, qualifier);
    final HBaseColumnName columnName =
        capsule.getColumnNameTranslator().toHBaseColumnName(kijiColumnName);
    final byte[] encoded;

    // If passed value is null, then let encoded value be null.
    // HBase will check for non-existence of cell.
    if (null == value) {
      encoded = null;
    } else {
      final KijiCellEncoder cellEncoder =
          capsule.getCellEncoderProvider().getEncoder(family, qualifier);
      encoded = cellEncoder.encode(value);
    }

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
    */
  }

  /** {@inheritDoc} */
  @Override
  public void rollback() {
    // TODO(gabe): Replace this with asynchbase
    throw new UnsupportedOperationException("Not yet implemented to work with AsyncHBase");

    /*
    Preconditions.checkState(mPut != null, "rollback() must be paired with a call to begin()");
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot rollback a transaction on an AtomicKijiPutter instance in state %s.", state);
    reset();
    */
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(String family, String qualifier, T value) throws IOException {
    put(family, qualifier, HConstants.LATEST_TIMESTAMP, value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(String family, String qualifier, long timestamp, T value) throws IOException {

    // TODO(gabe): Replace this with asynchbase
    throw new UnsupportedOperationException("Not yet implemented to work with AsyncHBase");

    /*
    Preconditions.checkState(mPut != null, "calls to put() must be between calls to begin() and "
        + "commit(), checkAndCommit(), or rollback()");
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot put cell to an AtomicKijiPutter instance in state %s.", state);
    final WriterLayoutCapsule capsule = getWriterLayoutCapsule();
    final KijiColumnName kijiColumnName = new KijiColumnName(family, qualifier);
    final HBaseColumnName columnName =
        capsule.getColumnNameTranslator().toHBaseColumnName(kijiColumnName);

    final KijiCellEncoder cellEncoder =
        capsule.getCellEncoderProvider().getEncoder(family, qualifier);
    final byte[] encoded = cellEncoder.encode(value);

    mHopper.add(new KeyValue(
        mId, columnName.getFamily(), columnName.getQualifier(), timestamp, encoded));
    */
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

    // TODO(gabe): Replace this with asynchbase
    throw new UnsupportedOperationException("Not yet implemented to work with AsyncHBase");

    /*
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close an AtomicKijiPutter instance in state %s.", oldState);
    if (mPut != null) {
      LOG.warn("Closing HBaseAtomicKijiPutter while a transaction on table {} on entity ID {} is "
          + "in progress. Rolling back transaction.", mTable.getURI(), mEntityId);
      reset();
    }
    mTable.unregisterLayoutConsumer(mInnerLayoutUpdater);
    mHTable.close();
    mTable.release();
    */
  }
}
