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

package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.AtomicKijiPutter;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.impl.LayoutConsumer;
import org.kiji.schema.impl.cassandra.CassandraKijiTableWriter.WriterLayoutCapsule;
import org.kiji.schema.layout.CassandraColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.LayoutUpdatedException;
import org.kiji.schema.layout.impl.CellEncoderProvider;

/**
 * Cassandra implementation of AtomicKijiPutter.
 *
 * Facilitates guaranteed atomic puts in batch on a single row.
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
public final class CassandraAtomicKijiPutter implements AtomicKijiPutter {
  // TODO: Implement compare-and-set.  Should be possible in C* 2.0.6.
  // See this thread on the C* user list: http://tinyurl.com/lcz73s3

  private static final Logger LOG = LoggerFactory.getLogger(CassandraAtomicKijiPutter.class);

  /** The Kiji table instance. */
  private final CassandraKijiTable mTable;

  /** Contains shared code with TableWriter, BufferedWriter. */
  private final CassandraKijiWriterCommon mWriterCommon;

  /** States of an atomic kiji putter instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this atomic kiji putter. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** Layout consumer registration resource. */
  private final LayoutConsumer.Registration mLayoutConsumerRegistration;

  /** Object which processes layout update from the KijiTable to which this Writer writes. */
  private final InnerLayoutUpdater mInnerLayoutUpdater = new InnerLayoutUpdater();

  /** Lock for synchronizing layout update mutations. */
  private final Object mLock = new Object();

  /** EntityId of the row to mutate atomically. */
  private EntityId mEntityId;

  /** List of cells to be written. */
  private ArrayList<Statement> mStatements = null;

  /**
   * All state which should be modified atomically to reflect an update to the underlying table's
   * layout.
   */
  private volatile WriterLayoutCapsule mWriterLayoutCapsule = null;

  /**
   * <p>
   *   Set to true when the table calls
   *   {@link CassandraAtomicKijiPutter.InnerLayoutUpdater#update(KijiTableLayout)} to
   *   indicate a table layout update.  Set to false when a user calls
   *   {@link #begin(org.kiji.schema.EntityId)}.  If this becomes true while a transaction is in
   *   progress all methods which would advance the transaction will instead call
   *   {@link #rollback()} and throw a {@link org.kiji.schema.layout.LayoutUpdatedException}.
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
    public void update(final KijiTableLayout layout) throws IOException {
      final State state = mState.get();
      Preconditions.checkState(state != State.CLOSED,
          "Cannot update an AtomicKijiPutter instance in state %s.", state);
      synchronized (mLock) {
        mLayoutOutOfDate = true;
        // Update the state of the writer.
        final CellEncoderProvider provider = new CellEncoderProvider(
            mTable.getURI(),
            layout,
            mTable.getKiji().getSchemaTable(),
            DefaultKijiCellEncoderFactory.get());
        // If the layout is null this is the initial setup and we do not need a log message.
        if (mWriterLayoutCapsule != null) {
          LOG.debug(
              "Updating layout used by AtomicKijiPutter: {} for table: {} from version: {} to: {}",
              this,
              mTable.getURI(),
              mWriterLayoutCapsule.getLayout().getDesc().getLayoutId(),
              layout.getDesc().getLayoutId());
        } else {
          LOG.debug(
              "Initializing AtomicKijiPutter: {} for table: {} with table layout version: {}",
              this,
              mTable.getURI(),
              layout.getDesc().getLayoutId());
        }
        mWriterLayoutCapsule = new WriterLayoutCapsule(
            provider,
            layout,
            CassandraColumnNameTranslator.from(layout));
      }
    }
  }

  /**
   * Constructor for this AtomicKijiPutter.
   *
   * @param table The CassandraKijiTable to which this writer writes.
   * @throws java.io.IOException in case of an error.
   */
  public CassandraAtomicKijiPutter(CassandraKijiTable table) throws IOException {
    mTable = table;
    mLayoutConsumerRegistration = mTable.registerLayoutConsumer(mInnerLayoutUpdater);
    Preconditions.checkState(mWriterLayoutCapsule != null,
        "AtomicKijiPutter for table: %s failed to initialize.", mTable.getURI());

    // Retain the table only when everything succeeds.
    table.retain();
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open AtomicKijiPutter instance in state %s.", oldState);
    mWriterCommon = new CassandraKijiWriterCommon(mTable);
  }

  /** Resets the current transaction. */
  private void reset() {
    mEntityId = null;
    mStatements = null;
  }

  /** {@inheritDoc} */
  @Override
  public void begin(EntityId eid) {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot begin a transaction on an AtomicKijiPutter instance in state %s.", state);
    // Preconditions.checkArgument() cannot be used here because mEntityId is null between calls to
    // begin().
    if (mStatements != null) {
      throw new IllegalStateException(String.format("There is already a transaction in progress on "
          + "row: %s. Call commit(), checkAndCommit(), or rollback() to clear the Put.",
          mEntityId.toShellString()));
    }
    synchronized (mLock) {
      mLayoutOutOfDate = false;
    }
    mEntityId = eid;
    mStatements = new ArrayList<Statement>();
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mEntityId;
  }

  /** {@inheritDoc} */
  @Override
  public void commit() throws IOException {
    Preconditions.checkState(mStatements != null, "commit() must be paired with a call to begin()");
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot commit a transaction on an AtomicKijiPutter instance in state %s.", state);
    // We don't actually need the writer layout capsule here, but we want the layout update check.
    getWriterLayoutCapsule();

    assert(mStatements.size() > 0);

    BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.LOGGED);
    batchStatement.addAll(mStatements);

    // TODO: Possibly check that execution worked correctly.
    ResultSet resultSet = mTable.getAdmin().execute(batchStatement);
    LOG.info("Results from batch set: " + resultSet);

    reset();
  }

  /** {@inheritDoc} */
  @Override
  public <T> boolean checkAndCommit(String family, String qualifier, T value) throws IOException {
    Preconditions.checkState(mStatements != null,
        "checkAndCommit() must be paired with a call to begin()");
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot checkAndCommit a transaction on an AtomicKijiPutter instance in state %s.", state);
    /*
    final WriterLayoutCapsule capsule = getWriterLayoutCapsule();
    final KijiColumnName kijiColumnName = new KijiColumnName(family, qualifier);
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

    // CQL currently supports only check-and-set operations that set the same cell that they
    // check.  See https://issues.apache.org/jira/browse/CASSANDRA-5633 for more details.  Thrift
    // may support everything that we need.
    */

    // TODO: Possibly suport checkAndCommit if cell to check and cell to set are the same.

    throw new KijiIOException(
        "Cassandra Kiji cannot yet support check-and-commit that inserts more than one cell.");
  }

  /** {@inheritDoc} */
  @Override
  public void rollback() {
    Preconditions.checkState(
        mStatements != null,
        "rollback() must be paired with a call to begin()"
    );
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot rollback a transaction on an AtomicKijiPutter instance in state %s.", state);
    reset();
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(String family, String qualifier, T value) throws IOException {
    if (mWriterCommon.isCounterColumn(family, qualifier)) {
      throw new UnsupportedOperationException(
          "Cannot modify counters within Cassandra atomic putter.");
    }
    put(family, qualifier, System.currentTimeMillis(), value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(
      String family,
      String qualifier,
      long timestamp,
      T value) throws IOException {
    Preconditions.checkState(mStatements != null,
        "calls to put() must be between calls to begin() and "
            + "commit(), checkAndCommit(), or rollback()");
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot put cell to an AtomicKijiPutter instance in state %s.", state);
    //final WriterLayoutCapsule capsule = getWriterLayoutCapsule();

    Statement statement = mWriterCommon.getPutStatement(
        mWriterLayoutCapsule.getCellEncoderProvider(),
        mEntityId,
        family,
        qualifier,
        timestamp,
        value);
    mStatements.add(statement);
  }

  /**
   * Get the writer layout capsule ensuring that the layout has not been updated while a transaction
   * is in progress.
   *
   * @return the WriterLayoutCapsule for this writer.
   * @throws org.kiji.schema.layout.LayoutUpdatedException in case the table layout has been
   * updated while a transaction is in progress
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
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close an AtomicKijiPutter instance in state %s.", oldState);
    if (mStatements != null) {
      LOG.warn("Closing HBaseAtomicKijiPutter while a transaction on table {} on entity ID {} is "
          + "in progress. Rolling back transaction.", mTable.getURI(), mEntityId);
      reset();
    }
    mLayoutConsumerRegistration.close();
    mTable.release();
  }
}
