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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.stumbleupon.async.Deferred;
import org.apache.hadoop.conf.Configuration;
import org.hbase.async.HBaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiReaderFactory;
import org.kiji.schema.KijiRegion;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableAnnotator;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiWriterFactory;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.impl.LayoutConsumer;
import org.kiji.schema.impl.LayoutConsumer.Registration;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.TableLayoutMonitor;
import org.kiji.schema.util.Debug;
import org.kiji.schema.util.DebugResourceTracker;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.schema.util.VersionInfo;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

/**
 * <p>A KijiTable that exposes the underlying HBase implementation.</p>
 *
 * <p>Within the internal Kiji code, we use this class so that we have
 * access to the HTable interface.  Methods that Kiji clients should
 * have access to should be added to org.kiji.schema.KijiTable.</p>
 */
@ApiAudience.Private
public final class AsyncKijiTable implements KijiTable {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncKijiTable.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger("cleanup." + AsyncKijiTable.class.getName());
  private static final String ENABLE_CONSTRUCTOR_STACK_LOGGING_MESSAGE = String.format(
      "Enable DEBUG log level for logger: %s for a stack trace of the construction of this object.",
      CLEANUP_LOG.getName());

  /** The kiji instance this table belongs to. */
  private final AsyncKiji mKiji;

  /** The name of this table (the Kiji name, not the HBase name). */
  private final String mName;

  /** URI of this table. */
  private final KijiURI mTableURI;

  /** States of a kiji table instance. */
  private static enum State {
    /**
     * Initialization begun but not completed.  Retain counter and DebugResourceTracker counters
     * have not been incremented yet.
     */
    UNINITIALIZED,
    /**
     * Finished initialization.  Both retain counters and DebugResourceTracker counters have been
     * incremented.  Resources are successfully opened and this HBaseKijiTable's methods may be
     * used.
     */
    OPEN,
    /**
     * Closed.  Other methods are no longer supported.  Resources and connections have been closed.
     */
    CLOSED
  }

  /** Tracks the state of this kiji table. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** String representation of the call stack at the time this object is constructed. */
  private final String mConstructorStack;

  /** HBaseClient for managing AsyncHBase tables associated with this KijiTable. */
  private final HBaseClient mHBClient;

  /** The factory for EntityIds. */
  private final EntityIdFactory mEntityIdFactory;

  /** Retain counter. When decreased to 0, the HBase KijiTable may be closed and disposed of. */
  private final AtomicInteger mRetainCount = new AtomicInteger(0);

  /** Writer factory for this table. */
  private final KijiWriterFactory mWriterFactory;

  /** Reader factory for this table. */
  private final KijiReaderFactory mReaderFactory;

/** Name of the HBase table backing this Kiji table. */
  private final String mHBaseTableName;

  /**
   * Monitor for the layout of this table. Should be initialized in the constructor and nulled out
   * in {@link #closeResources()}. No other method should modify this pointer.
   **/
  private final TableLayoutMonitor mLayoutMonitor;

  /**
   * Construct an opened Kiji table stored in HBase.
   *
   * @param kiji The Kiji instance.
   * @param name The name of the Kiji user-space table to open.
   * @param hbClient An HBaseClient that manages AsyncHBase tables.
   * @param layoutMonitor a valid TableLayoutMonitor for this table.
   *
   * @throws IOException On an HBase error.
   *     <p> Throws KijiTableNotFoundException if the table does not exist. </p>
   */
  AsyncKijiTable(
      AsyncKiji kiji,
      String name,
      HBaseClient hbClient,
      TableLayoutMonitor layoutMonitor)
      throws IOException {
    mConstructorStack = (CLEANUP_LOG.isDebugEnabled())
        ? Debug.getStackTrace()
        : ENABLE_CONSTRUCTOR_STACK_LOGGING_MESSAGE;

    mKiji = kiji;
    mKiji.retain();

    mName = name;
    mHBClient = hbClient;
    mTableURI = KijiURI.newBuilder(mKiji.getURI()).withTableName(mName).build();
    LOG.debug("Opening Kiji table '{}' with client version '{}'.",
        mTableURI, VersionInfo.getSoftwareVersion());
    mHBaseTableName =
        KijiManagedHBaseTableName.getKijiTableName(mTableURI.getInstance(), mName).toString();

    try {
      mHBClient.ensureTableExists(mHBaseTableName).join();
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }
    if (!mKiji.getTableNames().contains(mName)) {
      closeResources();
      throw new KijiTableNotFoundException(mTableURI);
    }

    mWriterFactory = new AsyncKijiWriterFactory(this);
    mReaderFactory = new AsyncKijiReaderFactory(this);

    mLayoutMonitor = layoutMonitor;
    mEntityIdFactory = createEntityIdFactory(mLayoutMonitor.getLayout());

    // Table is now open and must be released properly:
    mRetainCount.set(1);

    DebugResourceTracker.get().registerResource(this, mConstructorStack);

    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open KijiTable instance in state %s.", oldState);
  }

  /**
   * Constructs an Entity ID factory from a layout capsule.
   *
   * @param layout layout to construct an entity ID factory from.
   * @return a new entity ID factory as described from the table layout.
   */
  private static EntityIdFactory createEntityIdFactory(final KijiTableLayout layout) {
    final Object format = layout.getDesc().getKeysFormat();
    if (format instanceof RowKeyFormat) {
      return EntityIdFactory.getFactory((RowKeyFormat) format);
    } else if (format instanceof RowKeyFormat2) {
      return EntityIdFactory.getFactory((RowKeyFormat2) format);
    } else {
      throw new RuntimeException("Invalid Row Key format found in Kiji Table: " + format);
    }
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId(Object... kijiRowKey) {
    return mEntityIdFactory.getEntityId(kijiRowKey);
  }

  /** {@inheritDoc} */
  @Override
  public Kiji getKiji() {
    return mKiji;
  }

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return mName;
  }

  /** {@inheritDoc} */
  @Override
  public KijiURI getURI() {
    return mTableURI;
  }

  /**
   * Register a layout consumer that must be updated before this table will report that it has
   * completed a table layout update.  Sends the first update immediately before returning. The
   * returned registration object must be closed when layout updates are no longer needed.
   *
   * @param consumer the LayoutConsumer to be registered.
   * @return a registration object which must be closed when layout updates are no longer needed.
   * @throws IOException in case of an error updating the LayoutConsumer.
   */
  public Registration registerLayoutConsumer(LayoutConsumer consumer) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot register a new layout consumer to a KijiTable in state %s.", state);
    return mLayoutMonitor.registerLayoutConsumer(consumer);
  }

  /**
   * {@inheritDoc}
   * If you need both the table layout and a column name translator within a single short lived
   * operation, you should create the column name translator directly from the returned layout.
   */
  @Override
  public KijiTableLayout getLayout() {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get the layout of a table in state %s.", state);
    return mLayoutMonitor.getLayout();
  }

  /**
   * Get the column name translator for the current layout of this table.  Do not cache this object.
   * If you need both the table layout and a column name translator within a single short lived
   * operation, you should use {@link #getLayout()}} and create your own
   * {@link org.kiji.schema.layout.HBaseColumnNameTranslator} to ensure consistent state.
   * @return the column name translator for the current layout of this table.
   */
  public HBaseColumnNameTranslator getColumnNameTranslator() {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get the column name translator of a table in state %s.", state);
    return HBaseColumnNameTranslator.from(getLayout());
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableReader openTableReader() {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot open a table reader on a KijiTable in state %s.", state);
    try {
      return AsyncKijiTableReader.create(this);
    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableWriter openTableWriter() {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot open a table writer on a KijiTable in state %s.", state);
    try {
      return new AsyncKijiTableWriter(this);
    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }
  }

  /** {@inheritDoc} */
  @Override
  public KijiReaderFactory getReaderFactory() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get the reader factory for a KijiTable in state %s.", state);
    return mReaderFactory;
  }

  /** {@inheritDoc} */
  @Override
  public KijiWriterFactory getWriterFactory() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get the writer factory for a KijiTable in state %s.", state);
    return mWriterFactory;
  }

  /**
   * Not supported by AsyncKijiTable.
   */
  @Override
  public List<KijiRegion> getRegions() throws IOException {
    throw new UnsupportedOperationException("Not supported by AsyncKijiTable");
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableAnnotator openTableAnnotator() throws IOException {
    // TODO(gabe): Replace this with asynchbase
    throw new UnsupportedOperationException("Not yet implemented to work with AsyncHBase");

    /*
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get the TableAnnotator for a table in state: %s.", state);
    return new HBaseKijiTableAnnotator(this);
    */
  }

  /**
   * Releases the resources used by this table.
   *
   * @throws IOException on I/O error.
   */
  private void closeResources() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN || oldState == State.UNINITIALIZED,
        "Cannot close KijiTable instance %s in state %s.", this, oldState);
    LOG.debug("Closing AsyncHBaseKijiTable '{}'.", this);

    ResourceUtils.releaseOrLog(mKiji);
    if (oldState != State.UNINITIALIZED) {
      DebugResourceTracker.get().unregisterResource(this);
    }

    LOG.debug("AsyncHBaseKijiTable '{}' closed.", mTableURI);
  }

  /** {@inheritDoc} */
  @Override
  public KijiTable retain() {
    final int counter = mRetainCount.getAndIncrement();
    Preconditions.checkState(counter >= 1,
        "Cannot retain a closed KijiTable %s: retain counter was %s.", mTableURI, counter);
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public void release() throws IOException {
    final int counter = mRetainCount.decrementAndGet();
    Preconditions.checkState(counter >= 0,
        "Cannot release closed KijiTable %s: retain counter is now %s.", mTableURI, counter);
    if (counter == 0) {
      closeResources();
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (null == obj) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (!getClass().equals(obj.getClass())) {
      return false;
    }
    final KijiTable other = (KijiTable) obj;

    // Equal if the two tables have the same URI:
    return mTableURI.equals(other.getURI());
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return mTableURI.hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    String layoutId = mState.get() == State.OPEN
        ? mLayoutMonitor.getLayout().getDesc().getLayoutId()
        : "unknown";
    return Objects.toStringHelper(AsyncKijiTable.class)
        .add("id", System.identityHashCode(this))
        .add("uri", mTableURI)
        .add("retain_counter", mRetainCount.get())
        .add("layout_id", layoutId)
        .add("state", mState.get())
        .toString();
  }

  /**
   * When we know that a KijiTable is really a AsyncKijiTables
   * instance, this is a convenience method for downcasting, which
   * is common within the internals of Kiji code.
   *
   * @param kijiTable The Kiji table to downcast to an AsyncKijiTable.
   * @return The given Kiji table as an AsyncKijiTable.
   */
  public static AsyncKijiTable downcast(KijiTable kijiTable) {
    if (!(kijiTable instanceof AsyncKijiTable)) {
      // This should really never happen.  Something is seriously
      // wrong with Kiji code if we get here.
      throw new InternalKijiError(
          "Found a KijiTable object that was not an instance of AsyncKijiTable.");
    }
    return (AsyncKijiTable) kijiTable;
  }

  /**
   * Getter method for this instance's HBaseClient.
   *
   * @return The HBaseClient that manages this table.
   */
  public HBaseClient getHBClient() {
    return mHBClient;
  }
}
