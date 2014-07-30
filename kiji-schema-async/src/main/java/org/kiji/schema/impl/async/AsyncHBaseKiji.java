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
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.hbase.async.HBaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiNotInstalledException;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.ColumnDesc;
import org.kiji.schema.avro.FamilyDesc;
import org.kiji.schema.avro.LocalityGroupDesc;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.impl.Versions;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.layout.impl.InstanceMonitor;
import org.kiji.schema.security.KijiSecurityManager;
import org.kiji.schema.util.Debug;
import org.kiji.schema.util.DebugResourceTracker;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.schema.util.VersionInfo;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

/**
 * Kiji instance class that contains configuration and table information.
 * Multiple instances of Kiji can be installed onto a single HBase cluster.
 * This class represents a single one of those instances.
 *
 * <p>
 *   An opened Kiji instance ignores changes made to the system version, as seen by
 *   {@code Kiji.getSystemTable().getDataVersion()}.
 *   If the system version is modified, the opened Kiji instance should be closed and replaced with
 *   a new Kiji instance.
 * </p>
 */
@ApiAudience.Private
public final class AsyncHBaseKiji implements Kiji {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncHBaseKiji.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger("cleanup." + AsyncHBaseKiji.class.getName());
  private static final String ENABLE_CONSTRUCTOR_STACK_LOGGING_MESSAGE = String.format(
      "Enable DEBUG log level for logger: %s for a stack trace of the construction of this object.",
      CLEANUP_LOG.getName());

  /** The hadoop configuration. */
  private final Configuration mConf;

  /** HBaseClient for managing AsyncHBase tables */
  private final HBaseClient mHBClient;

  /** URI for this AsyncHBaseKiji instance. */
  private final KijiURI mURI;

  /** States of a Kiji instance. */
  private static enum State {
    /**
     * Initialization begun but not completed.  Retain counter and DebugResourceTracker counters
     * have not been incremented yet.
     */
    UNINITIALIZED,
    /**
     * Finished initialization.  Both retain counters and DebugResourceTracker counters have been
     * incremented.  Resources are successfully opened and this AsyncHBaseKiji's methods may be used.
     */
    OPEN,
    /**
     * Closed.  Other methods are no longer supported.  Resources and connections have been closed.
     */
    CLOSED
  }

  /** Tracks the state of this Kiji instance. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** Retain counter. When decreased to 0, the AsyncHBaseKiji may be closed and disposed of. */
  private final AtomicInteger mRetainCount = new AtomicInteger(0);

  /**
   * String representation of the call stack at the time this object is constructed.
   * Used for debugging
   **/
  private final String mConstructorStack;

  /** ZooKeeper client for this Kiji instance. */
  private final CuratorFramework mZKClient;

  /** Provides table layout updates and user registrations. */
  private final InstanceMonitor mInstanceMonitor;

  /**
   * Cached copy of the system version, oblivious to system table mutation while the connection to
   * this Kiji instance lives.
   * Internally, the Kiji instance must use this version instead of
   * {@code getSystemTable().getDataVersion()} to avoid inconsistent behaviors.
   */
  private final ProtocolVersion mSystemVersion;

  /** The schema table for this kiji instance. */
  private final KijiSchemaTable mSchemaTable;

  /** The system table for this kiji instance. The system table is always open. */
  private final KijiSystemTable mSystemTable;

  /** The meta table for this kiji instance. */
  private final KijiMetaTable mMetaTable;

  /**
   * The security manager for this instance, lazily initialized through {@link #getSecurityManager}.
   */
  private KijiSecurityManager mSecurityManager = null;

  AsyncHBaseKiji(KijiURI kijiURI) throws IOException {
    this(kijiURI, new Configuration(), new HBaseClient(kijiURI.getZooKeeperEnsemble()));
  }
  /**
   * Creates a new <code>AsyncHBaseKiji</code> instance.
   *
   * <p> Should only be used by Kiji.Factory.open().
   * <p> Caller does not need to use retain(), but must call release() when done with it.
   *
   * @param kijiURI the KijiURI.
   * @throws IOException on I/O error.
   */
  AsyncHBaseKiji(KijiURI kijiURI, Configuration conf, HBaseClient hbClient) throws IOException {
    // Deep copy the configuration.
    mConf = new Configuration(conf);

    // Validate arguments.
    mHBClient = Preconditions.checkNotNull(hbClient);
    mURI = Preconditions.checkNotNull(kijiURI);

    // Configure the ZooKeeper quorum:
    mConf.setStrings("hbase.zookeeper.quorum", mURI.getZookeeperQuorum().toArray(new String[0]));
    mConf.setInt("hbase.zookeeper.property.clientPort", mURI.getZookeeperClientPort());

    // Check for an instance name.
    Preconditions.checkArgument(mURI.getInstance() != null,
        "KijiURI '%s' does not specify a Kiji instance name.", mURI);


    if (LOG.isDebugEnabled()) {
      Debug.logConfiguration(mConf);
      LOG.debug(
          "Opening kiji instance '{}'"
              + " with client software version '{}'"
              + " and client data version '{}'.",
          mURI, VersionInfo.getSoftwareVersion(), VersionInfo.getClientDataVersion());
    }

    try {
      mSystemTable = new AsyncHBaseSystemTable(mURI, mHBClient);
    } catch (KijiNotInstalledException kie) {
      // Some clients handle this unchecked Exception so do the same here.
      close();
      throw kie;
    }
    mSchemaTable = new AsyncHBaseSchemaTable(mURI, mHBClient);
    mMetaTable = new AsyncHBaseMetaTable(mURI, mSchemaTable, mHBClient);

    LOG.debug("Kiji instance '{}' is now opened.", mURI);

    mSystemVersion = mSystemTable.getDataVersion();
    LOG.debug("Kiji instance '{}' has data version '{}'.", mURI, mSystemVersion);

    // Make sure the data version for the client matches the cluster.
    LOG.debug("Validating version for Kiji instance '{}'.", mURI);
    try {
      VersionInfo.validateVersion(mSystemTable);
    } catch (IOException ioe) {
      // If an IOException occurred the object will not be constructed so need to clean it up.
      close();
      throw ioe;
    } catch (KijiNotInstalledException kie) {
      // Some clients handle this unchecked Exception so do the same here.
      close();
      throw kie;
    }

    if (mSystemVersion.compareTo(Versions.MIN_SYS_VER_FOR_LAYOUT_VALIDATION) >= 0) {
      // system-2.0 clients must connect to ZooKeeper:
      //  - to register themselves as table users;
      //  - to receive table layout updates.
      mZKClient = ZooKeeperUtils.getZooKeeperClient(mURI);
    } else {
      // system-1.x clients do not need a ZooKeeper connection.
      mZKClient = null;
    }

    mInstanceMonitor = new InstanceMonitor(
        mSystemVersion,
        mURI,
        mSchemaTable,
        mMetaTable,
        mZKClient);
    mInstanceMonitor.start();

    mRetainCount.set(1);
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open Kiji instance in state %s.", oldState);

    mConstructorStack = (CLEANUP_LOG.isDebugEnabled())
        ? Debug.getStackTrace()
        : ENABLE_CONSTRUCTOR_STACK_LOGGING_MESSAGE;
    DebugResourceTracker.get().registerResource(this, mConstructorStack);


  }

  /**
   * <p>
   *   Ensures that a table is not created or modified to enable layout validation without the
   *   requisite system version.
   * </p>
   *
   * <p>
   *   Throws an exception if a table layout has validation enabled, but the overall instance data
   *   version is too low to support table layout validation.
   * </p>
   *
   * <p>
   *   Table layouts with layout version <tt>layout-1.3.0</tt> or higher must be applied to systems
   *   with data version <tt>system-2.0</tt> or higher. A layout of 1.3 or above in system-1.0
   *   environment will trigger an exception in this method.
   * </p>
   *
   * <p>
   *   Older layout versions may be applied in <tt>system-1.0</tt> or <tt>system-2.0</tt>
   *   environments; such layouts are ignored by this method.
   * </p>
   *
   * @param layout the table layout for which to ensure compatibility.
   * @throws IOException in case of an error reading from the system table.
   * @throws InvalidLayoutException if the layout and system versions are incompatible.
   */
  private void ensureValidationCompatibility(TableLayoutDesc layout) throws IOException {
    final ProtocolVersion layoutVersion = ProtocolVersion.parse(layout.getVersion());
    final ProtocolVersion systemVersion = getSystemTable().getDataVersion();

    if ((layoutVersion.compareTo(Versions.LAYOUT_VALIDATION_VERSION) >= 0)
        && (systemVersion.compareTo(Versions.MIN_SYS_VER_FOR_LAYOUT_VALIDATION) < 0)) {
      throw new InvalidLayoutException(
          String.format("Layout version: %s not supported by system version: %s",
              layoutVersion, systemVersion));
    }
  }

  /**
   * {@inheritDoc}
   * AsyncHBaseKiji instances do not support Hadoop configuration.
   * */
  @Override
  public Configuration getConf() {
    throw new UnsupportedOperationException(
        "AsyncHBaseKiji instances do not support Hadoop configuration");
  }

  /** {@inheritDoc} */
  @Override
  public KijiURI getURI() {
    return mURI;
  }

  /** {@inheritDoc} */
  @Override
  public KijiSchemaTable getSchemaTable() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get schema table for Kiji instance %s in state %s.", this, state);
    return mSchemaTable;
  }

  /** {@inheritDoc} */
  @Override
  public KijiSystemTable getSystemTable() {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get system table for Kiji instance %s in state %s.", this, state);
    return mSystemTable;
  }

  /** {@inheritDoc} */
  @Override
  public KijiMetaTable getMetaTable() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get meta table for Kiji instance %s in state %s.", this, state);
    return mMetaTable;
  }

  public HBaseClient getHBaseClient() {
    return mHBClient;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isSecurityEnabled() throws IOException {
    return mSystemTable.getSecurityVersion().compareTo(Versions.MIN_SECURITY_VERSION) >= 0;
  }

  /**
   * Sets the flush interval for the HBaseClient.
   *
   * @param flushInterval the new flush interval
   * @return the old flush interval
   */
  public short setFlushInterval(short flushInterval) {
    return mHBClient.setFlushInterval(flushInterval);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized KijiSecurityManager getSecurityManager() throws IOException {
    // TODO(gabe): Replace this with asynchbase
    throw new UnsupportedOperationException("Not yet implemented to work with AsyncHBase");
    /*
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get security manager for Kiji instance %s in state %s.", this, state);
    if (null == mSecurityManager) {
      if (isSecurityEnabled()) {
        mSecurityManager = KijiSecurityManager.Factory.create(mURI, getConf(), mHTableFactory);
      } else {
        throw new KijiSecurityException("Cannot create a KijiSecurityManager for security version "
            + mSystemTable.getSecurityVersion() + ". Version must be "
            + Versions.MIN_SECURITY_VERSION + " or higher.");
      }
    }
    return mSecurityManager;
    */
  }

  /** {@inheritDoc} */
  @Override
  public KijiTable openTable(String tableName) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot open table in Kiji instance %s in state %s.", this, state);

    if (!getTableNames().contains(tableName)) {
      throw new KijiTableNotFoundException(
          KijiURI.newBuilder(mURI).withTableName(tableName).build());
    }

    return new AsyncHBaseKijiTable(
        this,
        tableName,
        mHBClient,
        mInstanceMonitor.getTableLayoutMonitor(tableName));
  }

  /**
   * {@inheritDoc}
   * Table creation is not supported by AsyncHBaseKiji.
   * */
  @Deprecated
  @Override
  public void createTable(String tableName, KijiTableLayout tableLayout)
      throws IOException {
    throw new UnsupportedOperationException("Table creation is outside the scope of async-kiji");
  }

  /**
   * {@inheritDoc}
   * Table creation is not supported by AsyncHBaseKiji.
   * */
  @Override
  public void createTable(TableLayoutDesc tableLayout)
      throws IOException {
    throw new UnsupportedOperationException("Table creation is outside the scope of async-kiji");
  }

  /**
   * {@inheritDoc}
   * Table creation is not supported by AsyncHBaseKiji.
   * */
  @Deprecated
  @Override
  public void createTable(String tableName, KijiTableLayout tableLayout, int numRegions)
      throws IOException {
    throw new UnsupportedOperationException("Table creation is outside the scope of async-kiji");
  }

  /**
   * {@inheritDoc}
   * Table creation is not supported by AsyncHBaseKiji.
   * */
  @Override
  public void createTable(TableLayoutDesc tableLayout, int numRegions)
      throws IOException {
    throw new UnsupportedOperationException("Table creation is outside the scope of async-kiji");
  }

  /**
   * {@inheritDoc}
   * Table creation is not supported by AsyncHBaseKiji.
   * */
  @Deprecated
  @Override
  public void createTable(String tableName, KijiTableLayout tableLayout, byte[][] splitKeys)
      throws IOException {
    throw new UnsupportedOperationException("Table creation is outside the scope of async-kiji");
  }

  /**
   * {@inheritDoc}
   * Table creation is not supported by AsyncHBaseKiji.
   * */
  @Override
  public void createTable(TableLayoutDesc tableLayout, byte[][] splitKeys) throws IOException {
    throw new UnsupportedOperationException("Table creation is outside the scope of async-kiji");
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public KijiTableLayout modifyTableLayout(String tableName, TableLayoutDesc update)
      throws IOException {
    if (!tableName.equals(update.getName())) {
      throw new InvalidLayoutException(String.format(
          "Name of table in descriptor '%s' does not match table name '%s'.",
          update.getName(), tableName));
    }

    return modifyTableLayout(update);
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableLayout modifyTableLayout(TableLayoutDesc update) throws IOException {
    return modifyTableLayout(update, false, null);
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public KijiTableLayout modifyTableLayout(
      String tableName,
      TableLayoutDesc update,
      boolean dryRun,
      PrintStream printStream)
      throws IOException {
    if (!tableName.equals(update.getName())) {
      throw new InvalidLayoutException(String.format(
          "Name of table in descriptor '%s' does not match table name '%s'.",
          update.getName(), tableName));
    }

    return modifyTableLayout(update, dryRun, printStream);
  }

  /** {@inheritDoc}
   *
   *  <p>Note: AsyncHBaseKiji only supports updates to the {@code KijiTableLayout} that
   *  do not require adding or removing any locality groups, families, or columns.</p>
   * */
  @Override
  public KijiTableLayout modifyTableLayout(
      TableLayoutDesc update,
      boolean dryRun,
      PrintStream printStream)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot modify table layout in Kiji instance %s in state %s.", this, state);
    Preconditions.checkNotNull(update);

    ensureValidationCompatibility(update);

    if (dryRun && (null == printStream)) {
      printStream = System.out;
    }

    final KijiMetaTable metaTable = getMetaTable();

    final String tableName = update.getName();
    // Throws a KijiTableNotFoundException if there is no table.
    metaTable.getTableLayout(tableName);

    final KijiURI tableURI = KijiURI.newBuilder(mURI).withTableName(tableName).build();
    LOG.debug("Applying layout update {} on table {}", update, tableURI);

    KijiTableLayout newLayout = null;

    // Check to see if update requires adding or removing something
    final List<KijiTableLayout> currentLayouts = metaTable.getTableLayoutVersions(tableName, 1);
    final KijiTableLayout currentLayout = currentLayouts.isEmpty() ? null : currentLayouts.get(0);
    final Map<String, LocalityGroupLayout> currentLocalityGroupMap =
        currentLayout.getLocalityGroupMap();
    for (LocalityGroupDesc locGroupDesc : update.getLocalityGroups()) {
      if (!currentLocalityGroupMap.containsKey(locGroupDesc.getName())
          || locGroupDesc.getDelete()) {
        throw new UnsupportedOperationException("AsyncHBaseKiji cannot add or remove locality groups");
      }
      else {
        final Map<String, FamilyLayout> currentFamilyLayoutMap =
            currentLocalityGroupMap.get(locGroupDesc.getName()).getFamilyMap();
        for (FamilyDesc familyDesc : locGroupDesc.getFamilies()) {
          if (!currentFamilyLayoutMap.containsKey(familyDesc.getName())
              || familyDesc.getDelete()) {
            throw new UnsupportedOperationException("AsyncHBaseKiji cannot add or remove families");
          }
          else {
            final Map<String, ColumnLayout> currentColumnLayout =
                currentFamilyLayoutMap.get(familyDesc.getName()).getColumnMap();
            for (ColumnDesc columnDesc : familyDesc.getColumns()) {
              if (!currentColumnLayout.containsKey(columnDesc.getName())
                  || columnDesc.getDelete()) {
                throw new UnsupportedOperationException("AsyncHBaseKiji cannot add or remove columns");
              }
            }
          }

        }
      }

    }

    if (dryRun) {
      // Process column ids and perform validation, but don't actually update the meta table.
      newLayout = KijiTableLayout.createUpdatedLayout(update, currentLayout);
    } else {
      // Actually set it.
      if (mSystemVersion.compareTo(Versions.SYSTEM_2_0) >= 0) {
        try {
          // Use ZooKeeper to inform all watchers that a new table layout is available.
          final AsyncHBaseTableLayoutUpdater updater =
              new AsyncHBaseTableLayoutUpdater(this, tableURI, update);
          try {
            updater.update();
            newLayout = updater.getNewLayout();
          } finally {
            updater.close();
          }
        } catch (KeeperException ke) {
          throw new IOException(ke);
        }
      } else {
        // System versions before system-2.0 do not enforce table layout update consistency or
        // validation.
        newLayout = metaTable.updateTableLayout(tableName, update);
      }
    }
    Preconditions.checkNotNull(newLayout);

    if (dryRun) {
      printStream.println("This table layout is valid.");
    }
    return newLayout;
  }

  /**
   * {@inheritDoc}
   * Table deletion is not supported by AsyncHBaseKiji.
   * */
  @Override
  public void deleteTable(String tableName) throws IOException {
    throw new UnsupportedOperationException("Table deletion is outside the scope of async-kiji");
  }

  /** {@inheritDoc} */
  @Override
  public List<String> getTableNames() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get table names in Kiji instance %s in state %s.", this, state);
    return getMetaTable().listTables();
  }

  /**
   * Releases all the resources used by this Kiji instance.
   *
   * @throws IOException on I/O error.
   */
  private void close() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN || oldState == State.UNINITIALIZED,
        "Cannot close Kiji instance %s in state %s.", this, oldState);

    LOG.debug("Closing {}.", this);

    ResourceUtils.closeOrLog(mSystemTable);
    ResourceUtils.closeOrLog(mSchemaTable);
    ResourceUtils.closeOrLog(mMetaTable);
    ResourceUtils.closeOrLog(mInstanceMonitor);

    synchronized (this) {
      ResourceUtils.closeOrLog(mSecurityManager);
      mSecurityManager = null;
    }

    ResourceUtils.closeOrLog(mZKClient);

    if (oldState != State.UNINITIALIZED) {
      DebugResourceTracker.get().unregisterResource(this);
    }
    LOG.debug("{} closed.", this);
    mHBClient.shutdown();
  }

  /** {@inheritDoc} */
  @Override
  public Kiji retain() {
    LOG.debug("Retaining {}.", this);
    final int counter = mRetainCount.getAndIncrement();
    Preconditions.checkState(counter >= 1,
        "Cannot retain Kiji instance %s: retain counter was %s.", this, counter);
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public void release() throws IOException {
    LOG.debug("Releasing {}", this);
    final int counter = mRetainCount.decrementAndGet();
    Preconditions.checkState(counter >= 0,
        "Cannot release Kiji instance %s: retain counter is now %s.", this, counter);
    if (counter == 0) {
      close();
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
    final Kiji other = (Kiji) obj;

    // Equal if the two instances have the same URI:
    return mURI.equals(other.getURI());
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return mURI.hashCode();
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    final State state = mState.get();
    if (state != State.CLOSED) {
      CLEANUP_LOG.warn("Finalizing unreleased AsyncHBaseKiji instance {} in state {}.", this, state);
      if (CLEANUP_LOG.isDebugEnabled()) {
        CLEANUP_LOG.debug(
            "AsyncHBaseKiji '{}' was constructed through:\n{}",
            mURI,
            mConstructorStack);
      }
      close();
    }
    super.finalize();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(AsyncHBaseKiji.class)
        .add("id", System.identityHashCode(this))
        .add("uri", mURI)
        .add("retain-count", mRetainCount)
        .add("state", mState.get())
        .toString();
  }

  /**
   * Returns the ZooKeeper client for this Kiji instance.
   *
   * @return the ZooKeeper client for this Kiji instance.
   *     Null if the data version &le; {@code system-2.0}.
   */
  CuratorFramework getZKClient() {
    return mZKClient;
  }

}
