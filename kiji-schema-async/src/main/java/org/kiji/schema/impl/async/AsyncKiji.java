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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
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
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.impl.Versions;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.InstanceMonitor;
import org.kiji.schema.security.KijiSecurityManager;
import org.kiji.schema.util.Debug;
import org.kiji.schema.util.DebugResourceTracker;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.schema.util.VersionInfo;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

// TODO(gabe): REMOVE THESE

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
public final class AsyncKiji implements Kiji {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncKiji.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger("cleanup." + AsyncKiji.class.getName());
  private static final String ENABLE_CONSTRUCTOR_STACK_LOGGING_MESSAGE = String.format(
      "Enable DEBUG log level for logger: %s for a stack trace of the construction of this object.",
      CLEANUP_LOG.getName());

  /** HBaseClient for managing AsyncHBase tables */
  private final HBaseClient mHBClient;

  // TODO(gabe): DELETE THIS! PART OF HACK!
  /** Factory for HTable instances. */
  //private final HTableInterfaceFactory mHTableFactory;
  private final Kiji mKiji;

  /** URI for this HBaseKiji instance. */
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
     * incremented.  Resources are successfully opened and this HBaseKiji's methods may be used.
     */
    OPEN,
    /**
     * Closed.  Other methods are no longer supported.  Resources and connections have been closed.
     */
    CLOSED
  }

  /** Tracks the state of this Kiji instance. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** Retain counter. When decreased to 0, the HBase Kiji may be closed and disposed of. */
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

  /**
   * Creates a new <code>HBaseKiji</code> instance.
   *
   * <p> Should only be used by Kiji.Factory.open().
   * <p> Caller does not need to use retain(), but must call release() when done with it.
   *
   * @param kijiURI the KijiURI.
   * @throws IOException on I/O error.
   */
  AsyncKiji(KijiURI kijiURI) throws IOException {
    mURI = Preconditions.checkNotNull(kijiURI);

    // TODO(gabe): TOTAL HACK! FIX THIS ASAP!!
    mKiji = Kiji.Factory.open(mURI);
    mHBClient = new HBaseClient(mURI.getZooKeeperEnsemble());

    // Validate arguments.
    // TODO(gabe): Update to work with AsyncHBase
    //mHTableFactory = Preconditions.checkNotNull(tableFactory);

    // Check for an instance name.
    Preconditions.checkArgument(mURI.getInstance() != null,
        "KijiURI '%s' does not specify a Kiji instance name.", mURI);

    try {
      // TODO(gabe): REPLACE THESE!!
      mSystemTable = mKiji.getSystemTable();
    } catch (KijiNotInstalledException kie) {
      // Some clients handle this unchecked Exception so do the same here.
      close();
      throw kie;
    }
    // TODO(gabe): REPLACE THESE!
    mSchemaTable = mKiji.getSchemaTable();
    mMetaTable = mKiji.getMetaTable();

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
   * AsyncKiji instances do not support Hadoop configuration.
   * */
  @Override
  public Configuration getConf() {
    throw new UnsupportedOperationException("AsyncKiji instances do not support Hadoop configuration");
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

  /** {@inheritDoc} */
  @Override
  public boolean isSecurityEnabled() throws IOException {
    return mSystemTable.getSecurityVersion().compareTo(Versions.MIN_SECURITY_VERSION) >= 0;
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

    return new AsyncKijiTable(
        this,
        tableName,
        mHBClient,
        mInstanceMonitor.getTableLayoutMonitor(tableName));
  }

  /**
   * {@inheritDoc}
   * Table creation is not supported by AsyncKiji.
   * */
  @Deprecated
  @Override
  public void createTable(String tableName, KijiTableLayout tableLayout)
      throws IOException {
    throw new UnsupportedOperationException("Table creation is outside the scope of async-kiji");
  }

  /**
   * {@inheritDoc}
   * Table creation is not supported by AsyncKiji.
   * */
  @Override
  public void createTable(TableLayoutDesc tableLayout)
      throws IOException {
    throw new UnsupportedOperationException("Table creation is outside the scope of async-kiji");
  }

  /**
   * {@inheritDoc}
   * Table creation is not supported by AsyncKiji.
   * */
  @Deprecated
  @Override
  public void createTable(String tableName, KijiTableLayout tableLayout, int numRegions)
      throws IOException {
    throw new UnsupportedOperationException("Table creation is outside the scope of async-kiji");
  }

  /**
   * {@inheritDoc}
   * Table creation is not supported by AsyncKiji.
   * */
  @Override
  public void createTable(TableLayoutDesc tableLayout, int numRegions)
      throws IOException {
    throw new UnsupportedOperationException("Table creation is outside the scope of async-kiji");
  }

  /**
   * {@inheritDoc}
   * Table creation is not supported by AsyncKiji.
   * */
  @Deprecated
  @Override
  public void createTable(String tableName, KijiTableLayout tableLayout, byte[][] splitKeys)
      throws IOException {
    throw new UnsupportedOperationException("Table creation is outside the scope of async-kiji");
  }

  /**
   * {@inheritDoc}
   * Table creation is not supported by AsyncKiji.
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

  // CSOFF: MethodLength
  /** {@inheritDoc} */
  @Override
  public KijiTableLayout modifyTableLayout(
      TableLayoutDesc update,
      boolean dryRun,
      PrintStream printStream)
      throws IOException {
    // TODO(gabe): Replace this with asynchbase
    throw new UnsupportedOperationException("Not yet implemented to work with AsyncHBase");

    /*
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

    if (dryRun) {
      // Process column ids and perform validation, but don't actually update the meta table.
      final List<KijiTableLayout> layouts = metaTable.getTableLayoutVersions(tableName, 1);
      final KijiTableLayout currentLayout = layouts.isEmpty() ? null : layouts.get(0);
      newLayout = KijiTableLayout.createUpdatedLayout(update, currentLayout);
    } else {
      // Actually set it.
      if (mSystemVersion.compareTo(Versions.SYSTEM_2_0) >= 0) {
        try {
          // Use ZooKeeper to inform all watchers that a new table layout is available.
          final HBaseTableLayoutUpdater updater =
              new HBaseTableLayoutUpdater(this, tableURI, update);
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

    LOG.debug("Computing new HBase schema");
    final HTableSchemaTranslator translator = new HTableSchemaTranslator();
    final HTableDescriptor newTableDescriptor =
        translator.toHTableDescriptor(mURI.getInstance(), newLayout);

    LOG.debug("Reading existing HBase schema");
    final KijiManagedHBaseTableName hbaseTableName =
        KijiManagedHBaseTableName.getKijiTableName(mURI.getInstance(), tableName);
    HTableDescriptor currentTableDescriptor = null;
    byte[] tableNameAsBytes = hbaseTableName.toBytes();
    try {
      currentTableDescriptor = getHBaseAdmin().getTableDescriptor(tableNameAsBytes);
    } catch (TableNotFoundException tnfe) {
      if (!dryRun) {
        throw tnfe; // Not in dry-run mode; table needs to exist. Rethrow exception.
      }
    }
    if (currentTableDescriptor == null) {
      if (dryRun) {
        printStream.println("Would create new table: " + tableName);
        currentTableDescriptor = HTableDescriptorComparator.makeEmptyTableDescriptor(
            hbaseTableName);
      } else {
        throw new RuntimeException("Table " + hbaseTableName.getKijiTableName()
            + " does not exist");
      }
    }
    LOG.debug("Existing table descriptor: {}", currentTableDescriptor);
    LOG.debug("New table descriptor: {}", newTableDescriptor);

    LOG.debug("Checking for differences between the new HBase schema and the existing one");
    final HTableDescriptorComparator comparator = new HTableDescriptorComparator();
    if (0 == comparator.compare(currentTableDescriptor, newTableDescriptor)) {
      LOG.debug("HBase schemas are the same.  No need to change HBase schema");
      if (dryRun) {
        printStream.println("This layout does not require any physical table schema changes.");
      }
    } else {
      LOG.debug("HBase schema must be changed, but no columns will be deleted");

      if (dryRun) {
        printStream.println("Changes caused by this table layout:");
      } else {
        LOG.debug("Disabling HBase table");
        getHBaseAdmin().disableTable(hbaseTableName.toString());
      }

      for (HColumnDescriptor newColumnDescriptor : newTableDescriptor.getFamilies()) {
        final String columnName = Bytes.toString(newColumnDescriptor.getName());
        final ColumnId columnId = ColumnId.fromString(columnName);
        final String lgName = newLayout.getLocalityGroupIdNameMap().get(columnId);
        final HColumnDescriptor currentColumnDescriptor =
            currentTableDescriptor.getFamily(newColumnDescriptor.getName());
        if (null == currentColumnDescriptor) {
          if (dryRun) {
            printStream.println("  Creating new locality group: " + lgName);
          } else {
            LOG.debug("Creating new column " + columnName);
            getHBaseAdmin().addColumn(hbaseTableName.toString(), newColumnDescriptor);
          }
        } else if (!newColumnDescriptor.equals(currentColumnDescriptor)) {
          if (dryRun) {
            printStream.println("  Modifying locality group: " + lgName);
          } else {
            LOG.debug("Modifying column " + columnName);
            getHBaseAdmin().modifyColumn(hbaseTableName.toString(), newColumnDescriptor);
          }
        } else {
          LOG.debug("No changes needed for column " + columnName);
        }
      }

      if (dryRun) {
        if (newTableDescriptor.getMaxFileSize() != currentTableDescriptor.getMaxFileSize()) {
          printStream.printf("  Changing max_filesize from %d to %d: %n",
            currentTableDescriptor.getMaxFileSize(),
            newTableDescriptor.getMaxFileSize());
        }
        if (newTableDescriptor.getMaxFileSize() != currentTableDescriptor.getMaxFileSize()) {
          printStream.printf("  Changing memstore_flushsize from %d to %d: %n",
            currentTableDescriptor.getMemStoreFlushSize(),
            newTableDescriptor.getMemStoreFlushSize());
        }
      } else {
        LOG.debug("Modifying table descriptor");
        getHBaseAdmin().modifyTable(tableNameAsBytes, newTableDescriptor);
      }

      if (!dryRun) {
        LOG.debug("Re-enabling HBase table");
        getHBaseAdmin().enableTable(hbaseTableName.toString());
      }
    }

    return newLayout;
    */
  }
  // CSON: MethodLength

  /**
   * {@inheritDoc}
   * Table deletion is not supported by AsyncKiji.
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

    ResourceUtils.closeOrLog(mInstanceMonitor);
    ResourceUtils.releaseOrLog(mKiji);

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
      CLEANUP_LOG.warn("Finalizing unreleased HBaseKiji instance {} in state {}.", this, state);
      if (CLEANUP_LOG.isDebugEnabled()) {
        CLEANUP_LOG.debug(
            "HBaseKiji '{}' was constructed through:\n{}",
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
    return Objects.toStringHelper(AsyncKiji.class)
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
