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
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiAlreadyExistsException;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiNotInstalledException;
import org.kiji.schema.KijiRowKeySplitter;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.impl.Versions;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.InstanceMonitor;
import org.kiji.schema.security.CassandraKijiSecurityManager;
import org.kiji.schema.security.KijiSecurityException;
import org.kiji.schema.security.KijiSecurityManager;
import org.kiji.schema.util.DebugResourceTracker;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.schema.util.VersionInfo;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

/**
 * Kiji instance class that contains configuration and table information.
 * Multiple instances of Kiji can be installed onto a single C* cluster.
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
public final class CassandraKiji implements Kiji {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKiji.class);

  /** Factory for CassandraTableInterface instances. */
  private final CassandraAdmin mAdmin;

  /** URI for this CassandraKiji instance. */
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
     * incremented.  Resources are successfully opened and this Kiji's methods may be used.
     */
    OPEN,
    /**
     * Closed.  Other methods are no longer supported.  Resources and connections have been closed.
     */
    CLOSED
  }

  /** Tracks the state of this Kiji instance. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** Retain counter. When decreased to 0, the C* Kiji may be closed and disposed of. */
  private final AtomicInteger mRetainCount = new AtomicInteger(0);

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

  /** The schema table for this kiji instance, or null if it has not been opened yet. */
  private final CassandraSchemaTable mSchemaTable;

  /** The system table for this kiji instance. The system table is always open. */
  private final CassandraSystemTable mSystemTable;

  /** The meta table for this kiji instance, or null if it has not been opened yet. */
  private final CassandraMetaTable mMetaTable;

  /**
   * The security manager for this instance, lazily initialized through {@link #getSecurityManager}.
   */
  private KijiSecurityManager mSecurityManager = null;

  /**
   * Creates a new <code>CassandraKiji</code> instance.
   *
   * <p> Should only be used by Kiji.Factory.open().
   * <p> Caller does not need to use retain(), but must call release() when done with it.
   *
   * @param kijiURI the KijiURI.
   * @param admin CassandraAdmin wrapper around open C* session.
   * @throws java.io.IOException on I/O error.
   */
  CassandraKiji(KijiURI kijiURI, CassandraAdmin admin) throws IOException {

    // Validate arguments.
    mAdmin = Preconditions.checkNotNull(admin);
    mURI = Preconditions.checkNotNull(kijiURI);

    // Check for an instance name.
    Preconditions.checkArgument(mURI.getInstance() != null,
        "KijiURI '%s' does not specify a Kiji instance name.", mURI);

    LOG.debug(
        "Opening Kiji instance {} with client software version {} and client data version {}.",
        mURI, VersionInfo.getSoftwareVersion(), VersionInfo.getClientDataVersion());

    try {
      mSystemTable = new CassandraSystemTable(mURI, mAdmin);
    } catch (KijiNotInstalledException kie) {
      // Some clients handle this unchecked Exception so do the same here.
      close();
      throw kie;
    }

    mSchemaTable = new CassandraSchemaTable(mAdmin, mURI);
    mMetaTable = new CassandraMetaTable(mURI, mAdmin, mSchemaTable);

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

    DebugResourceTracker.get().registerResource(this);
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
   * @throws java.io.IOException in case of an error reading from the system table.
   * @throws org.kiji.schema.layout.InvalidLayoutException if the layout and system versions are
   * incompatible.
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

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    throw new UnsupportedOperationException(
        "Cassandra-backed Kiji instances do not support Hadoop configuration.");
  }

  /** {@inheritDoc} */
  @Override
  public KijiURI getURI() {
    return mURI;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized KijiSchemaTable getSchemaTable() throws IOException {
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
  public synchronized KijiMetaTable getMetaTable() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get meta table for Kiji instance %s in state %s.", this, state);
    return mMetaTable;
  }

  /**
   * Gets the current CassandraAdmin instance for this Kiji. This method will open a new
   * CassandraAdmin if one doesn't exist already.
   *
   * @throws java.io.IOException If there is an error opening the CassandraAdmin.
   * @return The current CassandraAdmin instance for this Kiji.
   */
  public synchronized CassandraAdmin getCassandraAdmin() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get HBase admin for Kiji instance %s in state %s.", this, state);
    return mAdmin;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isSecurityEnabled() throws IOException {
    return mSystemTable.getSecurityVersion().compareTo(Versions.MIN_SECURITY_VERSION) >= 0;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized KijiSecurityManager getSecurityManager() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get security manager for Kiji instance %s in state %s.", this, state);
    if (null == mSecurityManager) {
      if (isSecurityEnabled()) {
        mSecurityManager = CassandraKijiSecurityManager.create(mURI, mAdmin);
      } else {
        throw new KijiSecurityException("Cannot create a KijiSecurityManager for security version "
            + mSystemTable.getSecurityVersion() + ". Version must be "
            + Versions.MIN_SECURITY_VERSION + " or higher.");
      }
    }
    return mSecurityManager;
  }

  /** {@inheritDoc} */
  @Override
  public KijiTable openTable(String tableName) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot open table in Kiji instance %s in state %s.", this, state);
    return new CassandraKijiTable(
        this,
        tableName,
        mAdmin,
        mInstanceMonitor.getTableLayoutMonitor(tableName));
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public void createTable(String tableName, KijiTableLayout tableLayout)
      throws IOException {
    if (!tableName.equals(tableLayout.getName())) {
      throw new RuntimeException(String.format(
          "Table name from layout descriptor '%s' does match table name '%s'.",
          tableLayout.getName(), tableName));
    }

    createTable(tableLayout.getDesc());
  }

  /** {@inheritDoc} */
  @Override
  public void createTable(TableLayoutDesc tableLayout)
      throws IOException {
    createTable(tableLayout, 1);
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public void createTable(String tableName, KijiTableLayout tableLayout, int numRegions)
      throws IOException {
    if (!tableName.equals(tableLayout.getName())) {
      throw new RuntimeException(String.format(
          "Table name from layout descriptor '%s' does match table name '%s'.",
          tableLayout.getName(), tableName));
    }

    createTable(tableLayout.getDesc(), numRegions);
  }

  /** {@inheritDoc} */
  @Override
  // TODO: Remove for C*, numRegions don't make sense
  public void createTable(TableLayoutDesc tableLayout, int numRegions)
      throws IOException {
    Preconditions.checkArgument((numRegions >= 1), "numRegions must be positive: " + numRegions);
    if (numRegions > 1) {
      if (KijiTableLayout.getEncoding(tableLayout.getKeysFormat())
          == RowKeyEncoding.RAW) {
        throw new IllegalArgumentException(
            "May not use numRegions > 1 if row key hashing is disabled in the layout");
      }

      createTable(tableLayout, KijiRowKeySplitter.get().getSplitKeys(numRegions,
          KijiRowKeySplitter.getRowKeyResolution(tableLayout)));
    } else {
      createTable(tableLayout, null);
    }
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  // TODO: Remove for C*, splitKeys don't make sense
  public void createTable(String tableName, KijiTableLayout tableLayout, byte[][] splitKeys)
      throws IOException {
    if (getMetaTable().tableExists(tableName)) {
      final KijiURI tableURI =
          KijiURI.newBuilder(mURI).withTableName(tableName).build();
      throw new KijiAlreadyExistsException(String.format(
          "Kiji table '%s' already exists.", tableURI), tableURI);
    }

    if (!tableName.equals(tableLayout.getName())) {
      throw new RuntimeException(String.format(
          "Table name from layout descriptor '%s' does match table name '%s'.",
          tableLayout.getName(), tableName));
    }

    createTable(tableLayout.getDesc(), splitKeys);
  }

  /** {@inheritDoc} */
  @Override
  // TODO: Remove for C*, splitKeys don't make sense
  public void createTable(TableLayoutDesc tableLayout, byte[][] splitKeys) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot create table in Kiji instance %s in state %s.", this, state);

    final KijiURI tableURI = KijiURI.newBuilder(mURI).withTableName(tableLayout.getName()).build();
    LOG.debug("Creating Kiji table '{}'.", tableURI);

    ensureValidationCompatibility(tableLayout);

    // If security is enabled, apply the permissions to the new table.
    if (isSecurityEnabled()) {
      getSecurityManager().lock();
      try {
        createTableUnchecked(tableLayout);
        getSecurityManager().applyPermissionsToNewTable(tableURI);
      } finally {
        getSecurityManager().unlock();
      }
    } else {
      createTableUnchecked(tableLayout);
    }
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
  // TODO: Implement C* version
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

    // Note that a Cassandra table layout modification should never require a schema change
    // in the underlying Cassandra table, unless we are adding or removing a counter.

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
          final CassandraTableLayoutUpdater updater =
              new CassandraTableLayoutUpdater(this, tableURI, update);
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
    Preconditions.checkState(newLayout != null);

    if (dryRun) {
      printStream.println("This table layout is valid.");
    }

    if (dryRun) {
      printStream.println("No changes possible for Cassandra-backed Kiji tables.");
    }

    return newLayout;
  }
  // CSON: MethodLength

  /** {@inheritDoc} */
  @Override
  public void deleteTable(String tableName) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete table in Kiji instance %s in state %s.", this, state);
    // Delete from Cassandra.
    final KijiURI tableURI = KijiURI.newBuilder(mURI).withTableName(tableName).build();
    final CassandraTableName mainTable = CassandraTableName.getKijiTableName(tableURI);
    final CassandraTableName counterTable = CassandraTableName.getKijiCounterTableName(tableURI);
    CassandraAdmin admin = getCassandraAdmin();

    admin.disableTable(mainTable);
    admin.deleteTable(mainTable);

    admin.disableTable(counterTable);
    admin.deleteTable(counterTable);

    // Delete from the meta table.
    getMetaTable().deleteTable(tableName);

    // If the table persists immediately after deletion attempt, then give up.
    if (admin.tableExists(mainTable)) {
      LOG.warn("C* table " + mainTable + " survives deletion attempt. Giving up...");
    }
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
   * @throws java.io.IOException on I/O error.
   */
  private void close() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN || oldState == State.UNINITIALIZED,
        "Cannot close Kiji instance %s in state %s.", this, oldState);

    LOG.debug("Closing {}.", this);

    ResourceUtils.closeOrLog(mInstanceMonitor);
    ResourceUtils.closeOrLog(mMetaTable);
    ResourceUtils.closeOrLog(mSystemTable);
    ResourceUtils.closeOrLog(mSchemaTable);
    ResourceUtils.closeOrLog(mAdmin);

    synchronized (this) {
      ResourceUtils.closeOrLog(mSecurityManager);
    }

    ResourceUtils.closeOrLog(mZKClient);

    if (oldState != State.UNINITIALIZED) {
      DebugResourceTracker.get().unregisterResource(this);
    }

    LOG.debug("{} closed.", this);
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
  public String toString() {
    return Objects.toStringHelper(CassandraKiji.class)
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


  /**
   * Creates a Kiji table in a Cassandra instance, without checking for validation compatibility and
   * without applying permissions.
   *
   * @param tableLayout The initial layout of the table (with unassigned column ids).
   * @throws java.io.IOException on I/O error.
   * @throws org.kiji.schema.KijiAlreadyExistsException if the table already exists.
   */
  private void createTableUnchecked(TableLayoutDesc tableLayout) throws IOException {

    // For this first-cut attempt at creating a C* Kiji table, all of the code for going from a Kiji
    // TableLayoutDesc to a C* table is in this method.  Later we'll refactor this!

    final KijiURI tableURI = KijiURI.newBuilder(mURI).withTableName(tableLayout.getName()).build();

    // This will validate the layout and may throw an InvalidLayoutException.
    final KijiTableLayout layout = KijiTableLayout.newLayout(tableLayout);

    if (getMetaTable().tableExists(tableLayout.getName())) {
      throw new KijiAlreadyExistsException(
          String.format("Kiji table '%s' already exists.", tableURI), tableURI);
    }

    if (tableLayout.getKeysFormat() instanceof RowKeyFormat) {
      throw new InvalidLayoutException(
          "CassandraKiji does not support 'RowKeyFormat', instead use 'RowKeyFormat2'.");
    }

    getMetaTable().updateTableLayout(tableLayout.getName(), tableLayout);

    if (mSystemVersion.compareTo(Versions.SYSTEM_2_0) >= 0) {
      // system-2.0 clients retrieve the table layout from ZooKeeper as a stream of notifications.
      // Invariant: ZooKeeper hold the most recent layout of the table.
      LOG.debug("Writing initial table layout in ZooKeeper for table {}.", tableURI);
      ZooKeeperUtils.setTableLayout(
          mZKClient,
          tableURI,
          layout.getDesc().getLayoutId());
    }

    // Super-primitive right now.  Assume that max versions is always 1.

    // Create a C* table name for this Kiji table.
    final CassandraTableName tableName = CassandraTableName.getKijiTableName(tableURI);

    // Create the table!
    mAdmin.createTable(tableName, CQLUtils.getCreateTableStatement(tableName, layout));

    // Add a secondary index on the locality group (needed for row scans - SCHEMA-846).
    mAdmin.execute(CQLUtils.getCreateIndexStatement(tableName, CQLUtils.LOCALITY_GROUP_COL));

    // Add a secondary index on the version.
    mAdmin.execute(CQLUtils.getCreateIndexStatement(tableName, CQLUtils.VERSION_COL));

    // Also create a second table, which we can use for counters.
    // Create a C* table name for this Kiji table.
    final CassandraTableName counterTableName =
        CassandraTableName.getKijiCounterTableName(tableURI);

    String createCounterTableStatement =
        CQLUtils.getCreateCounterTableStatement(counterTableName, layout);

    // Create the table!
    mAdmin.createTable(counterTableName, createCounterTableStatement);
  }
}
