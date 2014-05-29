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

package org.kiji.schema.layout.impl;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.Lock;
import org.kiji.schema.util.ZooKeeperLock;

/**
 * Monitor tracking table layouts.
 *
 * <p>
 *   The monitor roles include:
 *   <ul>
 *     <li> Reporting new layout updates to active users of a table:
 *       When a table's layout is being updated, users of that table will receive a notification and
 *       will automatically reload the table layout.
 *     </li>
 *     <li> Reporting active users of a table to table management client processes:
 *       Every user of a table will advertise itself as such and report the version of the table
 *       layout it currently uses.
 *       This makes it possible for a process to ensure that table users have a consistent view
 *       on the table layout before applying further updates.
 *     </li>
 *   </ul>
 * </p>
 *
 * <h2> ZooKeeper node tree structure </h2>
 *
 * <p>
 *  The monitor manages a tree of ZooKeeper nodes organized as follows:
 *  <ul>
 *    <li> {@code /kiji-schema} : Root ZooKeeper node for all Kiji instances. </li>
 *    <li> {@code /kiji-schema/instances/[instance-name]} :
 *        Root ZooKeeper node for the Kiji instance with name "instance-name".
 *    </li>
 *    <li> {@code /kiji-schema/instances/[instance-name]/tables/[table-name]} :
 *        Root ZooKeeper node for the Kiji table with name "table-name" belonging to the Kiji
 *        instance named "instance-name".
 *    </li>
 *  </ul>
 * </p>
 *
 * <h2> ZooKeeper nodes for a Kiji table </h2>
 *
 * Every table is associated with three ZooKeeper nodes:
 * <ul>
 *   <li>
 *     {@code /kiji-schema/instances/[instance-name]/tables/[table-name]/layout} :
 *     this node contains the most recent version of the table layout.
 *     Clients should watch this node for changes to be notified of table layout updates.
 *   </li>
 *   <li>
 *     {@code /kiji-schema/instances/[instance-name]/tables/[table-name]/users} :
 *     this directory contains a node for each user of the table;
 *     each user's node contains the version of the layout as seen by the client.
 *     Management tools should watch these users' nodes to ensure that all clients have a
 *     consistent view on a table's layout before/after pushing new updates.
 *   </li>
 *   <li>
 *     {@code /kiji-schema/instances/[instance-name]/tables/[table-name]/layout_update_lock} :
 *     this directory node is used to acquire exclusive lock for table layout updates.
 *     Layout management tools are required to acquire this lock before proceeding with any
 *     table layout update.
 *   </li>
 * </ul>
 *
 * @deprecated use the ZooKeeper recipes in the {@link org.kiji.schema.zookeeper} package instead.
 *    Will be removed in KijiSchema 2.0.
 */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
@Deprecated
public final class ZooKeeperMonitor implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperMonitor.class);

  /** Root path of the ZooKeeper directory node where to write Kiji nodes. */
  private static final File ROOT_ZOOKEEPER_PATH = new File("/kiji-schema");

  /** Path of the ZooKeeper directory where instance Kiji nodes are written. */
  public static final File INSTANCES_ZOOKEEPER_PATH = new File(ROOT_ZOOKEEPER_PATH, "instances");

  /** UTF-8 encoding name. */
  private static final String UTF8 = "utf-8";

  /** Separator used in ZooKeeper node names. */
  private static final String ZK_NODE_NAME_SEPARATOR = "#";

  /** Empty byte array used to create ZooKeeper nodes. */
  private static final byte[] EMPTY_BYTES = new byte[0];

  /** States of internal objects. */
  private static enum State {
    UNINITIALIZED,
    INITIALIZED,
    OPEN,
    CLOSED
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Reports the ZooKeeper node path for a Kiji instance.
   *
   * @param kijiURI URI of a Kiji instance to report the ZooKeeper node path for.
   * @return the ZooKeeper node path for a Kiji instance.
   */
  public static File getInstanceDir(KijiURI kijiURI) {
    return new File(INSTANCES_ZOOKEEPER_PATH, kijiURI.getInstance());
  }

  /**
   * Reports the path of the ZooKeeper node for permissions changes locking.
   *
   * @param instanceURI URI of the instance for which to get a lock for permissions changes.
   * @return the path of the ZooKeeper node used as a lock for permissions changes.
   */
  public static File getInstancePermissionsLock(KijiURI instanceURI) {
    return new File(getInstanceDir(instanceURI), "permissions_lock");
  }

  /**
   * Reports the ZooKeeper root path containing all tables in a Kiji instance.
   *
   * @param kijiURI URI of a Kiji instance to report the ZooKeeper node path for.
   * @return the ZooKeeper node path that contains all the tables in the specified Kiji instance.
   */
  public static File getInstanceTablesDir(KijiURI kijiURI) {
    return new File(getInstanceDir(kijiURI), "tables");
  }

  /**
   * Reports the ZooKeeper root path containing all users of a Kiji instance.
   *
   * @param kijiURI URI of a Kiji instance to report the ZooKeeper node path for.
   * @return the ZooKeeper node path that contains all users of the specified Kiji instance.
   */
  public static File getInstanceUsersDir(KijiURI kijiURI) {
    return new File(getInstanceDir(kijiURI), "users");
  }

  /**
   * Reports the ZooKeeper node path for a Kiji table.
   *
   * @param tableURI URI of a Kiji table to report the ZooKeeper node path for.
   * @return the ZooKeeper node path for a Kiji table.
   */
  public static File getTableDir(KijiURI tableURI) {
    return new File(getInstanceTablesDir(tableURI), tableURI.getTable());
  }

  /**
   * Reports the path of the ZooKeeper node containing the most recent version of a table's layout.
   *
   * @param tableURI Reports the path of the ZooKeeper node that contains the most recent layout
   *     version of the Kiji table identified by this URI.
   * @return the path of the ZooKeeper node that contains the most recent layout version of the
   *     specified Kiji table.
   */
  public static File getTableLayoutFile(KijiURI tableURI) {
    return new File(getTableDir(tableURI), "layout");
  }

  /**
   * Reports the path of the ZooKeeper node where users of a table register themselves.
   *
   * @param tableURI Reports the path of the ZooKeeper node where users of the Kiji table with this
   *     URI register themselves.
   * @return the path of the ZooKeeper node where users of a table register.
   */
  public static File getTableUsersDir(KijiURI tableURI) {
    return new File(getTableDir(tableURI), "users");
  }

  /**
   * Reports the path of the ZooKeeper node for table layout update locking.
   *
   * @param tableURI Reports the path of the ZooKeeper node used to create locks for table layout
   *     updates.
   * @return the path of the ZooKeeper node used to create locks for table layout updates.
   */
  public static File getTableLayoutUpdateLock(KijiURI tableURI) {
    return new File(getTableDir(tableURI), "layout_update_lock");
  }

  // -----------------------------------------------------------------------------------------------

  /** Underlying ZooKeeper client. */
  private final ZooKeeperClient mZKClient;

  // -----------------------------------------------------------------------------------------------

  /**
   * Initializes a new table layout monitor.
   *
   * @param zkClient ZooKeeper client.
   * @throws KeeperException on unrecoverable ZooKeeper error.
   */
  public ZooKeeperMonitor(ZooKeeperClient zkClient) throws KeeperException {
    this.mZKClient = zkClient;
    this.mZKClient.createNodeRecursively(ROOT_ZOOKEEPER_PATH);
    // ZooKeeperClient.retain() should be the last line of the constructor.
    this.mZKClient.retain();
  }

  /**
   * Closes the monitor.
   * @throws IOException in case of an error closing the underlying ZooKeeper connection.
   */
  public void close() throws IOException {
    this.mZKClient.release();
  }

  /**
   * Creates a tracker for a table layout.
   *
   * <p> The tracker must be opened and closed. </p>
   *
   * @param tableURI Tracks the layout of the table with this URI.
   * @param handler Handler invoked to process table layout updates.
   * @return a new layout tracker for the specified table.
   */
  public LayoutTracker newTableLayoutTracker(KijiURI tableURI, LayoutUpdateHandler handler) {
    return new LayoutTracker(tableURI, handler);
  }

  /**
   * Creates a registration for a table user.
   *
   * <p> The registration must be started and closed. </p>
   *
   * @param userID of user to be registered.
   * @param tableURI of table the user is using.
   * @return a new table user registration for the user and table.
   */
  public TableUserRegistration newTableUserRegistration(String userID, KijiURI tableURI) {
    return new TableUserRegistration(userID, tableURI);
  }

  /**
   * Creates a registration for an instance user.
   *
   * <p> The registration must be started and closed. </p>
   *
   * @param userID of user to be registered.
   * @param systemVersion of user to be registered.
   * @param instanceURI of instance the user is using.
   * @return a new table user registration for the user and table.
   */
  public InstanceUserRegistration newInstanceUserRegistration(
      String userID,
      String systemVersion,
      KijiURI instanceURI) {
    return new InstanceUserRegistration(userID, systemVersion, instanceURI);
  }

  /**
   * Constructs the ZooKeeper node name for the specified user ID and layout ID.
   *
   * @param userId ID of the user to construct a ZooKeeper node for.
   * @param layoutId ID of the table layout to construct a ZooKeeper node for.
   * @return the ZooKeeper node for the specified user ID and layout ID.
   */
  private static String makeZKNodeName(String userId, String layoutId) {
    try {
      return String.format("%s" + ZK_NODE_NAME_SEPARATOR + "%s",
          URLEncoder.encode(userId, UTF8),
          URLEncoder.encode(layoutId, UTF8));
    } catch (UnsupportedEncodingException uee) {
      throw new InternalKijiError(uee);
    }
  }

  /**
   * Creates a lock for layout updates on the specified table.
   *
   * @param tableURI URI of the table to create a lock for.
   * @return a Lock for the table with the specified URI.
   *     The lock is not acquired at this point: the user must calli {@code Lock.lock()} and then
   *     release the lock with {@code Lock.unlock()}.
   */
  public Lock newTableLayoutUpdateLock(KijiURI tableURI) {
    return new ZooKeeperLock(this.mZKClient, getTableLayoutUpdateLock(tableURI));
  }

  /**
   * Creates a tracker for the users of a table.
   *
   * <p> The tracker must be opened and closed. </p>
   *
   * @param tableURI Tracks the users of the table with this URI.
   * @param handler Handler invoked to process updates to the users list of the specified table.
   * @return a new tracker for the users of the specified table.
   */
  public UsersTracker newTableUsersTracker(KijiURI tableURI, UsersUpdateHandler handler) {
    return new UsersTracker(tableURI, handler);
  }

  /**
   * Notifies the users of a table of a new layout.
   *
   * <p>
   *   The caller must ensure proper locking of table layout update operations through
   *   {@link #newTableLayoutUpdateLock(KijiURI)}.
   * </p>
   *
   * @param tableURI Notify the users of the table with this URI.
   * @param layout Encoded layout update for the table with the specified URI.
   * @param version of the current table layout.
   * @throws KeeperException on unrecoverable ZooKeeper error.
   */
  public void notifyNewTableLayout(KijiURI tableURI, byte[] layout, int version)
      throws KeeperException {
    final File layoutPath = getTableLayoutFile(tableURI);
    this.mZKClient.createNodeRecursively(layoutPath);
    // This should not be needed if we add a lock for layout updates.
    final Stat updateStat =
        this.mZKClient.setData(layoutPath, layout, version);
    LOG.debug("Updated layout for table {}. Layout version is {}.",
        tableURI, updateStat.getVersion());
  }

  // -----------------------------------------------------------------------------------------------

  /** Interface for trackers of a table's layout. */
  public interface LayoutUpdateHandler {
    /**
     * Processes an update to the table layout.
     *
     * <p> If this method raises an unchecked exception, the tracking stops. </p>
     *
     * @param layout Layout update, as an encoded byte[].
     *     This is the update content of the layout ZooKeeper node.
     */
    // TODO(SCHEMA-412): Notifications for ZooKeeper disconnections.
    void update(byte[] layout);
  }

  /**
   * Tracks the layout of a table and reports updates to registered handlers.
   *
   * <p> The handler is always invoked in a separate thread. </p>
   */
  public final class LayoutTracker implements Closeable {
    private volatile LayoutUpdateHandler mHandler;
    private final KijiURI mTableURI;
    private final File mTableLayoutFile;
    private final LayoutWatcher mWatcher = new LayoutWatcher();
    private final Stat mLayoutStat = new Stat();
    private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);
    private volatile byte[] mLatestLayout = null;

    /** Automatically re-registers for new layout updates. */
    private class LayoutWatcher implements Watcher {
      /** {@inheritDoc} */
      @Override
      public void process(WatchedEvent event) {
        final State state = mState.get();
        if (state == State.OPEN) {
          registerWatcher();
        } else {
          LOG.debug("LayoutTracker is in state {} : dropping layout update.", state);
          // Do not re-register a watcher.
        }
      }
    }

    /**
     * Initializes a new layout tracker with the given update handler on the specified table.
     *
     * @param tableURI Tracks the table with this URI.
     * @param handler Handler to process table layout updates.
     */
    private LayoutTracker(KijiURI tableURI, LayoutUpdateHandler handler) {
      this.mTableURI = tableURI;
      this.mTableLayoutFile = getTableLayoutFile(tableURI);
      this.mHandler = handler;
      final State oldState = mState.getAndSet(State.INITIALIZED);
      Preconditions.checkState(oldState == State.UNINITIALIZED,
          "Cannot create LayoutTracker instance in state %s.", oldState);
    }

    /** Starts the tracker. */
    public void open() {
      final State oldState = mState.getAndSet(State.OPEN);
      Preconditions.checkState(oldState == State.INITIALIZED,
          "Cannot start LayoutTracker instance in state %s.", oldState);

      // Always runs registerWatcher() in a separate thread:
      final Thread thread = new Thread() {
        /** {@inheritDoc} */
        @Override
        public void run() {
          registerWatcher();
        }
      };
      thread.start();
    }

    /**
     * Registers a ZooKeeper watcher for the specified table's layout.
     *
     * <p> Retries on ZooKeeper failure (no deadline, no limit). </p>
     * <p> Dies whenever an exception pops up while running a handler. </p>
     */
    private void registerWatcher() {
      try {
        final byte[] layoutUpdate =
            mZKClient.getData(mTableLayoutFile, mWatcher, mLayoutStat);

        if (!Arrays.equals(mLatestLayout, layoutUpdate)) {
          // Layout update may not be changed in the case where this was triggered by a ZooKeeper
          // connection state change.
          LOG.debug("Received layout update for table {}: {}.",
              mTableURI, Bytes.toStringBinary(layoutUpdate));
          mLatestLayout = layoutUpdate;

          // This assumes handlers do not let exceptions pop up:
          mHandler.update(layoutUpdate);
        }
      } catch (KeeperException.NoNodeException nne) {
        LOG.info("Tracked table layout node for table {} has been removed. Tracking will cease.",
            mTableURI);
      } catch (KeeperException ke) {
        LOG.error("Unrecoverable ZooKeeper error: {}", ke.getMessage());
      }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      LOG.debug("Closing LayoutTracker {}.", this);
      final State oldState = mState.getAndSet(State.CLOSED);
      Preconditions.checkState(oldState == State.OPEN,
          "Cannot close LayoutTracker instance in state %s.", oldState);
      // ZOOKEEPER-442: There is currently no way to cancel a watch.
      // The only way to neutralize this is by setting the state to CLOSED so that
      // when the layout updater does fire, we can simply ignore it.

      // Null out the mHandler so that ZK is not a GC Root for the mHandler
      // (via the inner class LayoutWatcher) whose one implementation is an inner class
      // in HBaseKijiTable.
      mHandler = null;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return Objects.toStringHelper(LayoutTracker.class)
          .add("table", mTableURI)
          .add("state", mState.get())
          .toString();
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Interface for trackers of a table's users.
   */
  public interface UsersUpdateHandler {

    /**
     * Processes an update to the list of users of the Kiji table being tracked.
     *
     * <p> If this method raises an unchecked exception, the tracking stops. </p>
     *
     * @param users Updated mapping from user ID to layout IDs of the Kiji table being tracked.
     */
    // TODO(SCHEMA-412): Notifications for ZooKeeper disconnections.
    void update(Multimap<String, String> users);
  }

  /**
   * Tracks users of a table.
   *
   * <p> Monitors the users of a table and reports updates to the registered handlers. </p>
   * <p> The handler is always invoked in a separate thread. </p>
   */
  public final class UsersTracker implements Closeable {
    private final UsersUpdateHandler mHandler;
    private final KijiURI mTableURI;
    private final File mUsersDir;
    private final UsersWatcher mWatcher = new UsersWatcher();
    private final Stat mStat = new Stat();
    private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

    /** Automatically re-registers for users updates. */
    private class UsersWatcher implements Watcher {
      /** {@inheritDoc} */
      @Override
      public void process(WatchedEvent event) {
        final State state = mState.get();
        if (state == State.OPEN) {
          registerWatcher();
        } else {
          LOG.debug("LayoutTracker is in state {} : dropping layout update.", state);
          // Do not re-register a watcher.
        }
      }
    }

    /**
     * Initializes a new users tracker with the given update handler on the specified table.
     *
     * @param tableURI Tracks the users of the table with this URI.
     * @param handler Handler to process updates of the table users list.
     */
    private UsersTracker(KijiURI tableURI, UsersUpdateHandler handler) {
      this.mTableURI = tableURI;
      this.mUsersDir = getTableUsersDir(tableURI);
      this.mHandler = handler;
      final State oldState = mState.getAndSet(State.INITIALIZED);
      Preconditions.checkState(oldState == State.UNINITIALIZED,
          "Cannot open UsersTracker instance in state %s.", oldState);
    }

    /**
     * Starts the tracker.
     */
    public void open() {
      final State oldState = mState.getAndSet(State.OPEN);
      Preconditions.checkState(oldState == State.INITIALIZED,
          "Cannot open UsersTracker instance in state %s.", oldState);
      final Thread thread = new Thread() {
        /** {@inheritDoc} */
        @Override
        public void run() {
          try {
            ZooKeeperMonitor.this.mZKClient.createNodeRecursively(mUsersDir);
          } catch (KeeperException ke) {
            LOG.error("Unrecoverable ZooKeeper error: {}", ke.getMessage());
            throw new RuntimeException(ke);
          }
          registerWatcher();
        }
      };
      thread.start();
    }

    /**
     * Registers a ZooKeeper watcher for the specified table's users.
     *
     * <p> Retries on ZooKeeper failure (no deadline, no limit). </p>
     * <p> Dies whenever an exception pops up while running a handler. </p>
     */
    private void registerWatcher() {
      try {
        // Lists the children nodes of the users ZooKeeper node path for this table,
        // and registers a watcher for updates on the children list:
        final List<String> zkNodeNames =
            ZooKeeperMonitor.this.mZKClient.getChildren(mUsersDir, mWatcher, mStat);
        LOG.debug("Received users update for table {}: {}.", mTableURI, zkNodeNames);

        final ImmutableSetMultimap.Builder<String, String> children =
            ImmutableSetMultimap.builder();
        try {
          for (String nodeName : zkNodeNames) {
            final String[] split = nodeName.split(ZK_NODE_NAME_SEPARATOR);
            if (split.length != 2) {
              LOG.error("Ignorning invalid ZooKeeper node name: {}", nodeName);
              continue;
            }
            final String userId = URLDecoder.decode(split[0], UTF8);
            final String layoutId = URLDecoder.decode(split[1], UTF8);
            children.put(userId, layoutId);
          }
        } catch (UnsupportedEncodingException uee) {
          throw new InternalKijiError(uee);
        }

        LOG.debug("Notifying watchers of changed layout users {}.", children.build());
        // This assumes handlers do not let exceptions pop up:
        mHandler.update(children.build());

      } catch (KeeperException ke) {
        LOG.error("Unrecoverable ZooKeeper error: {}", ke.getMessage());
        throw new RuntimeException(ke);
      }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      LOG.debug("Closing UsersTracker {}.", this);
      final State oldState = mState.getAndSet(State.CLOSED);
      Preconditions.checkState(oldState == State.OPEN,
          "Cannot close UsersTracker instance in state %s.", oldState);
      // ZOOKEEPER-442: There is currently no way to cancel a watch.
      //     All we can do here is to neutralize the handler by setting mClosed.
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return Objects.toStringHelper(UsersTracker.class)
          .add("tableURI", mTableURI)
          .add("state", mState.get())
          .toString();
    }
  }

  /**
   * Registers a table user with ZooKeeper.
   */
  public final class TableUserRegistration implements Closeable {
    private final String mUserID;
    private final KijiURI mTableURI;
    private final Object mMonitor = new Object();
    /** protected by mMonitor. */
    private File mCurrentNode;

    /**
     * Create a new table user registration in ZooKeeper.  The registration will not take effect
     * until {@link #register(String)} is called with the initial layout id.
     *
     * @param userID of table user.
     * @param tableURI of table.
     */
    public TableUserRegistration(String userID, KijiURI tableURI) {
      mUserID = userID;
      mTableURI = tableURI;
    }

    /**
     * Unregisters this table user from ZooKeeper.
     *
     * @throws IOException on unrecoverable ZooKeeper error.
     */
    private void unregister() throws IOException {
      synchronized (mMonitor) {
        if (mCurrentNode != null) {
          try {
            ZooKeeperMonitor.this.mZKClient.delete(mCurrentNode, -1);
            LOG.debug("TableUserRegistration node {} removed.", mCurrentNode);
            mCurrentNode = null;
          } catch (KeeperException e) {
            throw new IOException(e);
          }
        }
      }
    }

    /**
     * Register this table user with ZooKeeper.
     *
     * @param layoutID layout id known by table user.
     * @throws IOException on unrecoverable ZooKeeper error.
     */
    private void register(String layoutID) throws IOException {
      synchronized (mMonitor) {
        try {
          final File usersDir = getTableUsersDir(mTableURI);
          LOG.debug("Registering user '{}' for Kiji table '{}' with layout ID '{}'.",
              mUserID, mTableURI, layoutID);

          ZooKeeperMonitor.this.mZKClient.createNodeRecursively(usersDir);
          final String nodeName = makeZKNodeName(mUserID, layoutID);
          final File nodePath = new File(usersDir, nodeName);
          final byte[] data = EMPTY_BYTES;
          mCurrentNode = ZooKeeperMonitor.this.mZKClient.create(
              nodePath, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException e) {
          throw new IOException(e);
        }
      }
    }

    /**
     * Update this TableUserRegistration with the supplied layout ID.
     *
     * @param layoutID id of layout being used by registered table user.
     * @throws IOException on unrecoverable ZooKeeper error.
     */
    public void updateRegisteredLayout(String layoutID) throws IOException {
      synchronized (mMonitor) {
        unregister();
        register(layoutID);
      }
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
      LOG.debug("Closing TableUserRegistration {}.", this);
      unregister();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return Objects.toStringHelper(TableUserRegistration.class)
          .add("userID", mUserID)
          .add("table", mTableURI)
          .toString();
    }
  }

  /**
   * Registers an instance user with ZooKeeper.
   */
  public final class InstanceUserRegistration implements Closeable {
    private final String mUserID;
    private final String mSystemVersion;
    private final KijiURI mInstanceURI;
    private final Object mMonitor = new Object();
    /** protected by mMonitor. */
    private File mCurrentNode;

    /**
     * Create a new instance user registration.
     *
     * @param userID of instance user.
     * @param systemVersion of instance user.
     * @param instanceURI Kiji uri of instance being used.
     */
    public InstanceUserRegistration(String userID, String systemVersion, KijiURI instanceURI) {
      mUserID = userID;
      mSystemVersion = systemVersion;
      mInstanceURI = instanceURI;
    }

    /**
     * Start this instance user registration.
     *
     * @throws IOException on unrecoverable ZooKeeper error.
     */
    public void start() throws IOException {
      synchronized (mMonitor) {
        try {
          final File usersDir = getInstanceUsersDir(mInstanceURI);
          LOG.debug("Registering user '{}' for Kiji instance '{}' with system version '{}'.",
              mUserID, mInstanceURI, mSystemVersion);

          ZooKeeperMonitor.this.mZKClient.createNodeRecursively(usersDir);
          final String nodeName = makeZKNodeName(mUserID, mSystemVersion);
          final File nodePath = new File(usersDir, nodeName);
          mCurrentNode = ZooKeeperMonitor.this.mZKClient.create(
              nodePath, EMPTY_BYTES, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException e) {
          throw new IOException(e);
        }
      }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      synchronized (mMonitor) {
        if (mCurrentNode != null) {
          try {
            if (ZooKeeperMonitor.this.mZKClient.exists(mCurrentNode) != null) {
              ZooKeeperMonitor.this.mZKClient.delete(mCurrentNode, -1);
            }
          } catch (KeeperException e) {
            throw new IOException(e);
          }
        }
      }
    }
  }
}
