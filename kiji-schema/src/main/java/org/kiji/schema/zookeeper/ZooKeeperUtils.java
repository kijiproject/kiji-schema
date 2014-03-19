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

package org.kiji.schema.zookeeper;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiURI;

/**
 * Utility class which holds constants and utitily methods for working with ZooKeeper.
 */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
public final class ZooKeeperUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperUtils.class);

  /** Root path of the ZooKeeper directory node where to write Kiji nodes. */
  private static final File ROOT_ZOOKEEPER_PATH = new File("/kiji-schema");

  /** Path of the ZooKeeper directory where instance Kiji nodes are written. */
  public static final File INSTANCES_ZOOKEEPER_PATH = new File(ROOT_ZOOKEEPER_PATH, "instances");

  /** Separator used in ZooKeeper node names. */
  public static final String ZK_NODE_NAME_SEPARATOR = "#";

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

  /**
   * Updates the table layout ID in ZooKeeper. This will notify all watchers of the table layout.
   *
   * <p>
   *   The caller must ensure proper locking of table layout update operations.
   * </p>
   *
   * @param zkClient connection to ZooKeeper.
   * @param tableURI Notify the users of the table with this URI.
   * @param layoutID Encoded layout update for the table with the specified URI.
   * @throws IOException on unrecoverable ZooKeeper error.
   */
  public static void setTableLayout(
      final CuratorFramework zkClient,
      final KijiURI tableURI,
      final String layoutID) throws IOException {
    final String path = ZooKeeperUtils.getTableLayoutFile(tableURI).getPath();
    try {
      zkClient.newNamespaceAwareEnsurePath(path).ensure(zkClient.getZookeeperClient());
      zkClient.setData().forPath(path, Bytes.toBytes(layoutID));
      LOG.debug("Updated layout ID for table {} to {}.",
          tableURI, layoutID);
    } catch (Exception e) {
      wrapAndRethrow(e);
    }
  }

  /**
   * Return a {@link org.kiji.schema.util.Lock} instance which must be aquired before updating a
   * table layout.
   *
   * @param zkClient connection to ZooKeeper.
   * @param tableURI of table to be updated.
   * @return a {@link org.kiji.schema.util.Lock} for updating the table's layout.
   */
  public static ZooKeeperLock newTableLayoutLock(
      final CuratorFramework zkClient,
      final KijiURI tableURI) {
    return new ZooKeeperLock(zkClient, getTableLayoutUpdateLock(tableURI));
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Constructs the ZooKeeper node name for the specified user ID and layout ID.
   *
   * @param userId ID of the user to construct a ZooKeeper node for.
   * @param layoutId ID of the table layout to construct a ZooKeeper node for.
   * @return the ZooKeeper node for the specified user ID and layout ID.
   */
  public static String makeZKNodeName(String userId, String layoutId) {
    try {
      return String.format("%s" + ZK_NODE_NAME_SEPARATOR + "%s",
          URLEncoder.encode(userId, Charsets.UTF_8.displayName()),
          URLEncoder.encode(layoutId, Charsets.UTF_8.displayName()));
    } catch (UnsupportedEncodingException uee) {
      throw new InternalKijiError(uee);
    }
  }


  /**
   * Takes any Exception and rethrows it if it is an IOException, or wraps it in a KijiIOException
   * and throws that.
   *
   * @param e exception to be wrapped.
   * @throws IOException if the provided exception is an IOException.
   * @throws KijiIOException for any other Exception type.
   */
  public static void wrapAndRethrow(Exception e) throws IOException {
    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    } else if (e instanceof IOException) {
      throw (IOException) e;
    } else if (e instanceof RuntimeException) {
      throw (RuntimeException) e;
    } else {
      throw new KijiIOException(e);
    }
  }

  /**
   * Create a new ZooKeeper connection to the provided ensemble.  The returned client is already
   * started, but the caller is responsible for closing it.
   *
   * @param zkEnsemble to connect to.
   * @return a ZooKeeper client using the new connection.
   */
  public static CuratorFramework getZooKeeperClient(String zkEnsemble) {
    Preconditions.checkNotNull(zkEnsemble);
    String address = zkEnsemble;
    String namespace = null;

    int index = zkEnsemble.indexOf('/');

    if (index != -1) {
      address = zkEnsemble.substring(0, index);
      namespace = zkEnsemble.substring(index + 1);
    }

    CuratorFramework zkClient =
        CuratorFrameworkFactory
            .builder()
            .connectString(address)
            .namespace(namespace)
            .retryPolicy(new ExponentialBackoffRetry(1000, 1))
            .build();
    zkClient.getConnectionStateListenable().addListener(new LoggingConnectionStateListener());
    zkClient.start();
    return zkClient;
  }

  /**
   * Create a new ZooKeeper connection to the provided instance.  The returned client is already
   * started, but the caller is responsible for closing it.
   *
   * @param instanceURI of cluster to connect to.
   * @return a ZooKeeper client using the new connection.
   */
  public static CuratorFramework getZooKeeperClient(KijiURI instanceURI) {
    Preconditions.checkNotNull(instanceURI);
    return getZooKeeperClient(instanceURI.getZooKeeperEnsemble());
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * A ConnectionStateListener which logs connection change events.
   */
  public static final class LoggingConnectionStateListener implements ConnectionStateListener {
    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
      switch (newState) {
        case CONNECTED: {
          LOG.debug("ZooKeeper session established. {}.", client);
          break;
        }
        case SUSPENDED: {
          LOG.warn("ZooKeeper session disconnected. {}.", client);
          break;
        }
        case RECONNECTED: {
          LOG.info("ZooKeeper session reestablished. {}.", client);
          break;
        }
        case LOST: {
          LOG.warn("ZooKeeper session expired. {}.", client);
          break;
        }
        case READ_ONLY: {
          // This should never happen
          LOG.error("ZooKeeper session is read only. {}.");
          break;
        }
        default: {
          // This should really never happen
          LOG.error("ZooKeeper session in unknown state {}. {}.", newState, client);
          break;
        }
      }
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Private constructor for non-instantiable utility class.
   */
  private ZooKeeperUtils() {
  }
}
