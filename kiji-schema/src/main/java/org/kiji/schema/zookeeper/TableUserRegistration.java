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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiURI;

/**
 * Registers a table user with ZooKeeper.
 */
public final class TableUserRegistration implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(TableUserRegistration.class);

  private final String mUserID;

  private final CuratorFramework mZKClient;

  private final Object mMonitor = new Object();

  /** Protected by mMonitor. */
  private PersistentEphemeralNode mNode;

  private final KijiURI mTableURI;

  /**
   * Create a table registration for the table with the provided KijiURI. The actual
   * registration will take place once #start() is called.
   *
   * @param zkClient used for connecting to ZooKeeper.
   * @param tableURI of table being registered.
   * @param userID of user to register.
   */
  public TableUserRegistration(CuratorFramework zkClient, KijiURI tableURI, String userID) {
    mUserID = userID;
    mTableURI = tableURI;
    mZKClient = zkClient;
    String path = ZooKeeperUtils.getTableUsersDir(mTableURI).getPath();
    LOG.debug("Creating table user registration for table {} in path {}.", mTableURI, path);
  }

  /**
   * Start the table user registration with the provided registration information. This will
   * block the thread until the table user is successfully registered.
   *
   * @param layoutID to register.
   * @return this.
   * @throws IOException if unable to register the table user.
   */
  public TableUserRegistration start(String layoutID) throws IOException {
    synchronized (mMonitor) {
      Preconditions.checkState(mNode == null);
      createNode(layoutID);
    }
    return this;
  }

  /**
   * Update this table user registration with new layout ID.
   *
   * @param layoutID to update user registration with.
   * @throws IOException if unable to update the table user information.
   */
  public void updateLayoutID(String layoutID) throws IOException {
    synchronized (mMonitor) {
      mNode.close();
      createNode(layoutID);
    }
  }

  /**
   * Create a new registration node in ZooKeeper.
   *
   * @param layoutID of registered user.
   * @throws IOException on unrecoverable ZooKeeper error.
   */
  private void createNode(String layoutID) throws IOException {
    synchronized (mMonitor) {
      String path =
          new StringBuilder()
              .append(ZooKeeperUtils.getTableUsersDir(mTableURI))
              .append("/")
              .append(ZooKeeperUtils.makeZKNodeName(mUserID, layoutID))
              .toString();

      mNode = new PersistentEphemeralNode(mZKClient, Mode.EPHEMERAL, path, new byte[]{});
      mNode.start();
      try {
        mNode.waitForInitialCreate(42, TimeUnit.DAYS); // Wait forever
      } catch (InterruptedException e) {
        LOG.info("Interrupted while attempting to register table user.");
        Thread.currentThread().interrupt();
      }
    }
  }

  /** {@inheritDoc}. */
  @Override
  public void close() throws IOException {
    // It would be nice to log user info here, but not worth the trip to ZK
    LOG.debug("Closing table user registration on table {}.", mTableURI);
    synchronized (mMonitor) {
      mNode.close();
    }
  }
}
