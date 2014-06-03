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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiURI;

/**
 * Registers an instance user with ZooKeeper.
 */
public final class InstanceUserRegistration implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(InstanceUserRegistration.class);

  private final PersistentEphemeralNode mNode;

  private final KijiURI mInstanceURI;

  /**
   * Create an instance user registration for the instance with the provided KijiURI. The actual
   * registration will take place once #start() is called.
   *
   * @param zkClient used for connecting to ZooKeeper.
   * @param instanceURI of instance being registered.
   * @param userID of user to register.
   * @param systemVersion of user to register.
   */
  public InstanceUserRegistration(
      CuratorFramework zkClient,
      KijiURI instanceURI,
      String userID,
      String systemVersion) {
    mInstanceURI = instanceURI;
    String path = ZooKeeperUtils.getInstanceUsersDir(mInstanceURI).getPath();
    String node = ZooKeeperUtils.makeZKNodeName(userID, systemVersion);

    mNode = new PersistentEphemeralNode(zkClient, Mode.EPHEMERAL, path + "/" + node, new byte[] {});

    LOG.debug("Creating instance user registration for instance {}.", mInstanceURI);
  }

  /**
   * Start the instance user registration. This will block the thread until the instance user is
   * successfully registered.
   *
   * @return this.
   */
  public InstanceUserRegistration start() {
    mNode.start();
    try {
      mNode.waitForInitialCreate(42, TimeUnit.DAYS); // Wait forever
    } catch (InterruptedException e) {
      LOG.info("Interrupted while attempting to register instance user.");
      Thread.currentThread().interrupt();
    }
    return this;
  }

  /** {@inheritDoc}. */
  @Override
  public void close() throws IOException {
    // It would be nice to log user info here, but not worth the trip to ZK
    LOG.debug("Closing instance user registration for instance {}.", mInstanceURI);
    mNode.close();
  }
}
