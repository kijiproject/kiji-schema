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

package org.kiji.schema.util;

import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingZooKeeperServer;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiClientTest;

/**
 * Base class for tests that require a ZooKeeper cluster.
 */
public abstract class ZooKeeperTest extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperTest.class);

  private TestingCluster mZKCluster = null;

  /**
   * Starts the mini ZooKeeper cluster after it has been explicitly stopped via
   * {@link #stopZKCluster()}.
   *
   * @throws Exception on error.
   */
  public void startZKCluster() throws Exception {
    LOG.info("Restarting ZooKeeper mini cluster.");
    for (TestingZooKeeperServer server : mZKCluster.getServers()) {
      server.restart();
    }
    LOG.info("ZooKeeper mini cluster restarted.");
  }

  /**
   * Stops the mini ZooKeeper cluster.
   *
   * @throws Exception on error.
   */
  public void stopZKCluster() throws Exception {
    LOG.info("Shutting down ZooKeeper mini cluster.");
    for (TestingZooKeeperServer server : mZKCluster.getServers()) {
      server.stop();
    }
    LOG.info("ZooKeeper mini cluster is down.");
  }

  @Before
  public final void setupZooKeeperTest() throws Exception {
    mZKCluster = new TestingCluster(1);
    mZKCluster.start();
  }

  @After
  public final void teardownZooKeeperTest() throws Exception {
    mZKCluster.stop();
    mZKCluster = null;
  }

  /**
   * Reports the mini ZooKeeper cluster.
   *
   * @return the mini ZooKeeper cluster.
   */
  public TestingCluster getZKCluster() {
    return mZKCluster;
  }

  /**
   * Reports the address of the mini ZooKeeper cluster.
   *
   * @return the address of the mini ZooKeeper cluster.
   */
  public String getZKAddress() {
    return mZKCluster.getConnectString();
  }
}
