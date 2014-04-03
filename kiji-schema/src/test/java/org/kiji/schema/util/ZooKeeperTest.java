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

import java.io.File;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
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

  /**
   * Global lock for serializing ZooKeeperTest implementations. This is necessary to correct for a
   * bug in MiniZooKeeperCluster which prevent multiple clusters from being opened concurrently.
   */
  private static final ReentrantLock mLock = new ReentrantLock();

  private MiniZooKeeperCluster mZKCluster = null;
  private File mZKBaseDir = null;

  /**
   * Starts the mini ZooKeeper cluster.
   *
   * @throws Exception on error.
   */
  public void startZKCluster() throws Exception {
    LOG.info("Starting ZooKeeper mini cluster.");
    mZKCluster.startup(mZKBaseDir, 1);
    LOG.info("ZooKeeper mini cluster started.");
  }

  /**
   * Stops the mini ZooKeeper cluster.
   *
   * @throws Exception on error.
   */
  public void stopZKCluster() throws Exception {
    LOG.info("Shutting down ZooKeeper mini cluster.");
    mZKCluster.shutdown();
    LOG.info("ZooKeeper mini cluster is down.");
  }

  @Before
  public final void setupZooKeeperTest() throws Exception {
    LOG.info("taking lock: {}", getTestId());
    mLock.lock();
    LOG.info("took lock: {}", getTestId());
    mZKBaseDir = new File(getLocalTempDir(), "mini-zookeeper-cluster");
    mZKCluster = new MiniZooKeeperCluster();
    startZKCluster();
  }

  @After
  public final void teardownZooKeeperTest() throws Exception {
    stopZKCluster();
    mZKCluster = null;
    LOG.info("releasing lock: {}", getTestId());
    mLock.unlock();
    LOG.info("lock released: {}", getTestId());
  }

  /**
   * Reports the mini ZooKeeper cluster.
   *
   * @return the mini ZooKeeper cluster.
   */
  public MiniZooKeeperCluster getZKCluster() {
    return mZKCluster;
  }

  /**
   * Reports the address of the mini ZooKeeper cluster.
   *
   * @return the address of the mini ZooKeeper cluster.
   */
  public String getZKAddress() {
    return String.format("localhost:%d", mZKCluster.getClientPort());
  }

}
