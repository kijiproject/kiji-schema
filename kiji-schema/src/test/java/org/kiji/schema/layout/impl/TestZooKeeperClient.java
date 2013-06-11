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

import java.io.File;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.util.Time;
import org.kiji.schema.util.ZooKeeperTest;

/** Tests for ZooKeeperClient. */
public class TestZooKeeperClient extends ZooKeeperTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestZooKeeperClient.class);

  /** Minimal unit-test for ZooKeeperClient. */
  @Test
  public void testZooKeeperClient() throws Exception {
    final String zkAddr = String.format("localhost:%d", getZKCluster().getClientPort());

    final ZooKeeperClient client = new ZooKeeperClient(zkAddr, 10 * 1000);
    client.open();
    try {
      client.getZKClient(1.0);
      LOG.debug("Got a live ZooKeeper client.");

      // Kill mini ZooKeeper cluster and restart it in 0.5 seconds.
      // In the meantime, attempt to create a directory node.
      stopZKCluster();

      final Thread thread = new Thread() {
        /** {@inheritDoc} */
        @Override
        public void run() {
          Time.sleep(0.5);
          try {
            startZKCluster();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };
      thread.start();

      // This operation should block until ZooKeeper comes back online, then proceed:
      client.createNodeRecursively(new File("/a/b/c/d/e/f"));

      Assert.assertEquals(0, client.exists(new File("/a/b/c/d/e/f")).getVersion());

    } finally {
      client.close();
    }
  }
}
