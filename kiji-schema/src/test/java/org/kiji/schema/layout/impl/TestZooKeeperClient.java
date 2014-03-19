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

import com.google.common.base.Preconditions;
import org.apache.curator.test.KillSession;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.util.ZooKeeperTest;

/** Tests for ZooKeeperClient. */
public class TestZooKeeperClient extends ZooKeeperTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestZooKeeperClient.class);

  /** Minimal unit-test for ZooKeeperClient. */
  @Test
  public void testZooKeeperClient() throws Exception {
    final ZooKeeperClient client = ZooKeeperClient.getZooKeeperClient(getZKAddress());

    try {
      Preconditions.checkNotNull(client.getZKClient(1.0));

      // Kill the ZooKeeper session
      KillSession.kill(client.getZKClient(1.0), getZKAddress());

      // This operation should block until a new ZooKeeper session is established, then proceed:
      client.createNodeRecursively(new File("/a/b/c/d/e/f"));

      Assert.assertEquals(0, client.exists(new File("/a/b/c/d/e/f")).getVersion());

    } finally {
      client.release();
    }
  }

  @Test
  public void testZooKeeperClientsAreCached() throws Exception {
    final ZooKeeperClient client1 = ZooKeeperClient.getZooKeeperClient(getZKAddress());
    final ZooKeeperClient client2 = ZooKeeperClient.getZooKeeperClient(getZKAddress());

    Assert.assertTrue(client1 == client2);
    client1.release();
    client2.release();
  }

  @Test
  public void testZooKeeperClientGetZKClientBlocksWhileNotConnected() throws Exception {
    final ZooKeeperClient client = ZooKeeperClient.getZooKeeperClient(getZKAddress());
    stopZKCluster(); // Kill session
    Assert.assertNull(client.getZKClient(0.5));
    startZKCluster();
    Assert.assertNotNull(client.getZKClient(5.0)); // Don't try forever
  }
}
