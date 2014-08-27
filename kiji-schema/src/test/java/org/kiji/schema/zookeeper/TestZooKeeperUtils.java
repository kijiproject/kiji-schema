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

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NotEmptyException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ZooKeeperTest;

/**
 * Unit tests for {@link ZooKeeperUtils}.
 */
public class TestZooKeeperUtils extends ZooKeeperTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestZooKeeperUtils.class);

  private volatile CuratorFramework mZKClient;

  @Before
  public void setUpTestZooKeeperUtils() throws Exception {
    mZKClient = ZooKeeperUtils.getZooKeeperClient(getZKAddress());
  }

  @After
  public void tearDownTestZooKeeperUtils() throws Exception {
    mZKClient.close();
  }


  @Test
  public void testAtomicRecursiveDelete() throws Exception {
    final String path = "/foo/bar/baz/faz/fooz";
    mZKClient.create().creatingParentsIfNeeded().forPath(path);
    ZooKeeperUtils.atomicRecursiveDelete(mZKClient, "/foo");
    Assert.assertNull(mZKClient.checkExists().forPath("/foo"));
  }

  /**
   * Tests that the atomic transaction will succeed even if there is a persistent ephemeral node
   * in the tree.  The persistent ephemeral node *will* return after the transaction commits.
   *
   * @throws Exception
   */
  @Test
  public void testAtomicRecursiveDeleteWithPersistentENode() throws Exception {
    final String path = "/foo/bar/baz/faz/fooz";

    PersistentEphemeralNode node =
        new PersistentEphemeralNode(mZKClient, Mode.EPHEMERAL, path, new byte[0]);
    try {
      node.start();
      node.waitForInitialCreate(5, TimeUnit.SECONDS);
      Assert.assertTrue(ZooKeeperUtils.atomicRecursiveDelete(mZKClient, "/foo"));
      Thread.sleep(1000); // Give ephemeral node time to recreate itself
      Assert.assertNotNull(mZKClient.checkExists().forPath("/foo"));
    } finally {
      node.close();
    }
  }

  /**
   * Test that the atomic delete commit will throw NotEmptyException if there is a concurrent
   * node addition in the tree.  The tree will not be deleted.
   */
  @Test
  public void testAtomicRecusiveDeleteWithConcurrentNodeAddition() throws Exception {
    final String path = "/foo/bar/baz/faz/fooz";
    mZKClient.create().creatingParentsIfNeeded().forPath(path);
    CuratorTransactionFinal tx =
        ZooKeeperUtils.buildAtomicRecursiveDelete(mZKClient, mZKClient.inTransaction(), "/foo");
    mZKClient.create().forPath("/foo/new-child");

    try {
      tx.commit();
    } catch (NotEmptyException nee) {
      // expected
    }
    Assert.assertNotNull(mZKClient.checkExists().forPath("/foo"));
  }

  /**
   * Test that the atomic delete commit will throw NoNodeException if there is a concurrent
   * node deletion in the tree.  The tree will not be deleted.
   */
  @Test
  public void testAtomicRecusiveDeleteWithConcurrentNodeDeletion() throws Exception {
    final String path = "/foo/bar/baz/faz/fooz";
    mZKClient.create().creatingParentsIfNeeded().forPath(path);
    CuratorTransactionFinal tx =
        ZooKeeperUtils.buildAtomicRecursiveDelete(mZKClient, mZKClient.inTransaction(), "/foo");
    mZKClient.delete().forPath(path);

    try {
      tx.commit(); // should throw
    } catch (NoNodeException nne) {
      // expected
    }
    Assert.assertNotNull(mZKClient.checkExists().forPath("/foo"));
  }

  @Test
  public void testZooKeeperConnectionsAreCached() throws Exception {
    final CuratorFramework other = ZooKeeperUtils.getZooKeeperClient(getZKAddress());
    try {
        Assert.assertTrue(mZKClient.getZookeeperClient() == other.getZookeeperClient());
    } finally {
      other.close();
    }
  }

  @Test
  public void testFakeURIsAreNamespaced() throws Exception {
    final String namespace = "testFakeURIsAreNamespaced";
    final KijiURI uri = KijiURI.newBuilder("kiji://.fake." + namespace).build();
    final CuratorFramework framework = ZooKeeperUtils.getZooKeeperClient(uri);
    try {
      Assert.assertEquals(namespace, framework.getNamespace());
    } finally {
      framework.close();
    }
  }
}
