/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.schema.hbase;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.zookeeper.KeeperException;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.delegation.Priority;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.RuntimeInterruptedException;
import org.kiji.schema.impl.DefaultHBaseFactory;
import org.kiji.schema.impl.HBaseAdminFactory;
import org.kiji.schema.impl.HTableInterfaceFactory;
import org.kiji.schema.layout.impl.ZooKeeperClient;
import org.kiji.schema.util.LocalLockFactory;
import org.kiji.schema.util.LockFactory;
import org.kiji.testing.fakehtable.FakeHBase;

/** Factory for HBase instances based on URIs. */
public final class TestingHBaseFactory implements HBaseFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TestingHBaseFactory.class);

  /** Factory to delegate to. */
  private static final HBaseFactory DELEGATE = new DefaultHBaseFactory();

  /** Lock object to protect MINI_ZOOKEEPER_CLUSTER and MINIZK_CLIENT. */
  private static final Object MINIZK_CLUSTER_LOCK = new Object();

  /**
   * Singleton MiniZooKeeperCluster for testing.
   *
   * Lazily instantiated when the first test requests a ZooKeeperClient for a .fake Kiji instance.
   *
   * Once started, the mini-cluster remains alive until the JVM shuts down.
   */
  private static MiniZooKeeperCluster mMiniZkCluster;

  /**
   * ZooKeeperClient used to create chroot directories prior to instantiating test ZooKeeperClients.
   * Lazily instantiated when the first test requests a ZooKeeperClient for a .fake Kiji instance.
   *
   * This client will not be closed properly until the JVM shuts down.
   */
  private static ZooKeeperClient mMiniZkClient;

  /** Map from fake HBase ID to fake HBase instances. */
  private final Map<String, FakeHBase> mFakeHBase = Maps.newHashMap();

  /** Map from fake HBase ID to fake (local) lock factories. */
  private final Map<String, LockFactory> mLock = Maps.newHashMap();

  /**
   * Map from fake HBase ID to ZooKeeperClient.
   *
   * <p>
   *   ZooKeeperClient instances in this map are never released:
   *   unless client code releases ZooKeeperClient instance more than once,
   *   the retain counter is always &ge; 1.
   * </p>
   */
  private final Map<String, ZooKeeperClient> mZKClients = Maps.newHashMap();

  /**
   * Public constructor. This should not be directly invoked by users; you should
   * use HBaseFactory.get(), which retains a singleton instance.
   */
  public TestingHBaseFactory() {
  }

  /** URIs for fake HBase instances are "kiji://.fake.[fake-id]/instance/table". */
  private static final String FAKE_HBASE_ID_PREFIX = ".fake.";

  /**
   * Extracts the ID of the fake HBase from a Kiji URI.
   *
   * @param uri URI to extract a fake HBase ID from.
   * @return the fake HBase ID, if any, or null.
   */
  private static String getFakeHBaseID(KijiURI uri) {
    if (uri.getZookeeperQuorum().size() != 1) {
      return null;
    }
    final String zkHost = uri.getZookeeperQuorum().get(0);
    if (!zkHost.startsWith(FAKE_HBASE_ID_PREFIX)) {
      return null;
    }
    return zkHost.substring(FAKE_HBASE_ID_PREFIX.length());
  }

  /**
   * Gets the FakeHBase for a given HBase URI.
   *
   * @param uri URI of a fake HBase instance.
   * @return the FakeHBase for the specified URI, or null if the URI does not specify a fake HBase.
   */
  public FakeHBase getFakeHBase(KijiURI uri) {
    final String fakeID = getFakeHBaseID(uri);
    if (fakeID == null) {
      return null;
    }
    synchronized (mFakeHBase) {
      final FakeHBase hbase = mFakeHBase.get(fakeID);
      if (hbase != null) {
        return hbase;
      }
      final FakeHBase newFake = new FakeHBase();
      mFakeHBase.put(fakeID, newFake);
      return newFake;
    }
  }

  /** {@inheritDoc} */
  @Override
  public HTableInterfaceFactory getHTableInterfaceFactory(KijiURI uri) {
    final FakeHBase fake = getFakeHBase(uri);
    if (fake != null) {
      return fake.getHTableFactory();
    }
    return DELEGATE.getHTableInterfaceFactory(uri);
  }

  /** {@inheritDoc} */
  @Override
  public HBaseAdminFactory getHBaseAdminFactory(KijiURI uri) {
    final FakeHBase fake = getFakeHBase(uri);
    if (fake != null) {
      return fake.getAdminFactory();
    }
    return DELEGATE.getHBaseAdminFactory(uri);
  }

  /** {@inheritDoc} */
  @Override
  public LockFactory getLockFactory(KijiURI uri, Configuration conf) throws IOException {
    final String fakeID = getFakeHBaseID(uri);
    if (fakeID != null) {
      synchronized (mLock) {
        final LockFactory factory = mLock.get(fakeID);
        if (factory != null) {
          return factory;
        }
        final LockFactory newFactory = new LocalLockFactory();
        mLock.put(fakeID, newFactory);
        return newFactory;
      }
    }
    return DELEGATE.getLockFactory(uri, conf);
  }

  /** Resets the testing HBase factory. */
  public void reset() {
    mFakeHBase.clear();
    mLock.clear();
  }

  /** {@inheritDoc} */
  @Override
  public int getPriority(Map<String, String> runtimeHints) {
    // Higher priority than default factory.
    return Priority.HIGH;
  }

  /**
   * Creates a new ZooKeeperClient with a chroot set to the fakeId for a KijiClientTest.
   *
   * <p> The client will connect to the testing MiniZooKeeperCluster. </p>
   *
   * @param fakeId the id of the test instance.
   * @return a new ZooKeeperClient for the test instance.
   * @throws IOException on I/O error.
   */
  private static ZooKeeperClient createZooKeeperClientForFakeId(String fakeId)
      throws IOException {

    // Initializes the Mini ZooKeeperCluster, if necessary:
    final MiniZooKeeperCluster zkCluster = getMiniZKCluster();

    // Create the chroot node for this fake ID, if necessary:
    try {
      final File zkChroot = new File("/" + fakeId);
      if (mMiniZkClient.exists(zkChroot) == null) {
        mMiniZkClient.createNodeRecursively(new File("/" + fakeId));
      }
    } catch (KeeperException ke) {
      throw new KijiIOException(ke);
    }

    // Test ZooKeeperClients use a chroot to isolate testing environments.
    final String zkAddress = "localhost:" + zkCluster.getClientPort() + "/" + fakeId;

    Log.info("Creating test ZooKeeperClient for address {}", zkAddress);
    final ZooKeeperClient zkClient = ZooKeeperClient.getZooKeeperClient(zkAddress);
    return zkClient;
  }

  /**
   * Creates a new MiniZooKeeperCluster if one does not already exist.  Also creates and opens a
   * ZooKeeperClient for the minicluster which is used to create chroot nodes before opening test
   * ZooKeeperClients.
   *
   * @throws IOException in case of an error creating the temporary directory or starting the mini
   *    zookeeper cluster.
   */
  private static MiniZooKeeperCluster getMiniZKCluster() throws IOException {
    synchronized (MINIZK_CLUSTER_LOCK) {
      if (mMiniZkCluster == null) {
        final MiniZooKeeperCluster miniZK = new MiniZooKeeperCluster(new Configuration());
        final File tempDir = File.createTempFile("mini-zk-cluster", "dir");
        Preconditions.checkState(tempDir.delete());
        Preconditions.checkState(tempDir.mkdirs());
        try {
          miniZK.startup(tempDir);
        } catch (InterruptedException ie) {
          throw new RuntimeInterruptedException(ie);
        }
        mMiniZkCluster = miniZK;

        final String zkAddress ="localhost:" + mMiniZkCluster.getClientPort();
        LOG.info("Creating testing utility ZooKeeperClient for {}", zkAddress);
        mMiniZkClient = ZooKeeperClient.getZooKeeperClient(zkAddress);
      }
      return mMiniZkCluster;
    }
  }

  /**
   * Returns the ZooKeeperClient to use for the instance with the specified fake ID.
   *
   * @param fakeId ID of the fake testing instance to get a ZooKeeperClient for.
   * @return the ZooKeeperClient for the instance with the specified fake ID.
   * @throws IOException on I/O error.
   */
  private ZooKeeperClient getTestZooKeeperClient(String fakeId) throws IOException {
    synchronized (mZKClients) {
      ZooKeeperClient zkClient = mZKClients.get(fakeId);
      if (null == zkClient) {
        zkClient = createZooKeeperClientForFakeId(fakeId);
        mZKClients.put(fakeId, zkClient);
      }
      return zkClient.retain();
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>
   *   TestingHBaseFactory manages a pool of connections to a singleton MiniZooKeeperCluster for
   *   unit testing purposes.  These connections are lazily created and reused where possible.
   * </p>
   */
  @Override
  public ZooKeeperClient getZooKeeperClient(KijiURI uri) throws IOException {
    final String fakeId = getFakeHBaseID(uri);
    if (fakeId != null) {
      return getTestZooKeeperClient(fakeId);
    }

    // Not a test instance, delegate to default factory:
    return DELEGATE.getZooKeeperClient(uri);
  }
}
