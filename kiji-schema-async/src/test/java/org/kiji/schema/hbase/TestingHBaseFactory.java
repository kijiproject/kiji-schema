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

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.utils.EnsurePath;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.delegation.Priority;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.impl.HBaseAdminFactory;
import org.kiji.schema.impl.HTableInterfaceFactory;
import org.kiji.schema.impl.hbase.DefaultHBaseFactory;
import org.kiji.schema.layout.impl.ZooKeeperClient;
import org.kiji.schema.util.LocalLockFactory;
import org.kiji.schema.util.LockFactory;
import org.kiji.schema.zookeeper.ZooKeeperUtils;
import org.kiji.testing.fakehtable.FakeHBase;

/** Factory for HBase instances based on URIs. */
public final class TestingHBaseFactory implements HBaseFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TestingHBaseFactory.class);

  /** Factory to delegate to. */
  private static final HBaseFactory DELEGATE = new DefaultHBaseFactory();

  /** Lock object to protect MINI_ZOOKEEPER_CLUSTER and MINIZK_CLIENT. */
  private static final Object MINIZK_CLUSTER_LOCK = new Object();

  /**
   * Singleton in-process ZooKeeper cluster for testing.
   *
   * Lazily instantiated when the first test requests a ZooKeeperClient for a .fake Kiji instance.
   *
   * Once started, the mini-cluster remains alive until the JVM shuts down.
   */
  private static TestingCluster mZKCluster;

  /**
   * ZooKeeperClient used to create chroot directories prior to instantiating test ZooKeeperClients.
   * Lazily instantiated when the first test requests a ZooKeeperClient for a .fake Kiji instance.
   *
   * This client will not be closed properly.
   */
  private static volatile CuratorFramework mZKClient;

  /** Map from fake HBase ID to fake HBase instances. */
  private final Map<String, FakeHBase> mFakeHBase = Maps.newHashMap();

  /** Map from fake HBase ID to fake (local) lock factories. */
  private final Map<String, LockFactory> mLock = Maps.newHashMap();

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
   * Returns the ZooKeeper mini cluster address with a chroot set to the provided fakeId.
   *
   * <p> The address will be to the testing MiniZooKeeperCluster. </p>
   *
   * @param fakeId the id of the test instance.
   * @return the ZooKeeper address for the test instance.
   * @throws IOException on I/O error when creating the mini cluster.
   */
  private static String createZooKeeperAddressForFakeId(String fakeId) throws IOException {
    // Initializes the Mini ZooKeeperCluster, if necessary:
    final TestingCluster zkCluster = getMiniZKCluster();

    // Create the chroot node for this fake ID, if necessary:
    try {
      new EnsurePath("/" + fakeId).ensure(mZKClient.getZookeeperClient());
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }

    // Test ZooKeeperClients use a chroot to isolate testing environments.
    return zkCluster.getConnectString() + "/" + fakeId;
  }

  /**
   * Creates a new MiniZooKeeperCluster if one does not already exist.  Also creates and opens a
   * ZooKeeperClient for the minicluster which is used to create chroot nodes before opening test
   * ZooKeeperClients.
   *
   * @throws IOException in case of an error creating the temporary directory or starting the mini
   *    zookeeper cluster.
   */
  private static TestingCluster getMiniZKCluster() throws IOException {
    synchronized (MINIZK_CLUSTER_LOCK) {
      if (mZKCluster == null) {
        mZKCluster = new TestingCluster(1);
        try {
          mZKCluster.start();
        } catch (Exception e) {
          ZooKeeperUtils.wrapAndRethrow(e);
        }

        final String zkAddress = mZKCluster.getConnectString();
        LOG.debug("Creating testing utility ZooKeeperClient for {}", zkAddress);
        mZKClient = ZooKeeperUtils.getZooKeeperClient(zkAddress);
      }
      return mZKCluster;
    }
  }

  /** {@inheritDoc} */
  @Override
  public ZooKeeperClient getZooKeeperClient(KijiURI uri) throws IOException {
    return ZooKeeperClient.getZooKeeperClient(getZooKeeperEnsemble(uri));
  }

  /** {@inheritDoc} */
  @Override
  public String getZooKeeperEnsemble(KijiURI uri) {
    final String fakeId = getFakeHBaseID(uri);
    if (fakeId != null) {
      try {
        return createZooKeeperAddressForFakeId(fakeId);
      } catch (IOException e) {
        throw new KijiIOException(e);
      }
    }

    // Not a test instance, delegate to default factory:
    return DELEGATE.getZooKeeperEnsemble(uri);
  }
}
