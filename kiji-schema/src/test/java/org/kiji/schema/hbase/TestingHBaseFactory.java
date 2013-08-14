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

import org.kiji.delegation.Priority;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.RuntimeInterruptedException;
import org.kiji.schema.impl.DefaultHBaseAdminFactory;
import org.kiji.schema.impl.DefaultHBaseFactory;
import org.kiji.schema.impl.DefaultHTableInterfaceFactory;
import org.kiji.schema.impl.HBaseAdminFactory;
import org.kiji.schema.impl.HTableInterfaceFactory;
import org.kiji.schema.layout.impl.ZooKeeperClient;
import org.kiji.schema.util.LocalLockFactory;
import org.kiji.schema.util.LockFactory;
import org.kiji.schema.util.ZooKeeperLockFactory;
import org.kiji.testing.fakehtable.FakeHBase;

/** Factory for HBase instances based on URIs. */
public final class TestingHBaseFactory implements HBaseFactory {

  /** Map from fake HBase ID to fake HBase instances. */
  private final Map<String, FakeHBase> mFakeHBase = Maps.newHashMap();

  /** Map from fake HBase ID to fake (local) lock factories. */
  private final Map<String, LockFactory> mLock = Maps.newHashMap();

  /** Map from fake HBase ID to ZooKeeperClient. */
  private final Map<String, ZooKeeperClient> mZKClients = Maps.newHashMap();

  /**
   * Singleton MiniZooKeeperCluster for testing. Lazily instantiated when the first test requests a
   * ZooKeeperClient for a .fake Kiji instance.
   */
  private static MiniZooKeeperCluster mMiniZKCluster;
  private static final Object CLUSTER_LOCK = new Object();

  /**
   * ZooKeeperClient used to create chroot directories prior to instantiating test ZooKeeperClients.
   * Lazily instantiated when the first test requests a ZooKeeperClient for a .fake Kiji instance.
   */
  private ZooKeeperClient mUtilClient;

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
    return DefaultHTableInterfaceFactory.get();
  }

  /** {@inheritDoc} */
  @Override
  public HBaseAdminFactory getHBaseAdminFactory(KijiURI uri) {
    final FakeHBase fake = getFakeHBase(uri);
    if (fake != null) {
      return fake.getAdminFactory();
    }
    return DefaultHBaseAdminFactory.get();
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
    return new ZooKeeperLockFactory(ZooKeeperLockFactory.zkConnStr(uri));
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
   * Creates a new ZooKeeperClient with a chroot set to the fakeId for a KijiClientTest.  The client
   * will connect to the testing MiniZooKeeperCluster.
   *
   * @param fakeId the id of the test instance.
   * @return a new ZooKeeperClient for the test instance.
   */
  private ZooKeeperClient getZooKeeperClientForFakeId(String fakeId) {
    // Test ZooKeeperClients use a chroot to isolate testing environments.
    final ZooKeeperClient newZKClient = new ZooKeeperClient(
        "localhost:" + mMiniZKCluster.getClientPort() + "/" + fakeId, 60 * 1000);
    newZKClient.open();
    return newZKClient;
  }

  /**
   * Creates a new ZooKeeperClient from a KijiURI.  This is used when a non-fake
   * URI is passed to {@link #getZooKeeperClient(org.kiji.schema.KijiURI)}, for instance during
   * an integration test.  In this case we connect to a regular ZooKeeper which should already be
   * running instead of the mini zookeeper cluster for unit tests.
   *
   * @param uri the KijiURI of the instance for which to create a ZooKeeperClient.
   * @return a new ZooKeeperClient for the instance addressed by the KijiURI.
   */
  public ZooKeeperClient getDefaultZooKeeperClient(final KijiURI uri) throws IOException {
    return new DefaultHBaseFactory().getZooKeeperClient(uri);
  }

  /**
   * Creates a new MiniZooKeeperCluster if one does not already exist.  Also creates and opens a
   * ZooKeeperClient for the minicluster which is used to create chroot nodes before opening test
   * ZooKeeperClients.
   *
   * @throws IOException in case of an error creating the temporary directory or starting the mini
   *    zookeeper cluster.
   */
  private void createMiniZKCluster() throws IOException {
    synchronized (CLUSTER_LOCK) {
      if (mMiniZKCluster == null) {
        final MiniZooKeeperCluster miniZK = new MiniZooKeeperCluster(new Configuration());
        final File tempDir = File.createTempFile("temp", "dir");
        Preconditions.checkState(tempDir.delete());
        Preconditions.checkState(tempDir.mkdirs());
        try {
          miniZK.startup(tempDir);
        } catch (InterruptedException ie) {
          throw new RuntimeInterruptedException(ie);
        }
        mMiniZKCluster = miniZK;
        mUtilClient =
            new ZooKeeperClient("localhost:" + mMiniZKCluster.getClientPort(), 60 * 1000);
        mUtilClient.open();
      }
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
      if (mMiniZKCluster == null) {
        // Lazily create the mini zookeeper cluster the first time we need it.
        createMiniZKCluster();
      }
      synchronized (mZKClients) {
        if (mZKClients.containsKey(fakeId)) {
          // Check that the old client has not been closed before returning it.
          final ZooKeeperClient zkClient = mZKClients.get(fakeId);
          if (zkClient.isOpen()) {
            // If the previous zookeeper client is still open, retain and return it.
            return zkClient.retain();
          } else {
            // If the previous zookeeper client has been closed, make a new one, save it, and return
            final ZooKeeperClient newZKClient = getZooKeeperClientForFakeId(fakeId);
            mZKClients.put(fakeId, newZKClient);
            return newZKClient;
          }
        } else {
          // If we have not yet created a ZooKeeperClient for this fakeId we must first prepare the
          // chroot directory which will isolate this ZooKeeperClient from others then create, save,
          // and return the new client.
          try {
            // If this is the first time we are creating a ZooKeeperClient for this fakeId, we need
            // to create the chroot node before opening the ZooKeeperClient.
            mUtilClient.createNodeRecursively(new File("/" + fakeId));
          } catch (KeeperException ke) {
            throw new KijiIOException(ke);
          }
          final ZooKeeperClient newZKClient = getZooKeeperClientForFakeId(fakeId);
          mZKClients.put(fakeId, newZKClient);
          return newZKClient;
        }
      }
    } else {
      // If the URI does not address a .fake Kiji instance, return a regular ZooKeeperClient.
      return getDefaultZooKeeperClient(uri);
    }
  }
}
