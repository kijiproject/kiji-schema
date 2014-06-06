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

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingCluster;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.delegation.Priority;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.DebugResourceTracker;

/**
 * A {@link ZooKeeperFactory} which uses an in-process ZooKeeper cluster when for 'fake'
 * {@link KijiURI}s.
 */
@ApiAudience.Private
@Inheritance.Sealed
public class TestingZooKeeperFactory implements ZooKeeperFactory {
  /** Factory to delegate to. */
  private static final ZooKeeperFactory DELEGATE = new DefaultZooKeeperFactory();

  /**
   * URIs for fake cluster instances are of the form
   * "[uri-prefix]://.fake.[fake-id]/[uri-specifics]".
   */
  private static final String FAKE_CLUSTER_ID_PREFIX = ".fake.";

  /**
   * Singleton in-process ZooKeeper cluster for testing.
   *
   * Lazily started when the first test requests the ZooKeeper ensemble for a fake Kiji instance.
   *
   * Once started, the mini-cluster remains alive until the JVM shuts down.
   */
  private static final TestingCluster ZK_CLUSTER = new TestingCluster(1);

  /** Ensures the ZKCluster is only started once. */
  private static final AtomicBoolean ZK_CLUSTER_IS_STARTED = new AtomicBoolean(false);

  /**
   * Ensure that the global testing cluster is started.
   */
  private static void startTestingCluster() {
    try {
      if (ZK_CLUSTER_IS_STARTED.compareAndSet(false, true)) {
        ZK_CLUSTER.start();
        // We create this connection once and keep it open for the life of the JVM.  This
        // significantly speeds up tests, because only 1 ZK connection is established per surefire
        // run (instead of potentially many per individual test).
        final CuratorFramework zkConnection =
            ZooKeeperUtils.getZooKeeperClient(ZK_CLUSTER.getConnectString());
        DebugResourceTracker.get().unregisterResource(zkConnection);
      }
    } catch (Exception e) {
      throw new InternalKijiError(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public String getZooKeeperEnsemble(KijiURI clusterURI) {
    final String fakeID = getFakeClusterID(clusterURI);
    if (fakeID == null) {
      // Not a fake cluster; delegate to DefaultZooKeeperFactory.
      return DELEGATE.getZooKeeperEnsemble(clusterURI);
    }

    startTestingCluster();
    return String.format("%s/%s", ZK_CLUSTER.getConnectString(), fakeID);
  }

  /** {@inheritDoc} */
  @Override
  public int getPriority(Map<String, String> runtimeHints) {
    // High priority; should be used instead of DefaultZooKeeperFactory.
    return Priority.HIGH;
  }

  /**
   * Extracts the ID of the fake cluster instance from a {@link KijiURI}.  If the URI does not
   * include a fake cluster ID, {@code null} is returned.
   *
   * @param uri URI to extract a fake ZooKeeper ID from.
   * @return the fake ZooKeeper ID, if any, or null.
   */
  public static String getFakeClusterID(KijiURI uri) {
    if (uri.getZookeeperQuorum().size() != 1) {
      return null;
    }
    final String zkHost = uri.getZookeeperQuorum().get(0);
    if (!zkHost.startsWith(FAKE_CLUSTER_ID_PREFIX)) {
      return null;
    }
    return zkHost.substring(FAKE_CLUSTER_ID_PREFIX.length());
  }
}
