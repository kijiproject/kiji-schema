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

package org.kiji.schema.util;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiURI;

/** Factory for ZooKeeperLock instances. */
@ApiAudience.Private
public final class ZooKeeperLockFactory implements LockFactory {
  /** ZooKeeper watcher that simply discards events. */
  private static class ZooKeeperNoopWatcher implements Watcher {
    /** {@inheritDoc} */
    @Override
    public void process(WatchedEvent event) {
      // Ignore
    }
  }

  /** ZooKeeper watcher that simply discards events. */
  private static final Watcher ZOOKEEPER_NOOP_WATCHER = new ZooKeeperNoopWatcher();

  /**
   * Creates a ZooKeeper client.
   *
   * @param zkConnStr ZooKeeper quorum, as a comma separated list of ZooKeeper node "host:port".
   * @return a new ZooKeeper client.
   * @throws IOException on I/O error.
   */
  public static ZooKeeper newZooKeeper(String zkConnStr) throws IOException {
    return new ZooKeeper(zkConnStr, 60000, ZOOKEEPER_NOOP_WATCHER);
  }

  /**
   * Creates a ZooKeeper connection string from an HBase configuration.
   *
   * @param conf HBase configuration with ZooKeeper quorum and client port set.
   * @return a ZooKeeper connection string.
   */
  public static String zkConnStr(Configuration conf) {
    final int zkClientPort =
        conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT);
    final List<String> zkNodes = Lists.newArrayList();
    for (String zkNode : conf.get(HConstants.ZOOKEEPER_QUORUM).split(",")) {
      zkNodes.add(String.format("%s:%d", zkNode, zkClientPort));
    }
    return Joiner.on(",").join(zkNodes);
  }

  /**
   * Creates a ZooKeeper connection string from a Kiji URI specifying an HBase instance.
   *
   * @param uri Kiji URI specifying an HBase instance.
   * @return a ZooKeeper connection string.
   */
  public static String zkConnStr(KijiURI uri) {
    final int zkClientPort = uri.getZookeeperClientPort();
    final List<String> zkNodes = Lists.newArrayList();
    for (String zkNode : uri.getZookeeperQuorumOrdered()) {
      zkNodes.add(String.format("%s:%d", zkNode, zkClientPort));
    }
    return Joiner.on(",").join(zkNodes);
  }

  /**
   * Creates a factory for ZooKeeperLock.
   *
   * @param zkClient ZooKeeper client
   */
  public ZooKeeperLockFactory(ZooKeeper zkClient) {
    mZooKeeper = Preconditions.checkNotNull(zkClient);
  }

  /**
   * Creates a factory for ZooKeeperLock.
   *
   * @param zkConnStr ZooKeeper connection.
   * @throws IOException on I/O error.
   */
  public ZooKeeperLockFactory(String zkConnStr) throws IOException {
    this(newZooKeeper(zkConnStr));
  }

  /** ZooKeeper instance to use. */
  private final ZooKeeper mZooKeeper;

  /** {@inheritDoc} */
  @Override
  public Lock create(String name) {
    return new ZooKeeperLock(mZooKeeper, new File(name));
  }
}
