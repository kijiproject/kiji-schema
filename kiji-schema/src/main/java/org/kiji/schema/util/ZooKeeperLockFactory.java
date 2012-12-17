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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import org.kiji.annotations.ApiAudience;

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
   * @param conf Configuration.
   * @return a new ZooKeeper client.
   * @throws IOException on I/O error.
   */
  public static ZooKeeper newZooKeeper(Configuration conf) throws IOException {
    final String zkQuorum = conf.get(HConstants.ZOOKEEPER_QUORUM);
    final String zkClientPort = conf.get(HConstants.ZOOKEEPER_CLIENT_PORT);
    final String zkConnStr = String.format("%s:%s", zkQuorum, zkClientPort);
    return new ZooKeeper(zkConnStr, 60000, ZOOKEEPER_NOOP_WATCHER);
  }

  /** ZooKeeper instance to use. */
  private final ZooKeeper mZooKeeper;

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
   * @param conf ZooKeeper configuration.
   * @throws IOException on I/O error.
   */
  public ZooKeeperLockFactory(Configuration conf) throws IOException {
    this(newZooKeeper(conf));
  }

  /** {@inheritDoc} */
  @Override
  public Lock create(String name) {
    return new ZooKeeperLock(mZooKeeper, new File(name));
  }
}
