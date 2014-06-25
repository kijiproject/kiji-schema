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

package org.kiji.schema.util;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

/**
 * A Thread meant to be registered as a Shutdown Hook.  Closes the HBase and ZooKeeper
 * connections when run.
 */
@ApiAudience.Framework
@ApiStability.Experimental
public class CloseClusterConnectionsShutdownHook extends Thread {
  private static final Logger LOG =
      LoggerFactory.getLogger(CloseClusterConnectionsShutdownHook.class);

  /**
   * Constructs a new CloseClusterConnectionsShutdownHook.
   */
  public CloseClusterConnectionsShutdownHook() {
    // No-op constructor.
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void run() {
    LOG.info("Attempting to shut down cluster connections in shutdown hook.");
    HConnectionManager.deleteAllConnections();
    try {
      ZooKeeperUtils.closeAllZooKeeperConnections();
    } catch (IOException e) {
      LOG.warn("Encountered exception while deleting ZooKeeper connections:\n"
          + StringUtils.stringifyException(e));
    }
  }
}
