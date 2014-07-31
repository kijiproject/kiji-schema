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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.delegation.Lookups;
import org.kiji.delegation.PriorityProvider;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;
import org.kiji.schema.impl.HBaseAdminFactory;
import org.kiji.schema.impl.HTableInterfaceFactory;
import org.kiji.schema.layout.impl.ZooKeeperClient;
import org.kiji.schema.util.LockFactory;

/** Factory for HBase instances based on URIs. */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
public interface HBaseFactory extends PriorityProvider {

  /**
   * Provider for the default HBaseFactory.
   *
   * Ensures that there is only one HBaseFactory instance.
   */
  public static final class Provider {
    /** HBaseFactory instance. */
    private static final HBaseFactory INSTANCE = Lookups.getPriority(HBaseFactory.class).lookup();

    /** @return the default HBaseFactory. */
    public static HBaseFactory get() {
      return INSTANCE;
    }

    /** Utility class may not be instantiated. */
    private Provider() {
    }
  }

  /**
   * Reports a factory for HTableInterface for a given HBase instance.
   *
   * @param uri URI of the HBase instance to work with.
   * @return a factory for HTableInterface for the specified HBase instance.
   */
  HTableInterfaceFactory getHTableInterfaceFactory(KijiURI uri);

  /**
   * Reports a factory for HBaseAdmin for a given HBase instance.
   *
   * @param uri URI of the HBase instance to work with.
   * @return a factory for HBaseAdmin for the specified HBase instance.
   */
  HBaseAdminFactory getHBaseAdminFactory(KijiURI uri);

  /**
   * Gets an HConnection for the specified configuration. Caller is responsible
   * for closing this.
   *
   * @param kiji The Kiji to get a connection for.
   * @return a HConnection for the Kiji. Caller is responsible for closing this.
   */
  HConnection getHConnection(Kiji kiji);

  /**
   * Creates a lock factory for a given Kiji instance.
   *
   * @param uri URI of the Kiji instance to create a lock factory for.
   * @param conf Hadoop configuration.
   * @return a factory for locks for the specified Kiji instance.
   * @throws IOException on I/O error.
   * @deprecated {@link LockFactory} has been deprecated.
   *    Use  {@link org.kiji.schema.zookeeper.ZooKeeperLock} directly.
   *    Will be removed in the future.
   */
  @Deprecated
  LockFactory getLockFactory(KijiURI uri, Configuration conf) throws IOException;

  /**
   * Creates and opens a ZooKeeperClient for a given Kiji instance.
   *
   * <p>
   *   Caller must release the ZooKeeperClient object with {@link ZooKeeperClient#release()}
   *   when done with it.
   * </p>.
   *
   * @param uri URI of the Kiji instance for which to create a ZooKeeperClient.
   * @return a new open ZooKeeperClient.
   * @throws IOException in case of an error connecting to ZooKeeper.
   * @deprecated {@link ZooKeeperClient} has been deprecated.
   *    Use {@link org.kiji.schema.zookeeper.ZooKeeperUtils#getZooKeeperClient(String)} instead with
   *    the ZooKeeper ensemble from {@link KijiURI#getZooKeeperEnsemble()}.
   *    Will be removed in the future.
   */
  @Deprecated
  ZooKeeperClient getZooKeeperClient(KijiURI uri) throws IOException;

  /**
   * Returns the ZooKeeper quorum address of the provided KijiURI in comma-separated host:port
   * (standard ZooKeeper) format. This method is considered experimental and should not be called by
   * clients of Kiji Schema; instead use {@link KijiURI#getZooKeeperEnsemble()}.
   *
   * @param uri of the KijiCluster for which to return the ZooKeeper quorum address.
   * @return the ZooKeeper quorum address of the Kiji cluster.
   * @deprecated use {@link KijiURI#getZooKeeperEnsemble()} instead.
   *    Will be removed in the future.
   */
  @Deprecated
  String getZooKeeperEnsemble(KijiURI uri);
}
