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

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.delegation.Lookups;
import org.kiji.delegation.PriorityProvider;
import org.kiji.schema.KijiURI;

/**
 * An interface for classes which can translate a {@link KijiURI} into a valid ZooKeeper ensemble
 * addresses. This layer of indirection is necessary so that test code may seamlessly use in-process
 * ZooKeeper clusters.
 */
@ApiAudience.Private
@ApiStability.Experimental
public interface ZooKeeperFactory extends PriorityProvider {

  /**
   * Provides a {@link ZooKeeperFactory}. There should only be a single {@code ZooKeeperFactory}
   * active per JVM, so {@link #get()} will always return the same object.
   */
  public static final class Provider {
    private static final ZooKeeperFactory INSTANCE =
        Lookups.getPriority(ZooKeeperFactory.class).lookup();

    /**
     * @return the {@link ZooKeeperFactory} with the highest priority.
     */
    public static ZooKeeperFactory get() {
      return INSTANCE;
    }

    /** Utility class may not be instantiated. */
    private Provider() {
    }
  }

  /**
   * Returns the ZooKeeper quorum address of the provided KijiURI in comma-separated host:port
   * (standard ZooKeeper) format. In almost all cases, {@link KijiURI#getZooKeeperEnsemble()} should
   * be preferred to this method.
   *
   * @param clusterURI of the KijiCluster for which to return the ZooKeeper quorum address.
   * @return the ZooKeeper quorum address of the Kiji cluster.
   */
  String getZooKeeperEnsemble(KijiURI clusterURI);
}
