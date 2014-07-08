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

package org.kiji.schema.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.delegation.Lookups;
import org.kiji.delegation.PriorityProvider;
import org.kiji.schema.KijiURI;
import org.kiji.schema.impl.cassandra.CassandraAdminFactory;

/** Factory for Cassandra instances based on URIs. */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
public interface CassandraFactory extends PriorityProvider {

  /**
   * Provider for the default CassandraFactory.
   *
   * Ensures that there is only one CassandraFactory instance.
   */
  public static final class Provider {
    private static final Logger LOG = LoggerFactory.getLogger(Provider.class);

    /** CassandraFactory instance. */
    private static CassandraFactory mInstance;

    /** @return the default CassandraFactory. */
    public static CassandraFactory get() {
      synchronized (Provider.class) {
        if (null == mInstance) {
          mInstance = Lookups.getPriority(CassandraFactory.class).lookup();
        }
        return mInstance;
      }
    }

    /** Utility class may not be instantiated. */
    private Provider() {
    }
  }

  /**
   * Reports a factory for CassandraAdmin for a given HBase instance.
   *
   * @param uri URI of the Cassandra instance to work with.
   * @return a factory for CassandraAdmin for the specified HBase instance.
   */
  CassandraAdminFactory getCassandraAdminFactory(KijiURI uri);
}
