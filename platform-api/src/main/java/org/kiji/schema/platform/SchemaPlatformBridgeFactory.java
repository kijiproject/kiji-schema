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

package org.kiji.schema.platform;

import org.kiji.annotations.ApiAudience;
import org.kiji.delegation.PriorityProvider;

/**
 * Factory for a specific SchemaPlatformBridge implementation. This class represents
 * the service loaded by the delegation library; we do not directly load multiple
 * SchemaPlatformBridge implementations, because they may not all typecheck against
 * a particular live runtime environment (e.g., a class/interface different).
 *
 * <p>Individual SchemaPlatformBridgeFactory implementations should use Class.forName()
 * to load a specific SchemaPlatformBridge implementation dynamically only after it
 * has been chosen by the PriorityLookup library as the best implementation fit.</p>
 *
 * <p>It is very important that SchemaPlatformBridgeFactory does not inadvertently
 * attempt to classload portions of the Hadoop or HBase runtime as this may trigger
 * typechecking by the JVM that fails. SchemaPlatformBridgeFactory implementations
 * should be able to determine whether they are compatible by checking nothing more
 * than <tt>org.apache.hadoop.util.VersionInfo</tt> and
 * <tt>org.apache.hadoop.hbase.util.VersionInfo</tt>, which should not recursively
 * load more Hadoop classes into memory.</p>
 *
 */
@ApiAudience.Framework
abstract class SchemaPlatformBridgeFactory implements PriorityProvider {

  /**
   * This API should only be implemented by other modules within KijiSchema;
   * to discourage external users from extending this class, keep the c'tor
   * package-private.
   */
  SchemaPlatformBridgeFactory() {
  }

  /**
   * @return the SchemaPlatformBridge implementation appropriate to the current runtime
   * conditions. Must never return null.
   */
  public abstract SchemaPlatformBridge getBridge();
}

