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
import org.kiji.delegation.Lookup;

/**
 * Abstract representation of an underlying platform for KijiSchema. This interface
 * is fulfilled by specific implementation providers that are dynamically chosen
 * at runtime based on the Hadoop &amp; HBase jars available on the classpath.
 */
@ApiAudience.Framework
public abstract class SchemaPlatformBridge {

  /**
   * This API should only be implemented by other modules within KijiSchema;
   * to discourage external users from extending this class, keep the c'tor
   * package-private.
   */
  SchemaPlatformBridge() {
  }

  // NOTE: This is currently a placeholder. We have not yet identified specific differences
  // in KijiSchema that require this abstraction.

  /**
   * @return the SchemaPlatformBridge implementation appropriate to the current runtime
   * conditions.
   */
  public SchemaPlatformBridge get() {
    return Lookup.getPriority(SchemaPlatformBridgeFactory.class).lookup().getBridge();
  }
}

