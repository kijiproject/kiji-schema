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

package org.kiji.schema;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.kiji.annotations.ApiAudience;
import org.kiji.delegation.PriorityProvider;

/** Factory for Kiji instances. */
@ApiAudience.Public
public interface KijiFactory extends PriorityProvider {
  /**
   * Opens a Kiji instance by URI.
   *
   * @param uri URI specifying the Kiji instance to open.
   * @return the specified Kiji instance.
   * @throws IOException on I/O error.
   */
  Kiji open(KijiURI uri) throws IOException;

  /**
   * Opens a Kiji instance by URI.
   *
   * @param uri URI specifying the Kiji instance to open.
   * @param conf Hadoop configuration.
   * @return the specified Kiji instance.
   * @throws IOException on I/O error.
   */
  Kiji open(KijiURI uri, Configuration conf) throws IOException;

  /**
   * Opens a Kiji instance.
   *
   * @param kijiConf The configuration.
   * @return An opened kiji instance.
   * @throws IOException If there is an error.
   */
  Kiji open(KijiConfiguration kijiConf) throws IOException;
}
