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

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/** Factory for KijiTable instances. */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
public interface KijiTableFactory {
  /**
   * Opens a KijiTable by name.
   *
   * <p>Clients must take care to release the KijiTable instance when finished.</p>
   *
   * @param tableName Name of the table to open.
   * @return the opened Kiji table.
   * @throws IOException on I/O error.
   */
  KijiTable openTable(String tableName) throws IOException;
}
