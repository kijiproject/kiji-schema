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

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

import org.kiji.annotations.ApiAudience;

/**
 * Interface for performing puts on a Kiji table.
 *
 * Instantiated via {@code KijiTable.openTableWriter()}
 */
@ApiAudience.Framework
public interface KijiPutter extends Closeable, Flushable {
  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param value The data to write.
   * @param <T> The type of the value being written.
   * @throws IOException If there is an IO error.
   */
  <T> void put(EntityId entityId, String family, String qualifier, T value)
      throws IOException;

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp Timestamp, in millisecond since the Epoch.
   * @param value The data to write.
   * @param <T> The type of the value being written.
   * @throws IOException If there is an IO error.
   */
  <T> void put(EntityId entityId, String family, String qualifier, long timestamp, T value)
      throws IOException;
}
