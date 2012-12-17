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
 * Interface for performing increments on a Kiji table.
 *
 * Instantiated via {@code KijiTable.openTableWriter()}
 */
@ApiAudience.Framework
public interface KijiIncrementer extends Closeable, Flushable {
  /**
   * Atomically increments a counter in a kiji table.
   *
   * <p>Throws an exception if the specified column is not a counter.</p>
   *
   * @param entityId Entity ID of the row containing the counter.
   * @param family Column family.
   * @param qualifier Column qualifier.
   * @param amount Amount to increment the counter (may be negative).
   * @return the new counter value, post increment.
   * @throws IOException on I/O error.
   */
  KijiCounter increment(EntityId entityId, String family, String qualifier, long amount)
      throws IOException;
}
