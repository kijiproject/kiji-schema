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

package org.kiji.schema;

import java.io.Closeable;
import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/**
 * Interface for performing asynchronous increments on a Kiji table.
 *
 * <p>
 *   AsyncKijiIncrementer provides a method for modifying the value of a counter given
 *   an entity id, column family, column qualifier, and a signed long amount by which
 *   to modify the counter.  Increment does not support setting the timestamp of the
 *   data to write, and returns the newly incremented cell.
 * </p>
 *
 * <pre>
 *   final AsyncKijiIncrementer incrementer = myKijiTable.openAsyncTableWriter();
 *   incrementer.increment(entityId, columnFamily, columnQualifier, amount);
 * </pre>
 *
 * This interface is not used alone but is bundled within
 * {@link org.kiji.schema.KijiBufferedWriter}.
 */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Sealed
public interface AsyncKijiIncrementer extends Closeable {
  /**
   * Atomically increments a counter in a kiji table.
   *
   * <p>Throws an exception if the specified column is not a counter.</p>
   *
   * @param entityId Entity ID of the row containing the counter.
   * @param columnName The KijiColumName specifying the column to increment.
   * @param amount Amount to increment the counter (may be negative).
   * @return A {@code KijiFuture} of the new counter value, post increment.
   * @throws java.io.IOException on I/O error.
   */
  KijiFuture<KijiCell<Long>> increment(
      EntityId entityId,
      KijiColumnName columnName,
      long amount) throws IOException;
}
