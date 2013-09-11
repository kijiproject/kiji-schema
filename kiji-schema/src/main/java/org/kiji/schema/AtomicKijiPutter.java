/**
 * (c) Copyright 2013 WibiData, Inc.
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
 * Interface for performing atomic puts to a single row of a Kiji table.
 *
 * <p> Usage example:
 * <pre>
 *   KijiTable table = ...
 *   AtomicKijiPutter putter = table.getWriterFactory().openAtomicWriter();
 *   try {
 *     // Begin an atomic write transaction on the specified row:
 *     putter.begin(entityId);
 *
 *     // Accumulate a set of puts to write atomically:
 *     putter.put(family, qualifier, "value");
 *     // More puts...
 *
 *     // Write all puts atomically:
 *     putter.commit();
 *   } finally {
 *     putter.close();
 *   }
 * </pre>
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
public interface AtomicKijiPutter extends Closeable {
  /**
   * Begins a transaction of puts which all must use the specified Entity ID.
   *
   * @param id EntityId of the target row.
   */
  void begin(EntityId id);

  /**
   * Gets the EntityId of the row to which the current transaction will be applied.
   *
   * @return The EntityId of the row to which the current transaction will be applied.
   */
  EntityId getEntityId();

  /**
   * Atomically performs the puts accumulated in the current transaction.
   *
   * @throws IOException in case of an error.
   */
  void commit() throws IOException;

  /**
   * Atomically tests the content of a cell for a specific value,
   * and performs the accumulated puts if the test succeeds.
   *
   * <p>
   *   <li> If the passed value is null, the check is for the lack of a cell (i.e. non-existance).
   *   <li> If the check fails, the current transaction remains open and unmodified.
   *        It is up to the caller to retry or rollback.
   *   <li> If the commit succeeds, the current transaction is completed and a new transaction
   *        may start with begin().
   *
   * @param family Column Family of the cell to check.
   * @param qualifier Column Qualifier of the cell to check.
   * @param value Data in the cell to check.
   * @param <T> Type of the data to be checked.
   * @return Whether the check passed and Put executed.
   * @throws IOException in case of an error.
   */
  <T> boolean checkAndCommit(String family, String qualifier, T value) throws IOException;

  /**
   * Discards the current transaction.
   *
   * <p> The current transaction is aborted, the accumulated puts are lost.
   *     No calls to <code>put()</code>, <code>commit()</code> or <code>checkAndCommit()</code>
   *     are allowed until a new transaction may start with <code>begin()</code>.
   */
  void rollback();

  /**
   * Stages a put to be written.
   *
   * <p> Requires a current transaction started using <code>begin()</code>.
   *
   * @param family Column Family of the target cell.
   * @param qualifier Column Qualifier of the target cell.
   * @param value Data for the target cell.
   * @param <T> Type of the data to be written.
   * @throws IOException in case of an error.
   */
  <T> void put(String family, String qualifier, T value) throws IOException;

  /**
   * Stages a put to be written.
   *
   * <p> Requires a current transaction started using <code>begin()</code>.
   *
   * @param family Column Family of the target cell.
   * @param qualifier Column Qualifier of the target cell.
   * @param timestamp Timestamp of the data to write.
   * @param value Data for the target cell.
   * @param <T> Type of the data to be written.
   * @throws IOException in case of an error.
   */
  <T> void put(String family, String qualifier, long timestamp, T value) throws IOException;
}
