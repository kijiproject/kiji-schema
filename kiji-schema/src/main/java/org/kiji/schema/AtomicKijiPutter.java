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
import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;

/**
 * Interface for performing atomic puts to a single row of a Kiji table.
 */
@ApiAudience.Framework
@Inheritance.Sealed
public interface AtomicKijiPutter extends Closeable {
  /**
   * Begins a transaction of puts which all must use the specified Entity ID.
   * @param id EntityId of the target row.
   */
  void begin(EntityId id);

  /**
   * Get the EntityId of the row to which the current transaction will be applied.
   *
   * @return The EntityId of the row to which the current transaction will be applied.
   */
  EntityId getEntityId();

  /**
   * Explicitly perfom puts. All cells must be within the same locality group.
   *
   * @param localityGroup Name of the Locality Group of all columns to which to write.
   * @throws IOException in case of an error.
   */
  void commit(String localityGroup) throws IOException;

  /**
   * Check a given KijiCell and put if it contains the expected value. Cells may be in
   * different locality groups, but atomicity is only guaranteed if all cells are in the
   * same locality group.
   *
   * @param family Column Family of the cell to check.
   * @param qualifier Column Qualifier of the cell to check.
   * @param value Data in the cell to check.
   * @param <T> Type of the data to be checked.
   * @throws IOException in case of an error.
   * @return Whether the check passed and Put executed.
   */
  <T> boolean checkAndCommit(String family, String qualifier, T value) throws IOException;

  /** Discard pending puts. */
  void rollback();

  /**
   * Stage a put to be written.
   *
   * @param family Column Family of the target cell.
   * @param qualifier Column Qualifier of the target cell.
   * @param value Data for the target cell.
   * @param <T> Type of the data to be written.
   * @throws IOException in case of an error.
   */
  <T> void put(String family, String qualifier, T value) throws IOException;

  /**
   * Stage a put to be written.
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
