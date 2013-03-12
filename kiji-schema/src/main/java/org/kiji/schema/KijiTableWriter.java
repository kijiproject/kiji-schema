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
import java.util.List;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;

/**
 * Interface for modifying a Kiji table.
 *
 * <p>
 *   Wraps methods from KijiPutter, KijiIncrementer, and KijiDeleter.
 *   To get a KijiTableWriter, use {@link org.kiji.schema.KijiTable#openTableWriter()}
 *   or {@link org.kiji.schema.KijiTable#getWriterFactory()}.
 * </p>
 */
@ApiAudience.Public
@Inheritance.Sealed
public interface KijiTableWriter extends KijiPutter, KijiIncrementer, KijiDeleter {

  /**
   * Check against a single cell and conditionally put into any number of cells on the same row.
   * KijiCells do not contain entity information, so the caller is responsible for ensuring all
   * Cells are intended for the same row.
   *
   * HBase: Atomicity is guaranteed for puts within a single locality group.  If puts span multiple
   * locality groups, atomicity is guaranteed within each group, but not between them.
   * When constructing KijiCells for an HBase-backed Kiji Table, use HConstants.LATEST_TIMESTAMP to
   * write cells using the current system time of the region server hosting the table.
   *
   * @param entityId EntityId of the row to check and write to.
   * @param family Column family of the cell to check.
   * @param qualifier Column qualifier of the cell to check.
   * @param <T> Type of the value to check.
   * @param value Cell data to check.
   * @param puts List of KijiCell values to write upon successful check.
   *
   * @return Whether the check succeeded and puts were written.
   * @throws IOException in case of an error.
   */
  <T> boolean checkAndPut(
      EntityId entityId, String family, String qualifier, T value, List<KijiCell<?>> puts)
      throws IOException;
}
