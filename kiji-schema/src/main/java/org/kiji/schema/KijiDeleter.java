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
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/**
 * Interface for performing deletes on a Kiji table.
 *
 * <p>KijiDeleter provides methods for deleting data from a Kiji table.</p>
 *
 * <p>Delete all data on the given row in the given column (column family:column qualifier).</p>
 * <pre>
 *   final KijiDeleter deleter = myKijiTable.openTableWriter();
 *   deleter.deleteColumn(entityId, columnFamily, columnQualifier);
 * </pre>
 *
 * <p>Delete the most recent data in a cell.</p>
 * <pre>
 *   final KijiDeleter deleter = myKijiTable.openTableWriter();
 *   deleter.deleteCell(entityId, columnFamily, columnQualifier);
 * </pre>
 *
 * This interface is bundled within the {@link KijiTableWriter} interface.
 *
 * Delete operations in long running applications can be dangerous if the table layout may change
 * during the lifetime of the application.  The user must ensure that all writers are closed and
 * reopened to reflect new table layouts after a change.
 */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Sealed
public interface KijiDeleter extends Closeable, Flushable {
  /**
   * Deletes an entire row.
   *
   * @param entityId Entity ID of the row to delete.
   * @throws IOException on I/O error.
   */
  void deleteRow(EntityId entityId)
      throws IOException;

  /**
   * Delete all cells from a row with a timestamp less than or equal to the specified timestamp.
   *
   * @param entityId Entity ID of the row to delete data from.
   * @param upToTimestamp Delete cells with a timestamp older or equal to this parameter.
   * @throws IOException on I/O error.
   */
  void deleteRow(EntityId entityId, long upToTimestamp)
      throws IOException;

  /**
   * Deletes all versions of all cells in a family.
   *
   * @param entityId Entity ID of the row to delete data from.
   * @param family Column family.
   * @throws IOException on I/O error.
   */
  void deleteFamily(EntityId entityId, String family)
      throws IOException;

  /**
   * Deletes all cells from a family with a timestamp less than or equal to the specified timestamp.
   *
   * @param entityId Entity ID of the row to delete data from.
   * @param family Column family.
   * @param upToTimestamp Delete cells with a timestamp older or equal to this parameter.
   * @throws IOException on I/O error.
   */
  void deleteFamily(EntityId entityId, String family, long upToTimestamp)
      throws IOException;

  /**
   * Deletes all versions of all cells in a column.
   *
   * @param entityId Entity ID of the row to delete data from.
   * @param family Column family.
   * @param qualifier Column qualifier.
   * @throws IOException on I/O error.
   */
  void deleteColumn(EntityId entityId, String family, String qualifier)
      throws IOException;

  /**
   * Deletes all cells with a timestamp less than or equal to the specified timestamp.
   *
   * @param entityId Entity ID of the row to delete data from.
   * @param family Column family.
   * @param qualifier Column qualifier.
   * @param upToTimestamp Delete cells with a timestamp older or equal to this parameter.
   * @throws IOException on I/O error.
   */
  void deleteColumn(EntityId entityId, String family, String qualifier, long upToTimestamp)
      throws IOException;

  /**
   * Deletes the most recent version of the cell in a column.
   *
   * @param entityId Entity ID of the row to delete data from.
   * @param family Column family.
   * @param qualifier Column qualifier.
   * @throws IOException on I/O error.
   */
  void deleteCell(EntityId entityId, String family, String qualifier)
      throws IOException;

  /**
   * Deletes a single cell with a specified timestamp.
   *
   * @param entityId Entity ID of the row to delete data from.
   * @param family Column family.
   * @param qualifier Column qualifier.
   * @param timestamp The timestamp of the cell to delete (use HConstants.LATEST_TIMESTAMP
   *     to delete the most recent cell in the column).
   * @throws IOException on I/O error.
   */
  void deleteCell(EntityId entityId, String family, String qualifier, long timestamp)
      throws IOException;
}
