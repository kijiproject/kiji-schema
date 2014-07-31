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
 * Interface for performing asynchronous deletes on a Kiji table.
 *
 * <p>AsyncKijiDeleter provides methods for asynchronously deleting data from a Kiji table.</p>
 *
 * <p>Delete all data on the given row in the given column (column family:column qualifier).</p>
 * <pre>
 *   final AsyncKijiDeleter deleter = myKijiTable.openAsyncTableWriter();
 *   deleter.deleteColumn(entityId, columnFamily, columnQualifier);
 * </pre>
 *
 * <p>Delete the most recent data in a cell.</p>
 * <pre>
 *   final AsyncKijiDeleter deleter = myKijiTable.openAsyncTableWriter();
 *   deleter.deleteCell(entityId, columnFamily, columnQualifier);
 * </pre>
 *
 * This interface is bundled within the {@link org.kiji.schema.AsyncKijiBufferedWriter} interface.
 *
 * Delete operations in long running applications can be dangerous if the table layout may change
 * during the lifetime of the application.  The user must ensure that all writers are closed and
 * reopened to reflect new table layouts after a change.
 */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Sealed
public interface AsyncKijiDeleter extends Closeable {
  /**
   * Deletes an entire row.
   *
   * @param entityId Entity ID of the row to delete.
   * @return A {@code KijiFuture} object that indicates the completion of the request. The Object
   * has no special meaning and can be null (think of it as KijiFuture<Void>). But you probably want
   * to attach at least a {@link com.google.common.util.concurrent.FutureCallback}  in order to
   * handle failures.
   * @throws java.io.IOException on I/O error.
   */
  KijiFuture<Object> deleteRow(EntityId entityId)
      throws IOException;

  /**
   * Delete all cells from a row with a timestamp less than or equal to the specified timestamp.
   *
   * @param entityId Entity ID of the row to delete data from.
   * @param upToTimestamp Delete cells with a timestamp older or equal to this parameter.
   * @return A {@code KijiFuture} object that indicates the completion of the request. The Object
   * has no special meaning and can be null (think of it as KijiFuture<Void>). But you probably want
   * to attach at least a {@link com.google.common.util.concurrent.FutureCallback}  in order to
   * handle failures.
   * @throws java.io.IOException on I/O error.
   */
  KijiFuture<Object> deleteRow(EntityId entityId, long upToTimestamp)
      throws IOException;

  /**
   * Deletes all versions of all cells in a column. If the KijiColumnName is unqualified, then it
   * will delete all versions of all cells in the specified family.
   *
   * @param entityId The entity ID of the row to delete data from.
   * @param columnName The KijiColumnName specifying the column to delete.
   * @return A {@code KijiFuture} object that indicates the completion of the request. The Object
   * has no special meaning and can be null (think of it as KijiFuture<Void>). But you probably want
   * to attach at least a {@link com.google.common.util.concurrent.FutureCallback}  in order to
   * handle failures.
   * @throws java.io.IOException on I/O error.
   */
  KijiFuture<Object> deleteColumn(EntityId entityId, KijiColumnName columnName)
      throws IOException;

  /**
   * Deletes all cells from a column with a timestamp less than or equal to the specified timestamp.
   * If the KijiColumnName is unqualified, then it will delete all cells from the specified family
   * with a timestamp less than or equal to the specified timestamp.
   *
   * @param entityId The entity ID of the row to delete data from.
   * @param columnName The KijiColumnName specifying the column to delete.
   * @param upToTimestamp Delete cells with a timestamp older or equal to this parameter.
   * @return A {@code KijiFuture} object that indicates the completion of the request. The Object
   * has no special meaning and can be null (think of it as KijiFuture<Void>). But you probably want
   * to attach at least a {@link com.google.common.util.concurrent.FutureCallback}  in order to
   * handle failures.
   * @throws java.io.IOException on I/O error.
   */
  KijiFuture<Object> deleteColumn(EntityId entityId, KijiColumnName columnName, long upToTimestamp)
      throws IOException;

  /**
   * Deletes the most recent version of the cell in a column.
   *
   * @param entityId The entity ID of the row to delete data from.
   * @param columnName The KijiColumnName specifying the column to delete.
   * @return A {@code KijiFuture} object that indicates the completion of the request. The Object
   * has no special meaning and can be null (think of it as KijiFuture<Void>). But you probably want
   * to attach at least a {@link com.google.common.util.concurrent.FutureCallback}  in order to
   * handle failures.
   * @throws java.io.IOException on I/O error.
   */
  KijiFuture<Object> deleteCell(EntityId entityId, KijiColumnName columnName)
      throws IOException;

  /**
   * Deletes a single cell with a specified timestamp.
   *
   * @param entityId Entity ID of the row to delete data from.
   * @param columnName The KijiColumnName specifying the column to delete.
   * @param timestamp The timestamp of the cell to delete (use HConstants.LATEST_TIMESTAMP
   *     to delete the most recent cell in the column).
   * @return A {@code KijiFuture} object that indicates the completion of the request. The Object
   * has no special meaning and can be null (think of it as KijiFuture<Void>). But you probably want
   * to attach at least a {@link com.google.common.util.concurrent.FutureCallback}  in order to
   * handle failures.
   * @throws java.io.IOException on I/O error.
   */
  KijiFuture<Object> deleteCell(EntityId entityId, KijiColumnName columnName, long timestamp)
      throws IOException;
}
