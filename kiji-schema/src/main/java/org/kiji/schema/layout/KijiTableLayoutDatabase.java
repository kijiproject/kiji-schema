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

package org.kiji.schema.layout;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.avro.TableLayoutBackupEntry;
import org.kiji.schema.avro.TableLayoutDesc;

/**
 * A database of Kiji table layouts.
 */
@ApiAudience.Framework
public interface KijiTableLayoutDatabase {
  /**
   * Lists the tables in this Kiji instance.
   *
   * @return The list of table names.
   * @throws IOException If the list of tables cannot be retrieved.
   */
  List<String> listTables() throws IOException;

  /**
   * Sets a table's layout. Also calls validateAndAssignLayout().
   *
   * @param table The name of the Kiji table to affect.
   * @param update Descriptor for the layout update.
   * @return the new effective layout.
   * @throws IOException If there is an error.
   */
  KijiTableLayout updateTableLayout(String table, TableLayoutDesc update) throws IOException;

  /**
   * Gets the most recent versions of the layout for a table.
   *
   * @param table The name of the Kiji table.
   * @return The table's layout.
   * @throws IOException If there is an error or no such table.
   */
  KijiTableLayout getTableLayout(String table) throws IOException;

  /**
   * Gets a list of the most recent specified number of versions of the table layout.
   *
   * @param table The name of the Kiji table.
   * @param numVersions The maximum number of the most recent versions to retrieve.
   * @return A list of the most recent versions of the layout for the table, sorted by
   *     most-recent-first.  If there are no layouts, returns an empty list.
   * @throws IOException If there is an error.
   */
  List<KijiTableLayout> getTableLayoutVersions(String table, int numVersions) throws IOException;

  /**
   * Gets a map of the most recent versions of the layout for a table, keyed by timestamp.
   *
   * @param table The name of the Kiji table.
   * @param numVersions The maximum number of the most recent versions to retrieve.
   * @return A navigable map with values the most recent versions of the layout for the table, and
   *     keys the corresponding timestamps, ordered from most recent first to least recent last.
   * @throws IOException If there is an error.
   */
  NavigableMap<Long, KijiTableLayout> getTimedTableLayoutVersions(String table, int numVersions)
      throws IOException;

  /**
   * Gets a list of the TableLayoutBackupEntries which can be used to restore a table.
   *
   * @param table The name of the Kiji table.
   * @return A list of TableLayoutBackupEntries.
   * @throws IOException If there is an error.
   */
  List<TableLayoutBackupEntry> layoutsToBackup(String table) throws IOException;

  /**
   * Removes all layout information for a particular table.
   *
   * @param table The name of the Kiji table.
   * @throws IOException If there is an error.
   */
  void removeAllTableLayoutVersions(String table) throws IOException;

  /**
   * Removes the most recent layout information for a given table.
   *
   * @param table The name of the Kiji table.
   * @param numVersions The maximum number of the most recent versions to delete.
   * @throws IOException If there is an error.
   */
  void removeRecentTableLayoutVersions(String table, int numVersions) throws IOException;

  /**
   * Restores a table layout history from a backup.
   *
   * @param tableName The name of the table to restore layouts for.
   * @param tableBackup Table layout backup entries to restore.
   * @throws IOException on I/O error.
   */
  void layoutsFromBackup(String tableName, List<TableLayoutBackupEntry> tableBackup) throws
    IOException;
}
