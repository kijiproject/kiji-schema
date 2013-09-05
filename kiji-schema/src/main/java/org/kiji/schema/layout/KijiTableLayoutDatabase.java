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
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.avro.TableLayoutsBackup;

/**
 * <p>
 * A database of Kiji table layouts. It is strongly recommended that you access the
 * functionality of KijiTableKeyValueDatabase via the {@link org.kiji.schema.KijiMetaTable}.
 * </p>
 *
 */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
public interface KijiTableLayoutDatabase {
  /**
   * Lists the tables in this Kiji instance.
   *
   * @return The list of table names.
   * @throws IOException If the list of tables cannot be retrieved.
   */
  List<String> listTables() throws IOException;

  /**
   * Check if a table by this name exists in the kiji instance.
   *
   * @param tableName Name of the table.
   * @return True if table exists, false otherwise.
   * @throws IOException If the list of tables cannot be retrieved.
   */
  boolean tableExists(String tableName) throws IOException;

  /**
   * Sets the layout of a table.
   *
   * <p>
   *   If the table doesn't exist, this creates the initial layout of the table.
   *   Otherwise, this pushes a new layout on top of the most recent layout.
   * </p>
   *
   * @param table The name of the Kiji table to affect. Must match the table name
   * @param update Descriptor for the layout update.
   * @return the new effective layout.
   * @throws IOException If there is an error.
   */
  KijiTableLayout updateTableLayout(String table, TableLayoutDesc update) throws IOException;

  /**
   * Gets the most recent versions of the layout for a table.
   *
   * <p> Throws KijiTableNotFoundException if the table does not exist. </p>
   *
   * @param table The name of the Kiji table.
   * @return The table's layout.
   * @throws IOException If there is an error.
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
   *     keys the corresponding timestamps, ordered from least recent first to most recent last.
   * @throws IOException If there is an error.
   */
  NavigableMap<Long, KijiTableLayout> getTimedTableLayoutVersions(String table, int numVersions)
      throws IOException;

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
   * Gets the TableLayoutsBackup which can be used to restore a table.
   *
   * @param table The name of the Kiji table.
   * @return The backup information for the layouts of the table.
   * @throws IOException If there is an error.
   */
  TableLayoutsBackup layoutsToBackup(String table) throws IOException;

  /**
   * Restores a table layout history from a backup. This is equivalent to explicitly setting the
   * timestamp associated with a layout to the timestamp originally recorded for that layout.
   *
   * @param tableName The name of the table to restore layouts for.
   * @param tableBackup Table layout backup to restore from.
   * @throws IOException on I/O error.
   */
  void restoreLayoutsFromBackup(String tableName, TableLayoutsBackup tableBackup) throws
    IOException;
}
