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
import java.io.PrintStream;
import java.util.List;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Administration API for managing a Kiji instance.
 */
@ApiAudience.Public
@Inheritance.Sealed
public interface KijiAdmin {
  /**
   * Creates a Kiji table in an HBase instance.
   *
   * @param tableName The name of the table to create.
   * @param tableLayout The initial layout of the table (with unassigned column ids).
   * @param isRestore true if this is part of a metadata restore operation.
   *     This should be set to false by all callers except KijiMetadataTool.
   * @throws IOException on I/O error.
   * @throws KijiAlreadyExistsException if the table already exists.
   */
  void createTable(String tableName, KijiTableLayout tableLayout, boolean isRestore)
      throws IOException;

  /**
   * Creates a Kiji table in an HBase instance.
   *
   * @param tableName The name of the table to create.
   * @param tableLayout The initial layout of the table (with unassigned column ids).
   * @param isRestore true if this is part of a metadata restore operation.
   * @param numRegions The initial number of regions to create.
   *
   * @throws IllegalArgumentException If numRegions is non-positive, or if numRegions is
   *     more than 1 and row key hashing on the table layout is disabled.
   * @throws IOException on I/O error.
   * @throws KijiAlreadyExistsException if the table already exists.
   */
  void createTable(String tableName, KijiTableLayout tableLayout, boolean isRestore,
      int numRegions) throws IOException;

  /**
   * Creates a Kiji table in an HBase instance.
   *
   * @param tableName The name of the table to create.
   * @param tableLayout The initial layout of the table (with unassigned column ids).
   * @param isRestore true if this is part of a metadata restore operation.
   * @param splitKeys The initial key boundaries between regions.  There will be splitKeys
   *     + 1 regions created.  Pass null to specify the default single region.
   * @throws IOException on I/O error.
   * @throws KijiAlreadyExistsException if the table already exists.
   */
  void createTable(String tableName, KijiTableLayout tableLayout, boolean isRestore,
      byte[][] splitKeys) throws IOException;

  /**
   * Sets the layout of a table.
   *
   * @param tableName The name of the Kiji table to affect.
   * @param update Layout update.
   * @return the updated layout.
   * @throws IOException If there is an error.
   */
  KijiTableLayout setTableLayout(String tableName, TableLayoutDesc update) throws IOException;

  /**
   * Sets the layout of a table, or print the results of setting the table layout if
   * dryRun is true.
   *
   * @param tableName The name of the Kiji table to affect.
   * @param update Layout update.
   * @param dryRun true if this is a 'dry run', false if changes should be made.
   * @param printStream the print stream to use for information if dryRun is true.
   *     If null, stdout will be used.
   * @return the updated layout.
   * @throws IOException If there is an error.
   */
  KijiTableLayout setTableLayout(String tableName, TableLayoutDesc update, boolean dryRun,
      PrintStream printStream) throws IOException;

  /**
   * Deletes a Kiji table.  Removes it from HBase.
   *
   * @param tableName The name of the Kiji table to delete.
   * @throws IOException If there is an error.
   */
  void deleteTable(String tableName) throws IOException;

  /**
   * Gets the list of Kiji table names.
   *
   * @return A list of the names of Kiji tables installed in the Kiji instance.
   * @throws IOException If there is an error.
   */
  List<String> getTableNames() throws IOException;
}
