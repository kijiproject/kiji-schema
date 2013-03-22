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
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.avro.MetaTableBackup;
import org.kiji.schema.layout.KijiTableLayoutDatabase;

/**
 * <p>
 * The Kiji meta data table, which stores layouts and other user defined meta data on a per-table
 * basis.
 *
 * Instantiated in KijiSchema via {@link org.kiji.schema.Kiji#getMetaTable()}
 * </p>
 * @see KijiSchemaTable
 * @see KijiSystemTable
 */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
public interface KijiMetaTable extends Closeable, KijiTableLayoutDatabase,
    KijiTableKeyValueDatabase<KijiMetaTable> {

  /**
   * Remove all metadata, including layouts, for a particular table.
   *
   * @param table The name of the kiji table to delete.
   * @throws IOException If there is an error.
   */
  void deleteTable(String table) throws IOException;

  /** {@inheritDoc} */
  @Override
  void close() throws IOException;

  /**
   * Returns metadata backup information in a form that can be directly written to a MetadataBackup
   * record. To read more about the avro type that has been specified to store this info, see
   * Layout.avdl
   *
   * @throws IOException If there is an error.
   * @return A map from table names to TableBackup records.
   */
  MetaTableBackup toBackup() throws IOException;

  /**
   * Restores metadata from a backup record. This consists of table layouts, schemas, and user
   * defined key-value pairs.
   *
   * @param backup A map from table name to table backup record.
   * @throws IOException on I/O error.
   */
  void fromBackup(MetaTableBackup backup) throws IOException;
}
