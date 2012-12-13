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
import java.util.Set;

import org.kiji.annotations.ApiAudience;

/**
 * A database of per table key-value pairs. This is used to store meta data (in the form of
 * key-value pairs) on a per table basis.
 *
 * @see KijiMetaTable
 */
@ApiAudience.Framework
public interface KijiTableKeyValueDatabase {

  /**
   * Associates the specified value with the specified table and key

   * The specified key and value are associated with each other in the map for the specified table.
   *
   * @param table The kiji table that this key-value pair will be set with.
   * @param key The key to associate with the specified value.
   * @param value The value to associate with the specified key.
   * @return The same KijiTableKeyValueDatabase.
   * @throws IOException If there is an error.
   */
  KijiTableKeyValueDatabase putValue(String table, String key, byte[] value) throws
    IOException;

  /**
   * Returns the most recent value associated with the specified table and key.
   *
   * @param table The kiji table .
   * @param key The key to look up the associated value for.
   * @return The value in the meta table with the given key, or null if the key doesn't exist.
   * @throws IOException If there is an error.
   */
  byte[] getValue(String table, String key) throws IOException;

  /**
   * Removes all values associated with the specified table and key.
   *
   * @param table The kiji table associated with the metadata.
   * @param key The metadata key to look up.
   * @throws IOException If there is an error.
   */
  void removeValues(String table, String key) throws IOException;

  /**
   * Deletes all key-value pairs associated with the specified table.
   *
   * @param table The table to delete remove key-value pairs for.
   * @throws IOException If there is an error.
   */
  void removeAllValues(String table) throws IOException;

  /**
   * Returns a set view of keys contained in the map for the specified table.
   *
   * @param table The table to look up in use keys for.
   * @return A navigable set of keys used to store meta information for a given table.
   * @throws IOException If there is an error.
   */
  Set<String> keySet(String table) throws IOException;

  /**
   * Returns a set view of the tables that have key-value pairs defined.
   *
   * @return A set of tables that have key-value pairs defined.
   * @throws IOException If there is an error.
   */
  Set<String> tableSet() throws IOException;
}
