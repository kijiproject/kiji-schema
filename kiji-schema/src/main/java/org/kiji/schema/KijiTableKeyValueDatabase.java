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
import java.util.NavigableMap;
import java.util.Set;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.avro.KeyValueBackup;

/**
 * <p>
 * A database of per table key-value pairs. This is used to store meta data (in the form of
 * key-value pairs) on a per table basis. It is strongly recommended that you access the
 * functionality of KijiTableKeyValueDatabase via the {@link org.kiji.schema.KijiMetaTable}.
 * </p>
 *
 * <p>
 *   Non-framework users wishing to set key-value pairs on a per-table basis should use the
 *   {@link KijiTableAnnotator}.
 * </p>
 *
 * @param <T> the type of the class implementing KijiTableKeyValueDatabase, to be used
 *     in putValue()'s return type.
 */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
public interface KijiTableKeyValueDatabase<T extends KijiTableKeyValueDatabase<T>> {

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
  T putValue(String table, String key, byte[] value) throws
    IOException;

  /**
   * Returns the most recent value associated with the specified table and key.
   *
   * @param table The kiji table.
   * @param key The key to look up the associated value for.
   * @return The value in the meta table with the given key.
   * @throws IOException If there is an error or if the key doesn't exist.
   */
  byte[] getValue(String table, String key) throws IOException;

  /**
   * Gets a list of the most recent specified number of versions of the value corresponding to the
   * specified table and key.
   *
   * @param table The Kiji table.
   * @param key The key to look up associated values for.
   * @param numVersions The maximum number of the most recent versions to retrieve.
   * @return A list of the most recent versions of the values for the specified table and key,
   *     sorted by most-recent-first.
   * @throws IOException If there is an error or if the key doesn't exist.
   */
  List<byte[]> getValues(String table, String key, int numVersions) throws IOException;

   /**
   * Returns a map of values associated with the specified table and key. The Map
   * is keyed by timestamp.
   *
   * @param table The kiji table.
   * @param key The key to look up the associated value for.
   * @param numVersions The maximum number of the most recent versions to retrieve.
   * @return A navigable map with values that are the user defined values for the specified key
   *     and table, and keys that correspond to timestamps, ordered from newest to oldest.
   * @throws IOException If there is an error.
   */
  NavigableMap<Long, byte[]> getTimedValues(String table, String key, int numVersions)
      throws IOException;

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

   /**
   * Gets the backup information for the key-value metadata of the specified table.
   *
   * @param table The name of the Kiji table.
   * @return The keyvalue backup information for the table.
   * @throws IOException If there is an error.
   */
  KeyValueBackup keyValuesToBackup(String table) throws IOException;

   /**
   * Restores all table's key-value history from a backup. This is equivalent to explicitly setting
   * the timestamp associated with a key-value pair to the timestamp originally recorded for that
   * layout.
   *
   * @param table The name of the kiji table.
   * @param keyValueBackup The KV backup information of the table to restore.
   * @throws IOException on I/O error.
   */
  void restoreKeyValuesFromBackup(String table, KeyValueBackup keyValueBackup) throws IOException;
}
