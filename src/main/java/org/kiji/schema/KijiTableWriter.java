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

import org.apache.avro.generic.GenericContainer;

import org.kiji.schema.mapreduce.KijiDelete;
import org.kiji.schema.mapreduce.KijiIncrement;
import org.kiji.schema.mapreduce.KijiPut;

/**
 * Interface for performing writes to a Kiji table.
 *
 * Instantiated from {@link org.kiji.schema.KijiTable#openTableWriter()}
 */
public interface KijiTableWriter extends Closeable {
  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   */
  void put(EntityId entityId, String family, String qualifier, CharSequence value)
      throws IOException;

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   */
  void put(EntityId entityId, String family, String qualifier, GenericContainer value)
      throws IOException;

  /**
   * Puts a cell into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param cell The data to write.
   * @throws IOException If there is an IO error.
   */
  void put(EntityId entityId, String family, String qualifier, KijiCell<?> cell)
      throws IOException;

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   */
  void put(EntityId entityId, String family, String qualifier, boolean value)
      throws IOException;

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   */
  void put(EntityId entityId, String family, String qualifier, byte[] value)
      throws IOException;

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   */
  void put(EntityId entityId, String family, String qualifier, double value)
      throws IOException;

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   */
  void put(EntityId entityId, String family, String qualifier, float value)
      throws IOException;

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   */
  void put(EntityId entityId, String family, String qualifier, int value)
      throws IOException;

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   */
  void put(EntityId entityId, String family, String qualifier, long value)
      throws IOException;

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp The timestamp to put the data at
   *     (use HConstants.LATEST_TIMESTAMP for current time).
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   */
  void put(EntityId entityId, String family, String qualifier, long timestamp, CharSequence value)
      throws IOException;

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp The timestamp to put the data at
   *     (use HConstants.LATEST_TIMESTAMP for current time).
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   */
  void put(EntityId entityId, String family, String qualifier, long timestamp,
      GenericContainer value) throws IOException;

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp The timestamp to put the data at
   *     (use HConstants.LATEST_TIMESTAMP for current time).
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   */
  void put(EntityId entityId, String family, String qualifier, long timestamp, boolean value)
      throws IOException;

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp The timestamp to put the data at
   *     (use HConstants.LATEST_TIMESTAMP for current time).
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   */
  void put(EntityId entityId, String family, String qualifier, long timestamp, byte[] value)
      throws IOException;

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp The timestamp to put the data at
   *     (use HConstants.LATEST_TIMESTAMP for current time).
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   */
  void put(EntityId entityId, String family, String qualifier, long timestamp, double value)
      throws IOException;

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp The timestamp to put the data at
   *     (use HConstants.LATEST_TIMESTAMP for current time).
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   */
  void put(EntityId entityId, String family, String qualifier, long timestamp, float value)
      throws IOException;

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp The timestamp to put the data at
   *     (use HConstants.LATEST_TIMESTAMP for current time).
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   */
  void put(EntityId entityId, String family, String qualifier, long timestamp, int value)
      throws IOException;

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp The timestamp to put the data at
   *     (use HConstants.LATEST_TIMESTAMP for current time).
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   */
  void put(EntityId entityId, String family, String qualifier, long timestamp, long value)
      throws IOException;

  /**
   * Puts a cell into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp The timestamp to put the data at
   *     (use HConstants.LATEST_TIMESTAMP for current time).
   * @param cell The data to write.
   * @throws IOException If there is an IO error.
   */
  void put(EntityId entityId, String family, String qualifier, long timestamp, KijiCell<?> cell)
      throws IOException;

  /**
   * Increments a counter in a kiji table.
   *
   * <p>This method will throw an exception if called on a column that isn't a counter.</p>
   *
   * @param entityId The entity (row) that contains the counter.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param amount The amount to increment the counter (may be negative).
   * @return The new counter value, post increment.
   * @throws IOException If there is an IO error.
   */
  KijiCounter increment(EntityId entityId, String family, String qualifier, long amount)
      throws IOException;

  /**
   * Sets a counter value in a kiji table.
   *
   * <p>This method will throw an exception if called on a column that isn't a counter.</p>
   *
   * @param entityId The entity (row) that contains the counter.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param value The value to set the counter to.
   * @throws IOException If there is an IO error.
   */
  void setCounter(EntityId entityId, String family, String qualifier, long value)
      throws IOException;

  /**
   * Deletes everything from a row.
   *
   * @param entityId The entity (row) to delete.
   * @throws IOException If there is an IO error.
   */
  void deleteRow(EntityId entityId)
      throws IOException;

  /**
   * Delete all cells from a row with a timestamp less than or equal to the specified timestamp.
   *
   * @param entityId The entity (row) to delete data from.
   * @param upToTimestamp A timestamp.
   * @throws IOException If there is an IO error.
   */
  void deleteRow(EntityId entityId, long upToTimestamp)
      throws IOException;

  /**
   * Deletes all versions of all cells in a family.
   *
   * @param entityId The entity (row) to delete data from.
   * @param family A column family.
   * @throws IOException If there is an IO error.
   */
  void deleteFamily(EntityId entityId, String family)
      throws IOException;

  /**
   * Deletes all cells from a family with a timestamp less than or equal to the specified timestamp.
   *
   * @param entityId The entity (row) to delete data from.
   * @param family A column family.
   * @param upToTimestamp A timestamp.
   * @throws IOException If there is an IO error.
   */
  void deleteFamily(EntityId entityId, String family, long upToTimestamp)
      throws IOException;

  /**
   * Deletes all versions of all cells in a column.
   *
   * @param entityId The entity (row) to delete data from.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @throws IOException If there is an IO error.
   */
  void deleteColumn(EntityId entityId, String family, String qualifier)
      throws IOException;

  /**
   * Deletes all cells with a timestamp less than or equal to the specified timestamp.
   *
   * @param entityId The entity (row) to delete data from.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param upToTimestamp A timestamp.
   * @throws IOException If there is an IO error.
   */
  void deleteColumn(EntityId entityId, String family, String qualifier, long upToTimestamp)
      throws IOException;

  /**
   * Deletes the most recent version of the cell in a column.
   *
   * @param entityId The entity (row) to delete data from.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @throws IOException If there is an IO error.
   */
  void deleteCell(EntityId entityId, String family, String qualifier)
      throws IOException;

  /**
   * Deletes a single cell with a specified timestamp.
   *
   * @param entityId The entity (row) to delete data from.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp The timestamp of the cell to delete (use HConstants.LATEST_TIMESTAMP
   *     to delete the most recent cell in the column).
   * @throws IOException If there is an IO error.
   */
  void deleteCell(EntityId entityId, String family, String qualifier, long timestamp)
      throws IOException;

  /**
   * Flushes the pending modifications to the table.
   *
   * @throws IOException If there is an error.
   */
  void flush()
      throws IOException;

  /**
   * Closes this writer and all open connections that it is currently maintaining.
   *
   * @throws IOException If there is an error.
   */
  void close()
      throws IOException;

  // Methods to implement (if using BaseKijiTableWriter).
  /**
   * Performs the specified put.
   *
   * @param put The put to perform.
   * @throws IOException If there is an error.
   */
  void put(KijiPut put)
      throws IOException;

  /**
   * Performs the specified increment.
   *
   * @param increment The increment to perform.
   * @throws IOException If there is an error.
   * @return The new counter value.
   */
  KijiCounter increment(KijiIncrement increment)
      throws IOException;

  /**
   * Performs the specified delete.
   *
   * @param delete The delete to perform.
   * @throws IOException If there is an error.
   */
  void delete(KijiDelete delete)
      throws IOException;
}
