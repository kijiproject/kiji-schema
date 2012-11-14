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
import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

/**
 * The input data for a single entity in Kiji.  Implementations should be thread-safe.
 */
public interface KijiRowData {
  /**
   * Gets the entity id for this row of kiji data.
   *
   * @return The row key.
   */
  EntityId getEntityId();

  /**
   * Determines whether a particular column has data in this row.
   *
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @return Whether the column has data in this row.
   */
  boolean containsColumn(String family, String qualifier);

  /**
   * Determines whether a particular column family has any data in this row.
   *
   * @param family A column family.
   * @return Whether the family has data in this row.
   */
  boolean containsColumn(String family);

  /**
   * Gets the set of column qualifiers that exist in a family.
   *
   * @param family A column family name.
   * @return The set of column qualifiers that exist in <code>family</code>.
   */
  NavigableSet<String> getQualifiers(String family);

  /**
   * Gets the set of timestamps on cells that exist in a column.
   *
   * <p>If iterating over the set, you will get items in order of decreasing timestamp.</p>
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @return The set of all timestamps of cells in the <code>family:qualifier</code> column.
   */
  NavigableSet<Long> getTimestamps(String family, String qualifier);

  /**
   * Gets the reader schema for a column as declared in the layout of the table this row
   * comes from.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @return The Avro reader schema for the column.
   * @throws IOException If there is an error or the column does not exist.
   */
  Schema getReaderSchema(String family, String qualifier) throws IOException;

  /**
   * Gets the most recent cell in a column.
   *
   * @param <T> The type of the decoded avro cell data.
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param readerSchema The reader schema for the cell data, or null if the writer schema
   *     should be used for decoding.
   * @return The cell, or null if there is no data.
   * @throws IOException If there is an error.
   */
  <T> KijiCell<T> getCell(String family, String qualifier, Schema readerSchema) throws IOException;

  /**
   * Gets a decoded avro value from the most recent cell in a column.
   *
   * @param <T> The type of the decoded avro cell data.
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param readerSchema The reader schema for the cell data, or null if the writer schema
   *     should be used for decoding.
   * @return The contents of the cell, or null if there is no data.
   * @throws IOException If there is an error.
   */
  <T> T getValue(String family, String qualifier, Schema readerSchema) throws IOException;

  /**
   * Gets a cell with a given timestamp in a column.
   *
   * @param <T> The type of the decoded avro cell data.
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param timestamp The timestamp of the cell.
   * @param readerSchema The reader schema for the cell data, or null if the writer schema
   *     should be used for decoding.
   * @return The cell, or null if there is no data.
   * @throws IOException If there is an error.
   */
  <T> KijiCell<T> getCell(String family, String qualifier, long timestamp, Schema readerSchema)
      throws IOException;

  /**
   * Gets a decoded avro value from the cell with a given timestamp in a column.
   *
   * @param <T> The type of the decoded avro cell data.
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param timestamp The timestamp of the cell.
   * @param readerSchema The reader schema for the cell data, or null if the writer schema
   *     should be used for decoding.
   * @return The contents of the cell, or null if there is no data.
   * @throws IOException If there is an error.
   */
  <T> T getValue(String family, String qualifier, long timestamp, Schema readerSchema)
      throws IOException;

  /**
   * Gets the most recent cell in a column.
   *
   * @param <T> The type of the decoded avro cell data.
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param type The class for the decoded data.
   * @return The contents of the cell, or null if there is no data.
   * @throws IOException If there is an error.
   */
  <T extends SpecificRecord> KijiCell<T> getCell(String family, String qualifier, Class<T> type)
      throws IOException;

  /**
   * Gets a decoded avro value from the most recent cell in a column.
   *
   * @param <T> The type of the decoded avro cell data.
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param type The class for the decoded data.
   * @return The contents of the cell, of null if there is no data.
   * @throws IOException If there is an error.
   */
  <T extends SpecificRecord> T getValue(String family, String qualifier, Class<T> type)
      throws IOException;

  /**
   * Gets a specified version of a cell in a column.
   *
   * @param <T> The type of the decoded avro cell data.
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param timestamp The timestamp of the cell.
   * @param type The class for the decoded data.
   * @return The cell, or null if there is no data.
   * @throws IOException If there is an error.
   */
  <T extends SpecificRecord> KijiCell<T> getCell(
      String family, String qualifier, long timestamp, Class<T> type) throws IOException;

  /**
   * Gets a decoded avro value from the specified version of a cell in a column.
   *
   * @param <T> The type of the decoded avro cell data.
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param timestamp The timestamp of the cell.
   * @param type The class for the decoded data.
   * @return The contents of the cell, or null if there is no data.
   * @throws IOException If there is an error.
   */
  <T extends SpecificRecord> T getValue(
      String family, String qualifier, long timestamp, Class<T> type) throws IOException;

  /**
   * Gets the most recent counter from a column.
   *
   * <p>Only columns that have been declared with {@code <schema type="counter"/>} may be
   * read using this method, otherwise an exception is thrown.</p>
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @return The most recent counter.
   * @throws IOException If there is an error.
   */
  KijiCounter getCounter(String family, String qualifier) throws IOException;

  /**
   * Reads a counter as it was at a particular timestamp.
   *
   * <p>Only columns that have been declared with {@code <schema type="counter"/>} may be
   * read using this method, otherwise an exception is thrown.</p>
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param timestamp The timestamp of the cell.
   * @return The counter.
   * @throws IOException If there is an error.
   */
  KijiCounter getCounter(String family, String qualifier, long timestamp) throws IOException;

  /**
   * Reads the current value of a column that is a counter.
   *
   * <p>Only columns that have been declared with {@code <schema type="counter"/>} may be
   * read using this method.</p>
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @return The current value of the counter.
   * @throws IOException If there is an error.
   */
  long getCounterValue(String family, String qualifier) throws IOException;

  /**
   * Reads the value of a counter column as it was at a particular timestamp.
   *
   * <p>Only columns that have been declared with {@code <schema type="counter"/>} may be
   * read using this method.</p>
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param timestamp The timestamp of the cell.
   * @return The current value of the counter.
   * @throws IOException If there is an error.
   */
  long getCounterValue(String family, String qualifier, long timestamp) throws IOException;

  /**
   * Gets all the typed avro values in the family.
   *
   * <p>If iterating over the keys of the returned inner map from timestamps to values,
   * you will get items in order of decreasing timestamp.</p>
   *
   * @param <T> The type of the decoded avro cell data.
   * @param family A column family name.
   * @param readerSchema The reader schema for the cell data, or null if the writer schema
   *     should be used for decoding.
   * @return A map from qualifier name to a map from timestamps to cell values.
   * @throws IOException If there is an error.
   */
  <T> NavigableMap<String, NavigableMap<Long, T>> getValues(String family, Schema readerSchema)
      throws IOException;

  /**
   * Gets all the typed versioned values of a column.
   *
   * <p>If iterating over the keys of the map from timestamps to values,
   * you will get items in order of decreasing timestamp.</p>
   *
   * @param <T> The type of the decoded avro cell data.
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param readerSchema The reader schema for the cell data, or null if the writer schema
   *     should be used for decoding.
   * @return A map from timestamp to the decoded avro cell value.
   * @throws IOException If there is an error.
   */
  <T> NavigableMap<Long, T> getValues(String family, String qualifier, Schema readerSchema)
      throws IOException;

  /**
   * Gets all the specific typed values in a column family.
   *
   * <p>If iterating over the keys of the returned inner map from timestamps to values,
   * you will get items in order of decreasing timestamp.</p>
   *
   * @param <T> The type of the decoded avro cell data.
   * @param family A column family name.
   * @param type The type of the decoded avro data.
   * @return A map from column qualifier/key to a map from timestamps to cell value.
   * @throws IOException If there is an error.
   */
  <T extends SpecificRecord> NavigableMap<String, NavigableMap<Long, T>> getValues(
      String family, Class<T> type) throws IOException;

  /**
   * Gets the most recent values in a column family.
   *
   * @param <T> The type of the decoded avro cell data.
   * @param family A column family name.
   * @param readerSchema The reader schema for the cell data, or null if the writer schema
   *     should be used for decoding.
   * @return A map from column qualifier/key to the most recently-timestamped cell value.
   * @throws IOException If there is an error.
   */
  <T> NavigableMap<String, T> getRecentValues(String family, Schema readerSchema)
      throws IOException;

  /**
   * Gets the most recent specific-typed values in a column family.
   *
   * @param <T> The type of the decoded avro cell data.
   * @param family A column family name.
   * @param type The type of the decoded avro data.
   * @return A map from column qualifier/key to the most recently-timestamped cell value.
   * @throws IOException If there is an error.
   */
  <T extends SpecificRecord> NavigableMap<String, T> getRecentValues(String family, Class<T> type)
      throws IOException;

  /**
   * Gets all the typed versioned values in a column.
   *
   * <p>If iterating over the keys of the map from timestamps to values,
   * you will get items in order of decreasing timestamp.</p>
   *
   * @param <T> The type of the decoded avro cell data.
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param type The type of the decoded avro cell data.
   * @return A map from timestamp to the decoded avro cell values.
   * @throws IOException If there is an error.
   */
  <T extends SpecificRecord> NavigableMap<Long, T> getValues(
      String family, String qualifier, Class<T> type) throws IOException;

  /**
   * Convenience method for reading the most recent cell as a string.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @return The cell's value decoded as a CharSequence.
   * @throws IOException If there is an error.
   */
  CharSequence getStringValue(String family, String qualifier) throws IOException;

  /**
   * Convenience method for reading the most recent cell as a string.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param timestamp The timestamp of the cell.
   * @return The cell's value decoded as a CharSequence.
   * @throws IOException If there is an error.
   */
  CharSequence getStringValue(String family, String qualifier, long timestamp) throws IOException;

  /**
   * Convenience method for reading the most recent cell as an integer.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @return The cell's value decoded as an integer.
   * @throws IOException If there is an error.
   */
  Integer getIntValue(String family, String qualifier) throws IOException;

  /**
   * Convenience method for reading the most recent cell as an integer.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param timestamp The timestamp of the cell.
   * @return The cell's value decoded as an integer.
   * @throws IOException If there is an error.
   */
  Integer getIntValue(String family, String qualifier, long timestamp) throws IOException;

  /**
   * Convenience method for reading the most recent cell as a long.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @return The cell's value decoded as an integer.
   * @throws IOException If there is an error.
   */
  Long getLongValue(String family, String qualifier) throws IOException;

  /**
   * Convenience method for reading the most recent cell as a long.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param timestamp The timestamp of the cell.
   * @return The cell's value decoded as an integer.
   * @throws IOException If there is an error.
   */
  Long getLongValue(String family, String qualifier, long timestamp) throws IOException;

  /**
   * Convenience method for reading the most recent cell as a float.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @return The cell's value decoded as a float.
   * @throws IOException If there is an error.
   */
  Float getFloatValue(String family, String qualifier) throws IOException;

  /**
   * Convenience method for reading the most recent cell as a float.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param timestamp The timestamp of the cell.
   * @return The cell's value decoded as a float.
   * @throws IOException If there is an error.
   */
  Float getFloatValue(String family, String qualifier, long timestamp) throws IOException;

  /**
   * Convenience method for reading the most recent cell as a double.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @return The cell's value decoded as a double.
   * @throws IOException If there is an error.
   */
  Double getDoubleValue(String family, String qualifier) throws IOException;

  /**
   * Convenience method for reading the most recent cell as a double.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param timestamp The timestamp of the cell.
   * @return The cell's value decoded as a double.
   * @throws IOException If there is an error.
   */
  Double getDoubleValue(String family, String qualifier, long timestamp) throws IOException;

  /**
   * Convenience method for reading the most recent cell as a byte array.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @return The cell's value decoded as a byte array.
   * @throws IOException If there is an error.
   */
  ByteBuffer getByteArrayValue(String family, String qualifier) throws IOException;

  /**
   * Convenience method for reading the most recent cell as a byte array.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param timestamp The timestamp of the cell.
   * @return The cell's value decoded as a byte array.
   * @throws IOException If there is an error.
   */
  ByteBuffer getByteArrayValue(String family, String qualifier, long timestamp) throws IOException;

  /**
   * Convenience method for reading the most recent cell as a boolean.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @return The cell's value decoded as an integer.
   * @throws IOException If there is an error.
   */
  Boolean getBooleanValue(String family, String qualifier) throws IOException;

  /**
   * Convenience method for reading the most recent cell as a boolean.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param timestamp The timestamp of the cell.
   * @return The cell's value decoded as an integer.
   * @throws IOException If there is an error.
   */
  Boolean getBooleanValue(String family, String qualifier, long timestamp) throws IOException;

  /**
   * Gets all the string-typed versioned values in a column.
   *
   * <p>If iterating over the keys of the map from timestamps to values,
   * you will get items in order of decreasing timestamp.</p>
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @return A map from timestamp to the decoded avro cell value as a CharSequence.
   * @throws IOException If there is an error.
   */
  NavigableMap<Long, CharSequence> getStringValues(String family, String qualifier)
      throws IOException;

  /**
   * Gets all the integer-typed versioned values in a column.
   *
   * <p>If iterating over the keys of the map from timestamps to values,
   * you will get items in order of decreasing timestamp.</p>
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @return A map from timestamp to the decoded avro cell value as an integer.
   * @throws IOException If there is an error.
   */
  NavigableMap<Long, Integer> getIntValues(String family, String qualifier) throws IOException;

  /**
   * Gets all the long-typed versioned values in a column.
   *
   * <p>If iterating over the keys of the map from timestamps to values,
   * you will get items in order of decreasing timestamp.</p>
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @return A map from timestamp to the decoded avro cell value as a long.
   * @throws IOException If there is an error.
   */
  NavigableMap<Long, Long> getLongValues(String family, String qualifier) throws IOException;

  /**
   * Gets all the float-typed versioned values in a column.
   *
   * <p>If iterating over the keys of the map from timestamps to values,
   * you will get items in order of decreasing timestamp.</p>
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @return A map from timestamp to the decoded avro cell value as a float.
   * @throws IOException If there is an error.
   */
  NavigableMap<Long, Float> getFloatValues(String family, String qualifier) throws IOException;

  /**
   * Gets all the double-typed versioned values in a column.
   *
   * <p>If iterating over the keys of the map from timestamps to values,
   * you will get items in order of decreasing timestamp.</p>
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @return A map from timestamp to the decoded avro cell value as a double.
   * @throws IOException If there is an error.
   */
  NavigableMap<Long, Double> getDoubleValues(String family, String qualifier) throws IOException;

  /**
   * Gets all the boolean-typed versioned values in a column.
   *
   * <p>If iterating over the keys of the map from timestamps to values,
   * you will get items in order of decreasing timestamp.</p>
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @return A map from timestamp to the decoded avro cell value as a boolean.
   * @throws IOException If there is an error.
   */
  NavigableMap<Long, Boolean> getBooleanValues(String family, String qualifier) throws IOException;

  /**
   * Gets all the byte array-typed versioned values in a column.
   *
   * <p>If iterating over the keys of the map from timestamps to values,
   * you will get items in order of decreasing timestamp.</p>
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @return A map from timestamp to the decoded avro cell value as a byte array.
   * @throws IOException If there is an error.
   */
  NavigableMap<Long, ByteBuffer> getByteArrayValues(String family, String qualifier)
      throws IOException;

  /**
   * Populates the next page of cells for a column.
   *
   * <p>If there are cells remaining, this KijiRowData object is populated with the next
   * page of cells for the requested column and the method returns true. Otherwise, data
   * from the column's previous page is removed, and this method returns false. This
   * method throws an IOException if the column does not have paging enabled in the
   * KijiDataRequest.</p>
   *
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @return Whether a new page of cells has been fetched.
   * @throws IOException If there is an error.
   */
  boolean nextPage(String family, String qualifier) throws IOException;

  /**
   * Populates the next page of cells for a requested column family.
   *
   * <p>If there are cells remaining, this KijiRowData object is populated with the next
   * page of cells for the requested family and the method returns true. Otherwise, data
   * from the family's previous page is removed, and this method returns false. This
   * method throws an IOException if the column does not have paging enabled in the
   * KijiDataRequest.</p>
   *
   * <p>An IOException is thrown if this method is called on a family that was not
   * requested explicitly in the KijiDataRequest. For example, consider the following:
   *
   * <pre>
   *   new KijiDataRequest()
   *       .addColumn(new KijiDataRequest.Column("foo").withPageSize(10))
   *       .addColumn(new KijiDataRequest.Column("bar", "baz").withPageSize(10));
   * </pre>
   *
   * It would be valid to call nextPage("foo"), but calling nextPage("bar") would throw an
   * IOException, since paging was not enabled on the entire "bar" family.</p>
   *
   * @param family A column family.
   * @return Whether a new page of cells has been fetched.
   * @throws IOException If there is an error.
   */
  boolean nextPage(String family) throws IOException;
}
