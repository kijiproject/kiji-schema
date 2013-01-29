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
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.apache.avro.Schema;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;

/**
 * The input data for a single entity in Kiji.  Implementations should be thread-safe.
 */
@ApiAudience.Public
@Inheritance.Sealed
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
   * Determines if a particular column has data in this row at a specific time.
   *
   * @param family A column fmaily.
   * @param qualifier A column qualifier.
   * @param timestamp A cell timestamp.
   * @return Whether the column has data in this row at the specified time.
   */
  boolean containsCell(String family, String qualifier, Long timestamp);

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
   * Gets the value stored within the specified cell.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param timestamp The timestamp of the cell.
   * @param <T> The type of the values stored at the specified coordinates.
   * @return the value of the specified cell, or null if the cell does not exist.
   *     Note: this method does not distinguish between Avro encoded nulls and non-existant
   *     cells. Use {@link #containsColumn()} to distinguish between this scenarios.
   * @throws IOException If there is an error.
   */
  <T> T getValue(String family, String qualifier, long timestamp) throws IOException;

  /**
   * Gets the value with the latest timestamp stored within the specified cell.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param <T> The type of the values stored at the specified coordinates.
   * @return the value of the specified cell, or null if the cell does not exist.
   *     Note: this method does not distinguish between Avro encoded nulls and non-existant
   *     cells.
   * @throws IOException If there is an error.
   */
  <T> T getMostRecentValue(String family, String qualifier) throws IOException;

  /**
   * Gets the value with the latest timestamp stored within the specified cell.
   *
   * @param family A column family name.
   * @param <T> The type of the values stored at the specified coordinates.
   * @return the value of the specified cell, or null if the cell does not exist.
   *     Note: this method does not distinguish between Avro encoded nulls and non-existant
   *     cells.
   * @throws IOException If there is an error.
   */
  <T> NavigableMap<String, T> getMostRecentValues(String family)
      throws IOException;

  /**
   * Gets all the timestamp-value pairs stored within the specified family.
   *
   * @param family A column family name.
   * @param <T> The type of the values stored at the specified coordinates.
   * @return A sorted map containing the values stored in the specified cell.
   * @throws IOException If there is an error.
   */
  <T> NavigableMap<String, NavigableMap<Long, T>> getValues(String family) throws IOException;

  /**
   * Gets all the timestamp-value pairs stored within the specified cell.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param <T> The type of the values stored at the specified coordinates.
   * @return A sorted map containing the values stored in the specified cell.
   * @throws IOException If there is an error.
   */
  <T> NavigableMap<Long, T> getValues(String family, String qualifier) throws IOException;

  /**
   * Gets the specified cell.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param timestamp The timestamp of the cell.
   * @param <T> The type of the values stored at the specified coordinates.
   * @return the specified cell, or null if the cell does not exist.
   * @throws IOException If there is an error.
   */
  <T> KijiCell<T> getCell(String family, String qualifier, long timestamp) throws IOException;

  /**
   * Gets the latest version of the specified cell.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param <T> The type of the values stored at the specified coordinates.
   * @return the most recent version of the specified cell, or null if the cell does not exist.
   * @throws IOException If there is an error.
   */
  <T> KijiCell<T> getMostRecentCell(String family, String qualifier) throws IOException;

  /**
   * Gets the latest version of the specified cell.
   *
   * @param family A column family name.
   * @param <T> The type of the values stored at the specified coordinates.
   * @return a map from qualifier to the most recent versions of the cells.
   * @throws IOException If there is an error.
   */
  <T> NavigableMap<String, KijiCell<T>> getMostRecentCells(String family) throws IOException;

  /**
   * Gets all the timestamp-cell pairs stored within the specified family.
   *
   * @param family A column family name.
   * @param <T> The type of the values stored at the specified coordinates.
   * @return A sorted map versions of the specified cell.
   * @throws IOException If there is an error.
   */
  <T> NavigableMap<String, NavigableMap<Long, KijiCell<T>>> getCells(String family)
      throws IOException;

  /**
   * Gets all the timestamp-cell pairs stored within the specified cell.
   *
   * @param family A column family name.
   * @param qualifier A column qualifier name.
   * @param <T> The type of the values stored at the specified coordinates.
   * @return A sorted map versions of the specified cell.
   * @throws IOException If there is an error.
   */
  <T> NavigableMap<Long, KijiCell<T>> getCells(String family, String qualifier)
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
