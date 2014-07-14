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
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.apache.avro.Schema;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/**
 * KijiRowData provides a way for applications to access data read from a Kiji table.
 * KijiRowData objects contain a subset of the cells contained within a row in
 * a Kiji table. This subset is often determined by a {@link KijiDataRequest} object
 * that must be provided before a read operation.
 *
 * <p>
 *   KijiRowData objects should not be constructed directly, but rather should be
 *   obtained from a read operation. KijiRowData objects can be obtained by using a
 *   {@link KijiTableReader}, {@link KijiRowScanner}, or {@link KijiPager}.
 * </p>
 *
 * <h2>Reading cells</h2>
 * <p>
 *   Rows read from a Kiji table may contain multiple cells from multiple columns. To read the
 *   value from a particular column:
 * </p>
 * <pre>
 *   <code>
 *     final KijiRowData row = myTableReader.get(myEntityId, myDataRequest);
 *
 *     // Read the value stored in the column named 'info:name' with a timestamp of 42.
 *     final String name = row.getValue("info", "name", 42L);
 *   </code>
 * </pre>
 * <p>
 *   We also provide additional methods for accessing data stored withing a row. See the
 *   documentation below for methods with the name 'get*' for more information.
 * </p>
 *
 * <h2>Checking for existence of cells</h2>
 * <p>
 *   Since rows within a Kiji table may not contain all the columns defined within a layout
 *   we need a way to check this in code. KijiRowData contains several methods to help with
 *   this:
 * </p>
 * <pre>
 *   <code>
 *     final KijiRowData row = myTableReader.get(myEntityId, myDataRequest);
 *     if (row.containsColumn("info", "name")) {
 *       // Now that we know the cell exists, read the value.
 *       final String name = row.getMostRecentValue("info", "name");
 *     }
 *
 *     if (row.containsCell("info", "name", 42L)) {
 *       // Now that we know that this cell exists at the timestamp of 42.
 *       final String name = row.getValue("info", "name", 42L);
 *     }
 *   </code>
 * </pre>
 *
 * <h2>Dealing with large rows</h2>
 * <p>
 *   Rows in Kiji tables may contain large amounts of data. In some cases, this is caused by
 *   storing many versions of a cell with different timestamps in a row. This can become a
 *   problem when the size of a row/cell exceeds the amount of RAM available.
 *   To deal with large rows, you may use paging through a {@link KijiPager}.
 *   When paging is enabled on a column, cells from that column may only be accessed through
 *   {@link #getPager(String)} and {@link #getPager(String, String)}.
 *   See the documentation for {@link KijiPager} for more information.
 * </p>
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
public interface KijiRowData {
  /**
   * Gets the entity id for this row.
   *
   * @return The row key.
   */
  EntityId getEntityId();

  /**
   * Determines whether a particular column has data in this row.
   *
   * @param family Column family of the column to check for.
   * @param qualifier Column qualifier of the column to check for.
   * @return Whether the specified column has data in this row.
   */
  boolean containsColumn(String family, String qualifier);

  /**
   * Determines whether a particular column family has data in this row.
   *
   * @param family Column family to check for.
   * @return Whether the specified column family has data in this row.
   */
  boolean containsColumn(String family);

  /**
   * Determines whether a particular column has data in this row with the specified timestamp.
   *
   * @param family Column family of the column to check for.
   * @param qualifier Column qualifier of the column to check for.
   * @param timestamp Timestamp of the value to check for.
   * @return Whether the specified column has data in this row with the specified timestamp.
   */
  boolean containsCell(String family, String qualifier, long timestamp);

  /**
   * Gets the set of column qualifiers that exist in a column family in this row.
   *
   * @param family Column family to get column qualifiers from.
   * @return Set of column qualifiers that exist in the <code>family</code> column family.
   */
  NavigableSet<String> getQualifiers(String family);

  /**
   * Gets the set of timestamps on cells that exist in a column.
   *
   * <p>
   *   If iterating over the set, you will get timestamps in descending order.
   * </p>
   *
   * @param family Column family of the column to get timestamps from.
   * @param qualifier Column qualifier of the column to get timestamps from.
   * @return Set of all timestamps of cells in the <code>family:qualifier</code> column
   *     in this row in descending order.
   */
  NavigableSet<Long> getTimestamps(String family, String qualifier);

  /**
   * Gets the Avro reader schema used to decode the specified column, if any.
   *
   * @param family Column family of the desired column schema.
   * @param qualifier Column qualifier of the desired column schema.
   * @return Avro reader schema for the column.
   *     Null if the column is not encoded using Avro or if the writer schema was used.
   * @throws IOException If there is an error or the column does not exist.
   * @see org.kiji.schema.layout.KijiTableLayout
   */
  Schema getReaderSchema(String family, String qualifier)
      throws IOException;

  /**
   * Gets the data stored within the specified column with the specified timestamp.
   *
   * @param family Column family of the desired data.
   * @param qualifier Column qualifier of the desired data.
   * @param timestamp Timestamp of the desired data.
   * @param <T> Type of the data stored at the specified coordinates.
   * @return Data contained in the specified column with the specified timestamp, or null
   *     if the column or timestamp does not exist in this row. Note: this method does not
   *     distinguish between Avro encoded nulls and non-existent cells. Use
   *     {@link #containsColumn(String, String)} to distinguish between these scenarios.
   * @throws IOException If there is an error.
   */
  <T> T getValue(String family, String qualifier, long timestamp)
      throws IOException;

  /**
   * Gets the data stored within the specified column with the latest timestamp.
   *
   * @param family Column family of the desired data.
   * @param qualifier Column qualifier of the desired data.
   * @param <T> Type of the data stored at the specified coordinates.
   * @return Data contained in the specified column with the latest timestamp, or null
   *     if the column does not exist in this row. Note: this method does not distinguish
   *     between Avro encoded nulls and non-existent cells. Use
   *     {@link #containsColumn(String, String)} to distinguish between these scenarios.
   * @throws IOException If there is an error.
   */
  <T> T getMostRecentValue(String family, String qualifier)
      throws IOException;

  /**
   * Gets all data stored within the specified column family flattened to contain only
   * the data with the latest timestamps in each column.
   *
   * @param family Map type column family of the desired data.
   * @param <T> Type of the data stored at the specified coordinates.
   * @return Data contained in the specified column family flattened to contain only the
   *     data with the latest timestamps in each column. Note: this method does not distinguish
   *     between Avro encoded nulls and non-existent cells. Use
   *     {@link #containsColumn(String, String)} to distinguish between these scenarios.
   * @throws IOException If there is an error.
   */
  <T> NavigableMap<String, T> getMostRecentValues(String family)
      throws IOException;

  /**
   * Gets all data stored within the specified column family.
   *
   * @param family Map type column family of the desired data.
   * @param <T> Type of the data stored at the specified coordinates.
   * @return A sorted map containing the data stored in the specified column family.
   * @throws IOException If there is an error.
   */
  <T> NavigableMap<String, NavigableMap<Long, T>> getValues(String family)
      throws IOException;

  /**
   * Gets all data stored within the specified column.
   *
   * @param family Column family of the desired data.
   * @param qualifier Column qualifier of the desired data.
   * @param <T> Type of the data stored at the specified coordinates.
   * @return A sorted map containing the data stored in the specified column.
   * @throws IOException If there is an error.
   */
  <T> NavigableMap<Long, T> getValues(String family, String qualifier)
      throws IOException;

  /**
   * Gets the specified cell.
   *
   * @param family Column family of the desired cell.
   * @param qualifier Column qualifier of the desired cell.
   * @param timestamp Timestamp of the desired cell.
   * @param <T> Type of the cell stored at the specified coordinates.
   * @return Specified cell, or null if the cell does not exist.
   * @throws IOException If there is an error.
   */
  <T> KijiCell<T> getCell(String family, String qualifier, long timestamp)
      throws IOException;

  /**
   * Gets the cell in the specified column with the latest timestamp.
   *
   * @param family Column family of the desired cell.
   * @param qualifier Column qualifier of the desired cell.
   * @param <T> Type of the cell stored at the specified coordinates.
   * @return Cell in the specified column with the latest timestamp, or null if the cell
   *     does not exist.
   * @throws IOException If there is an error.
   */
  <T> KijiCell<T> getMostRecentCell(String family, String qualifier)
      throws IOException;

  /**
   * Gets the cells in the specified column family flattened to contain only the cells with
   * the latest timestamp in each column.
   *
   * @param family Map type column family of the desired cells.
   * @param <T> Type of the cells stored at the specified coordinates.
   * @return Map from column qualifier to the most recent versions of the cells.
   * @throws IOException If there is an error.
   */
  <T> NavigableMap<String, KijiCell<T>> getMostRecentCells(String family)
      throws IOException;

  /**
   * Gets all cells stored within the specified column family.
   *
   * @param family Map type column family of the desired cells.
   * @param <T> Type of the cells stored at the specified coordinates.
   * @return Sorted map versions of the specified cell.
   * @throws IOException If there is an error.
   */
  <T> NavigableMap<String, NavigableMap<Long, KijiCell<T>>> getCells(String family)
      throws IOException;

  /**
   * Gets all cells stored within the specified column.
   *
   * @param family Map type column family of the desired cells.
   * @param qualifier Column qualifier of the desired cells.
   * @param <T> Type of the cells stored at the specified coordinates.
   * @return A sorted map containing the cells stored in the specified column.
   * @throws IOException If there is an error.
   */
  <T> NavigableMap<Long, KijiCell<T>> getCells(String family, String qualifier)
      throws IOException;

  /**
   * Gets a KijiPager for the specified column.
   *
   * <p>
   *   The pager for a fully-qualified column allows one to list the versions of the column.
   *   Versions are retrieved in order (most recent first), one page at a time and include
   *   the cell content for each version (ie. for each timestamp).
   * </p>
   * <p> See {@link KijiPager} for more details on pagers. </p>
   * <p>
   *   If you don't need access to individual pages, you may use {@link ColumnVersionIterator}
   *   or {@link MapFamilyVersionIterator} instead.
   * </p>
   *
   * @param family Desired column family.
   * @param qualifier Desired column qualifier.
   * @return A pager for the specified column.
   * @throws KijiColumnPagingNotEnabledException If paging is not enabled for the
   *     specified column.
   *
   * @see KijiPager
   * @see ColumnVersionIterator
   * @see MapFamilyVersionIterator
   */
  KijiPager getPager(String family, String qualifier)
      throws KijiColumnPagingNotEnabledException;

  /**
   * Gets a KijiPager for the specified column family.
   *
   * <p>
   *   The pager for a column family allows one to list the qualifiers in the column family.
   *   Qualifiers are retrieved in order, one page at a time.
   *   No actual cell content is retrieved when using this pager.
   * </p>
   * <p> See {@link KijiPager} for more details on pagers. </p>
   * <p>
   *   If you don't need access to individual pages, you may use {@link MapFamilyQualifierIterator}
   *   or {@link MapFamilyVersionIterator} instead.
   * </p>
   *
   * @param family Desired map type column family.
   * @return A pager for the specified column.
   * @throws KijiColumnPagingNotEnabledException If paging is not enabled for the
   *     specified column family.
   *
   * @see KijiPager
   * @see MapFamilyQualifierIterator
   * @see MapFamilyVersionIterator
   */
  KijiPager getPager(String family)
      throws KijiColumnPagingNotEnabledException;

  /**
   * Gets an iterator over all cells for the specified column. Cells are returned sorted reverse
   * chronologically by version.
   *
   * @param family Column family of the desired cells.
   * @param qualifier Column qualifier of the desired cells.
   * @param <T> Type of the cells stored at the specified coordinates.
   * @return An iterator of the cells of the specified column.
   * @throws IOException If there is an error.
   */
  <T> Iterator<KijiCell<T>> iterator(String family, String qualifier) throws IOException;

  /**
   * Gets an iterator over all cells for the specified map type column. Cells are returned sorted
   * by the underlying HBase column name, and then reverse chronologically by version.
   *
   * @param family Map type column family of the desired cells.
   * @param <T> Type of the cells stored at the specified coordinates.
   * @return An iterator of the cells stored in the specified column.
   * @throws IOException If there is an error.
   */
  <T> Iterator<KijiCell<T>> iterator(String family) throws IOException;

  /**
   * Gets an iterable over all cells for the specified column.
   *
   * @param family Column family of the desired cells.
   * @param qualifier Column qualifier of the desired cells.
   * @param <T> Type of the cells stored at the specified coordinates.
   * @return An iterable of the cells of the specified column.
   * @throws IOException If there is an error.
   */
  <T> Iterable<KijiCell<T>> asIterable(String family, String qualifier) throws IOException;

  /**
   * Gets an iterable over all cells for the specified map type column.
   *
   * @param family Map type column family of the desired cells.
   * @param <T> Type of the cells stored at the specified coordinates.
   * @return An iterable of the cells stored in the specified column.
   * @throws IOException If there is an error.
   */
  <T> Iterable<KijiCell<T>> asIterable(String family) throws IOException;
}
