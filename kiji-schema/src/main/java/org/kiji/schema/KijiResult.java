/**
 * (c) Copyright 2014 WibiData, Inc.
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

import java.util.Iterator;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/** Wraps underlying database results to provide iteration and decoding. */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
public interface KijiResult extends Iterable<KijiCell<?>> {

  /**
   * Get the EntityId of this KijiResult.
   *
   * @return the EntityId of this KijiResult.
   */
  EntityId getEntityId();

  /**
   * Get the data request which defines this KijiResult.
   *
   * @return the data request which defines this KijiResult.
   */
  KijiDataRequest getDataRequest();

  /**
   * Get the most recent cell from the given column.
   *
   * <p>
   *   The column must be fully qualified and may not be marked as paged in the data request which
   *   defines this KijiResult.
   * </p>
   *
   * @param column name of the Kiji column from which to get the most recent cell.
   * @param <T> type of the value in the given column.
   * @return the most recent cell from the given column or null if no cells exist.
   * @throws NullPointerException if the given column is not included in the data request which
   *     defines this KijiResult.
   * @throws java.lang.IllegalArgumentException if the given column is marked as paged in the data
   *     request which defines this KijiResult.
   */
  <T> KijiCell<T> getMostRecentCell(KijiColumnName column);

  /**
   * Get the version of the cell in the given column at the given timestamp.
   *
   * <p>
   *   The column must be fully qualified and may not be marked as paged in the data request which
   *   defines this KijiResult.
   * </p>
   *
   * @param column name of the Kiji column from which to get the cell at the given timestamp.
   * @param timestamp timestamp at which to get the cell in the given column.
   * @param <T> type of the value in the given column.
   * @return the version of the cell in the given column at the given timestamp or null if none
   *     exists.
   * @throws NullPointerException if the given column is not included in the data request which
   *     defines this KijiResult.
   * @throws java.lang.IllegalArgumentException if the given column is marked as paged in the data
   *     request which defines this KijiResult.
   */
  <T> KijiCell<T> getCell(KijiColumnName column, long timestamp);

  /**
   * Get an iterator of the cells in the given qualified column or map-type family.
   *
   * <p>
   *   The column may not be a group-type family. The column may be marked as paged or unpaged in
   *   the data request which defines this KijiResult.
   * </p>
   *
   * <p>
   *   Multiple calls to <code>iterator(column)</code> with the same column will return separate
   *   iterators. If the column is paged, data will be re-requested from the underlying store and
   *   two active iterators on a paged column will hold two pages in memory simultaneously.
   * </p>
   *
   * <p>
   *   If the given column is a map-type family qualifiers are sorted lexicographically. Within one
   *   qualified column, cells are sorted most recent first.
   * </p>
   *
   * @param column qualified column or map-type family from which to get an iterator of cells.
   * @param <T> type of the value in the given column.
   * @return an iterator of the cells in the given qualified column or map-type family.
   * @throws NullPointerException if the given column is not included in the data request which
   *     defines this KijiResult.
   */
  <T> Iterator<KijiCell<T>> iterator(KijiColumnName column);

  /**
   * {@inheritDoc}
   *
   * <p>
   *   Multiple calls to <code>iterator()</code> will return separate iterators. If the column data
   *   request which defines this KijiResult contains paged columns, those pages will be
   *   re-requested from the underlying store. Two active iterators may hold two pages of data in
   *   memory simultaneously.
   * </p>
   *
   * <p>
   *   The order of columns in the iterator is not defined, but all non-paged columns will appear
   *   before all paged columns. Within one qualified column, cells are sorted most recent first.
   * </p>
   *
   * @return a new Iterator over all the cells in this KijiResult.
   */
  @Override
  Iterator<KijiCell<?>> iterator();
}
