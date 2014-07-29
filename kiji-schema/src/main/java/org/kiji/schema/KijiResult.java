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

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultiset;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.KijiDataRequest.Column;

/**
 * A view of a row in a Kiji table.
 *
 * <p>
 *   The view allows access to the columns specified in a {@code KijiDataRequest}. The primary
 *   method of accessing the {@link KijiCell}s in the row is through the {@link #iterator()} method.
 *   The {@link #narrowView} method can be used to narrow the view of the row to a specific
 *   column or column family.
 * </p>
 *
 * <h2>Type Safety</h2>
 *
 * <p>
 *   The {@code KijiResult} API is not compile-time type safe if an inappropriate type is specified
 *   on any method which takes a type parameter and returns a {@code KijiRestult}, such as
 *   {@link #narrowView}. These methods allow the caller to specify the value-type of
 *   {@code KijiCell}s in the requested column. If the wrong type is supplied, a runtime
 *   {@link java.lang.ClassCastException} will be thrown.
 * </p>
 *
 * <p>
 *   In particular, users should be cautious of specifying a type-bound (other than {@link Object})
 *   on requests with multiple columns or group-type families. It is only appropriate to supply a
 *   more specific type bound over a group-type family when the family contains columns of a single
 *   type. It is only appropriate to supply a more specific type bound over the result of a
 *   {@code KijiDataRequest} with multiple column requests if all of the requested columns share the
 *   same type.
 * </p>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>
 *   {@code KijiResult} implementations may not be relied upon to be thread safe, and thus should
 *   not be shared between threads.
 * </p>
 *
 * <h2>Closing Resources</h2>
 *
 * <p>
 *   {@code KijiResult} instances must be closed if their {@code KijiDataRequest} includes paged
 *   columns. {@code KijiResult}s returned from {@link #narrowView} must be closed independently of
 *   the {@code KijiResult} they are created from, if the narrowed view contains paged columns.
 * </p>
 *
 * @param <T> the type of {@code KijiCell} values in the view. See the 'Type Safety' section above
 *     for more information.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
public interface KijiResult<T> extends Iterable<KijiCell<T>>, Closeable {


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
   * {@inheritDoc}
   *
   * <p>
   *   Multiple calls to {@code iterator()} will return independent iterators. If the
   *   {@code DataRequest} which defines this {@code KijiResult} contains paged columns, those
   *   pages will be requested from the underlying store for each call to this method. Two active
   *   iterators may hold two pages of data in memory simultaneously.
   * </p>
   *
   * <h2>Ordering</h2>
   *
   * <p>
   *   {@code KijiResult} provides four guarantees about the ordering of the {@code KijiCell}s
   *   in the returned iterator:
   * </p>
   *
   * <ol>
   *   <li> Non-paged columns appear before paged columns.
   *   <li> Within a column family, columns will appear in ascending lexicographic qualifier order.
   *   <li> Within a qualified-column, {@code KijiCell}s will appear in descending version order.
   *   <li> The ordering of columns will be consistent in repeated calls to {@link #iterator()} for
   *        the same {@code KijiResult}.
   * </ol>
   *
   * <p>
   *   Where two of these guarantees apply, the earlier guarantee takes precedence. For instance, if
   *   a {@code KijiDataRequest} contains requests for a paged column {@code fam:qual1}, and a
   *   non-paged column {@code fam:qual2}, then {@code fam:qual2} will be returned first in the
   *   iterator, despite {@code qual1} sorting before {@code qual2}.
   * </p>
   *
   * <p>
   *   No guarantee is made about the ordering among families.
   * </p>
   *
   * @return an iterator over all the cells in this KijiResult.
   * @throws KijiIOException on unrecoverable I/O exception.
   */
  @Override
  Iterator<KijiCell<T>> iterator();

  /**
   * Get a view of this {@code KijiResult} restricted to the provided column.
   *
   * <p>
   *   The column may be a column family, or a fully-qualified column. If this {@code KijiResult}
   *   does not include the provided column, then the resulting {@code KijiResult} will be empty,
   *   and its {@code KijiDataRequest} will not contain any columns. If the provided column is
   *   non-paged, the cells contained in the parent will be shared with the view.
   * </p>
   *
   * <p>
   *   This method allows the caller to specify a type-bound on the values of the {@code KijiCell}s
   *   of the returned {@code KijiResult}. The caller should be careful to only specify an
   *   appropriate type. If the type is too specific (or wrong), a runtime
   *   {@link java.lang.ClassCastException} will be thrown when the returned {@code KijiResult} is
   *   used. See the 'Type Safety' section of {@code KijiResult}'s documentation for more details.
   * </p>
   *
   * <p>
   *   If the requested column is paged, then the returned {@code KijiResult} must be closed
   *   independently of the {@code KijiResult} it is created from.
   * </p>
   *
   * @param column The column which will be contained in the returned view.
   * @param <U> The value type of the provided column.
   * @return a {@code KijiResult} which contains only the provided column.
   */
  <U extends T> KijiResult<U> narrowView(KijiColumnName column);

  /**
   * Helper methods for working with {@code KijiResult}s.
   */
  public static final class Helpers {

    /**
     * Return the first (most recent) {@code KijiCell} in the first column of this
     * {@code KijiResult}.
     *
     * <p>
     *   Note that if all columns in the {@code KijiResult} are paged, then retrieving the first
     *   cell will require fetching a full page.
     * </p>
     *
     * @param result The {@code KijiResult} containing the cell to get.
     * @param <T> The type of values in the provided {@code KijiResult}'s cells.
     * @return The first {@code KijiCell} in the result, or {@code null} if the result is empty.
     */
    public static <T> KijiCell<T> getFirst(final KijiResult<T> result) {
      return Iterables.getFirst(result, null);
    }

    /**
     * Return the value of the first (most recent) {@code KijiCell} in the first column of this
     * {@code KijiResult}.
     *
     * <p>
     *   Note that if all columns in the {@code KijiResult} are paged, then retrieving the first
     *   cell will require fetching a full page.
     * </p>
     *
     * <p>
     *   If the distinction between an empty result and a result with a {@code null} value is
     *   important, use {@link #getFirst} to retrieve the first cell.  The cell will be {@code null}
     *   if the result is empty.
     * </p>
     *
     * @param result The {@code KijiResult} containing the value to get.
     * @param <T> The type of values in the provided {@code KijiResult}'s cells.
     * @return the value of the first {@code KijiCell} in the result, or {@code null} if the result
     *    is empty, or the value is {@code null}.
     */
    public static <T> T getFirstValue(final KijiResult<T> result) {
      final KijiCell<T> first = getFirst(result);
      if (first == null) {
        return null;
      } else {
        return first.getData();
      }
    }

    /**
     * Return an {@link Iterable} of the {@code KijiCell} columns in this {@code KijiResult}.
     *
     * <p>
     *   The returned {@code Iterable}'s iteration order is the same as the order of cells in the
     *   provided {@code KijiResult}.
     * </p>
     *
     * <p>
     *   The returned {@code Iterable} will require fetching paged columns when iterated exactly
     *   as {@code KijiResult}s do.
     * </p>
     *
     * <p>
     *   The returned {@code Iterable} may contain duplicates for columns specified with a max
     *   number of versions greater than 1 in the result's data request. If the provided
     *   {@code KijiResult} will only be used for retrieving the columns in the row, and the version
     *   count is not important, set the max number of versions to retrieve in the
     *   {@link KijiDataRequest} for each column to {@code 1} to avoid retrieving unnecessary
     *   {@code KeyValues}s.
     * </p>
     *
     * <p>
     *   If the provided {@code KijiResult} will only be used retrieving the columns, the
     *   {@link org.kiji.schema.filter.StripValueRowFilter} can be used to avoid retrieving the
     *   values of each {@code KijiCell}.
     * </p>
     *
     * @param result The {@code KijiResult} containing the columns to get.
     * @return The columns in the provided {@code KijiResult}.
     */
    public static Iterable<KijiColumnName> getColumns(final KijiResult<?> result) {
      return Iterables.transform(
          result,
          new Function<KijiCell<?>, KijiColumnName>() {
            @Override
            public KijiColumnName apply(final KijiCell<?> cell) {
              return cell.getColumn();
            }
          });
    }

    /**
     * Return an {@link Iterable} of the {@code KijiCell} versions in this {@code KijiResult}. Note
     * that the returned {@code Iterable} will require fetching paged columns when iterated exactly
     * as {@code KijiResult}s do.
     *
     * <p>
     *   The returned {@code Iterable}'s iteration order is the same as the order of cells in the
     *   provided {@code KijiResult}.
     * </p>
     *
     * <p>
     *   If the provided {@code KijiResult} will only be used retrieving the versions, the
     *   {@link org.kiji.schema.filter.StripValueRowFilter} can be used to avoid retrieving the
     *   values of each {@code KijiCell}.
     * </p>
     *
     * @param result The {@code KijiResult} containing the versions to get.
     * @return The versions in the provided {@code KijiResult}.
     */
    public static Iterable<Long> getVersions(final KijiResult<?> result) {
      return Iterables.transform(
          result,
          new Function<KijiCell<?>, Long>() {
            @Override
            public Long apply(final KijiCell<?> cell) {
              return cell.getTimestamp();
            }
          });
    }

    /**
     * Return an {@link Iterable} of the {@code KijiCell} values in this {@code KijiResult}. Note
     * that the returned {@code Iterable} will require fetching paged columns when iterated exactly
     * as {@code KijiResult}s do.
     *
     * <p>
     *   The returned {@code Iterable}'s iteration order is the same as the order of cells in the
     *   provided {@code KijiResult}.
     * </p>
     *
     * @param result The {@code KijiResult} containing the values to get.
     * @param <T> The type of values in the provided {@code KijiResult}'s cells.
     * @return The values in the result.
     */
    public static <T> Iterable<T> getValues(final KijiResult<T> result) {
      return Iterables.transform(
          result,
          new Function<KijiCell<T>, T>() {
            @Override
            public T apply(final KijiCell<T> cell) {
              return cell.getData();
            }
          });
    }

    /**
     * Returns a {@link com.google.common.collect.Multiset} of {@code KijiColumnName} to version
     * count for the columns in the provided {@code KijiResult}.
     *
     * <p>
     *   The returned {@code Multiset}'s iteration order is the order of columns returned by this
     *   {@code KijiResult}, and the count is the number of versions of the column. Note that this
     *   will require fetching paged columns.
     * </p>
     *
     * <p>
     *   If the provided {@code KijiResult} will only be used retrieving the columns of the row,
     *   the {@link org.kiji.schema.filter.StripValueRowFilter} can be used to avoid retrieving the
     *   values of each {@code KijiCell}.
     * </p>
     *
     * @param result The {@code KijiResult} containing the columns to count.
     * @return A {@code Multiset} of {@code KijiColumnName} to version count.
     */
    public static LinkedHashMultiset<KijiColumnName> countColumns(final KijiResult<?> result) {
      final LinkedHashMultiset<KijiColumnName> set = LinkedHashMultiset.create();
      for (KijiCell<?> cell : result) {
        set.add(cell.getColumn());
      }
      return set;
    }

    /**
     * Returns a {@code SortedMap} of {@code KijiColumnName} to list of {@code KijiCell} of the
     * provided {@code KijiResult}.
     *<p>
     *   Should not be used with PagedKijiResults
     *</p>
     *
     * @param result {@code KijiResult} for which to get materialized result
     * @param <T> the type of values in the {@code KijiResult}
     * @return A {@code SortedMap} of each {@code KijiColumnName} to list of {@code KijiCell}
     * the contents of the materialized result
     */
    public static <T> SortedMap<KijiColumnName, List<KijiCell<T>>>
        getMaterializedContents(final KijiResult<T> result) {
      SortedMap<KijiColumnName, List<KijiCell<T>>> materializedResult =
          new TreeMap<KijiColumnName, List<KijiCell<T>>>();
      for (Column column: result.getDataRequest().getColumns()) {
        if (column.isPagingEnabled()) {
          throw new IllegalArgumentException(
              "Columns should not be paged when using MaterializedResult");
        }
        KijiColumnName columnName = column.getColumnName();
        List<KijiCell<T>> cells = ImmutableList.copyOf(result.narrowView(columnName).iterator());
        materializedResult.put(columnName, cells);
      }
      return materializedResult;
    }

    /** Private constructor for utility class. */
    private Helpers() {
    }
  }
}
