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

package org.kiji.schema.impl;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiResult;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * A {@link KijiResult} backed by a map of {@code KijiColumnName} to
 * {@code List&lt;KijiCell&lt;T&gt;&gt;}.
 *
 * @param <T> The type of {@code KijiCell} values in the view.
 */
@ApiAudience.Private
public final class MaterializedKijiResult<T> implements KijiResult<T> {
  private final EntityId mEntityId;
  private final KijiDataRequest mDataRequest;
  private final SortedMap<KijiColumnName, List<KijiCell<T>>> mColumns;

  /**
   * Create a {@code MaterializedKijiResult}.
   *
   * @param entityId The entity ID of the row containing this result.
   * @param dataRequest The Kiji data request which defines the columns in this result.
   * @param columns The materialized results.
   */
  private MaterializedKijiResult(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final SortedMap<KijiColumnName, List<KijiCell<T>>> columns
  ) {
    mEntityId = entityId;
    mDataRequest = dataRequest;
    mColumns = columns;
  }

  /**
   * Create a new materialized {@code KijiResult} backed by the provided {@code KijiCell}s.
   *
   * @param entityId The entity ID of the row containing this result.
   * @param dataRequest The Kiji data request which defines the columns in this result.
   * @param layout The Kiji table layout of the table.
   * @param columns The materialized results. The cells must be in the order guaranteed by
   *     {@code KijiResult}. Must be mutable, and should not be modified after passing in.
   * @param <T> The type of {@code KijiCell} values in the view.
   * @return A new materialized {@code KijiResult} backed by {@code KijiCell}s.
   */
  public static <T> MaterializedKijiResult<T> create(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final KijiTableLayout layout,
      final SortedMap<KijiColumnName, List<KijiCell<T>>> columns
  ) {
    // We have to sort the results from group-type column families because the KijiResult API does
    // not specify the order of columns in a group-type family.  We rely on the columns being
    // ordered so that we can use binary search.

    final List<Map.Entry<KijiColumnName, List<KijiCell<T>>>> groupFamilyEntries =
        Lists.newArrayListWithCapacity(columns.size());
    for (Map.Entry<KijiColumnName, List<KijiCell<T>>> entry : columns.entrySet()) {
      final KijiColumnName column = entry.getKey();
      if (!column.isFullyQualified()
          && layout.getFamilyMap().get(column.getFamily()).isGroupType()) {
        groupFamilyEntries.add(entry);
      }
    }

    if (groupFamilyEntries.isEmpty()) {
      return new MaterializedKijiResult<T>(entityId, dataRequest, columns);
    }

    for (Map.Entry<KijiColumnName, List<KijiCell<T>>> entry : groupFamilyEntries) {
      final KijiColumnName groupFamily = entry.getKey();
      final List<KijiCell<T>> sortedColumn = entry.getValue();
      Collections.sort(sortedColumn, KijiCell.getKeyComparator());
      columns.put(groupFamily, sortedColumn);
    }

    return new MaterializedKijiResult<T>(entityId, dataRequest, columns);
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mEntityId;
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    return mDataRequest;
  }

  /** {@inheritDoc} */
  @Override
  public Iterator<KijiCell<T>> iterator() {
    return Iterables.concat(mColumns.values()).iterator();
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  @Override
  public <U extends T> KijiResult<U> narrowView(final KijiColumnName column) {
    final KijiDataRequest narrowRequest = DefaultKijiResult.narrowRequest(column, mDataRequest);
    if (narrowRequest.equals(mDataRequest)) {
      return (KijiResult<U>) this;
    }

    final ImmutableSortedMap.Builder<KijiColumnName, List<KijiCell<U>>> narrowedColumns =
        ImmutableSortedMap.naturalOrder();

    for (Column columnRequest : narrowRequest.getColumns()) {
      final KijiColumnName requestedColumn = columnRequest.getColumnName();

      // (Object) cast is necessary. Might be: http://bugs.java.com/view_bug.do?bug_id=6548436
      final List<KijiCell<U>> exactColumn =
          (List<KijiCell<U>>) (Object) mColumns.get(requestedColumn);
      if (exactColumn != null) {

        // We get here IF

        // `column` is a family, and `mDataRequest` contains a column request for the entire family.

        // OR

        // `column` is a family, and `mDataRequest` contains a column request for a qualified column
        // in the family.

        // OR

        // `column` is a qualified-column, and `mDataRequest` contains a request for the qualified
        // column.

        narrowedColumns.put(requestedColumn, exactColumn);
      } else {

        // `column` is a qualified-column, and `mDataRequest` contains a column request for the
        // column's family.

        final List<KijiCell<T>> familyCells =
            mColumns.get(KijiColumnName.create(requestedColumn.getFamily(), null));
        final List<KijiCell<T>> qualifierCells = getQualifierCells(requestedColumn, familyCells);
        narrowedColumns.put(requestedColumn, (List<KijiCell<U>>) (Object) qualifierCells);
      }
    }

    return new MaterializedKijiResult<U>(
        mEntityId,
        narrowRequest,
        narrowedColumns.build());
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    // No-op
  }

  /**
   * Get the {@code KijiCell}s in a list of cells from a family which belong to the specified
   * fully-qualified column.
   *
   * @param column The fully-qualified column to retrieve cells for.
   * @param familyCells The sorted list of cells belonging to a Kiji family.
   * @return The {@code Cell}s for the qualified column.
   * @param <T> The type of {@code KijiCell}s in the family.
   */
  private static <T> List<KijiCell<T>> getQualifierCells(
      final KijiColumnName column,
      final List<KijiCell<T>> familyCells
  ) {
    if (familyCells.size() == 0) {
      return ImmutableList.of();
    }

    final KijiCell<T> start = KijiCell.create(column, Long.MAX_VALUE, null);
    final KijiCell<T> end = KijiCell.create(column, -1L, null);

    int startIndex =
        Collections.binarySearch(familyCells, start, KijiCell.getKeyComparator());
    if (startIndex < 0) {
      startIndex = -1 - startIndex;
    }

    int endIndex =
        Collections.binarySearch(familyCells, end, KijiCell.getKeyComparator());
    if (endIndex < 0) {
      endIndex = -1 - endIndex;
    }

    return familyCells.subList(startIndex, endIndex);
  }
}
