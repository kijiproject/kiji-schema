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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiColumnPagingNotEnabledException;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiPager;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiResult.Helpers;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.util.TimestampComparator;

/**
 * A {@code KijiRowData} implementation backed by a {@code KijiResult}.
 */
@ApiAudience.Private
public class KijiResultRowData implements KijiRowData {
  private final KijiTableLayout mLayout;
  private final KijiResult<Object> mResult;

  /**
   * Constructor for {@code KijiResultRowData}.
   *
   * @param layout The {@code KijiTableLayout} for the table.
   * @param result The {@code KijiResult} backing this {@code KijiRowData}.
   */
  public KijiResultRowData(final KijiTableLayout layout, final KijiResult<Object> result) {
    mLayout = layout;
    mResult = result;
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mResult.getEntityId();
  }

  /** {@inheritDoc} */
  @Override
  public boolean containsColumn(final String family, final String qualifier) {
    final KijiColumnName column = KijiColumnName.create(family, qualifier);
    return containsColumnRequest(column) && mResult.narrowView(column).iterator().hasNext();
  }

  /** {@inheritDoc} */
  @Override
  public boolean containsColumn(final String family) {
    return containsColumn(family, null);
  }

  /** {@inheritDoc} */
  @Override
  public boolean containsCell(final String family, final String qualifier, final long timestamp) {
    final KijiColumnName column = KijiColumnName.create(family, qualifier);
    return containsColumnRequest(column) && getCell(family, qualifier, timestamp) != null;
  }

  /** {@inheritDoc} */
  @Override
  public NavigableSet<String> getQualifiers(final String family) {
    final KijiColumnName column = KijiColumnName.create(family, null);
    validateColumnRequest(column);
    final NavigableSet<String> qualifiers = Sets.newTreeSet();
    for (final KijiCell<?> cell : mResult.narrowView(column)) {
      qualifiers.add(cell.getColumn().getQualifier());
    }
    return qualifiers;
  }

  /** {@inheritDoc} */
  @Override
  public NavigableSet<Long> getTimestamps(final String family, final String qualifier) {
    final KijiColumnName column = KijiColumnName.create(family, qualifier);
    validateColumnRequest(column);
    final NavigableSet<Long> timestamps = Sets.newTreeSet();
    for (final KijiCell<?> cell : mResult.narrowView(column)) {
      timestamps.add(cell.getTimestamp());
    }
    return timestamps;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getReaderSchema(final String family, final String qualifier) throws IOException {
    return mLayout.getCellSpec(KijiColumnName.create(family, qualifier)).getAvroSchema();
  }

  /** {@inheritDoc} */
  @Override
  public <T> T getValue(final String family, final String qualifier, final long timestamp) {
    final KijiCell<T> cell = getCell(family, qualifier, timestamp);
    return cell == null ? null : cell.getData();
  }

  /** {@inheritDoc} */
  @Override
  public <T> T getMostRecentValue(final String family, final String qualifier) {
    final KijiCell<T> cell = getMostRecentCell(family, qualifier);
    return cell == null ? null : cell.getData();
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, T> getMostRecentValues(final String family) {
    final KijiColumnName column = KijiColumnName.create(family, null);
    validateColumnRequest(column);
    Preconditions.checkState(mLayout.getFamilyMap().get(family).isMapType(),
        "getMostRecentValues(String family) is only enabled on map type column families."
            + " The column family [%s], is a group type column family."
            + " Please use the getMostRecentValues(String family, String qualifier) method.",
        family);

    final NavigableMap<String, T> qualifiers = Maps.newTreeMap();
    final Collection<Column> columnRequests = mResult.getDataRequest().getColumns();

    final Column familyRequest = mResult.getDataRequest().getRequestForColumn(family, null);
    if (familyRequest != null) {
      String previousQualifier = null;
      final KijiResult<T> narrowView = mResult.narrowView(column);
      for (final KijiCell<T> cell : narrowView) {
        final String qualifier = cell.getColumn().getQualifier();
        if (!qualifier.equals(previousQualifier)) {
          qualifiers.put(qualifier, cell.getData());
          previousQualifier = qualifier;
        }
      }
    } else {
      final List<KijiColumnName> requestedColumns = Lists.newArrayList();
      for (final Column columnRequest : columnRequests) {
        if (columnRequest.getFamily().equals(family)) {
          requestedColumns.add(columnRequest.getColumnName());
        }
      }

      for (final KijiColumnName requestedColumn : requestedColumns) {
        final KijiCell<T> cell = Helpers.getFirst(mResult.<T>narrowView(requestedColumn));
        if (cell != null) {
          qualifiers.put(cell.getColumn().getQualifier(), cell.getData());
        }
      }
    }
    return qualifiers;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, NavigableMap<Long, T>> getValues(final String family) {
    final KijiColumnName column = KijiColumnName.create(family, null);
    validateColumnRequest(column);
    Preconditions.checkState(mLayout.getFamilyMap().get(family).isMapType(),
        "getValues(String family) is only enabled on map type column families."
            + " The column family [%s], is a group type column family."
            + " Please use the getValues(String family, String qualifier) method.", family);

    final NavigableMap<String, NavigableMap<Long, T>> qualifiers = Maps.newTreeMap();
    for (final KijiCell<T> cell : mResult.<T>narrowView(column)) {
      NavigableMap<Long, T> columnValues = qualifiers.get(cell.getColumn().getQualifier());
      if (columnValues == null) {
        columnValues = Maps.newTreeMap(TimestampComparator.INSTANCE);
        qualifiers.put(cell.getColumn().getQualifier(), columnValues);
      }
      columnValues.put(cell.getTimestamp(), cell.getData());
    }
    return qualifiers;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<Long, T> getValues(final String family, final String qualifier) {
    final KijiColumnName column = KijiColumnName.create(family, qualifier);
    validateColumnRequest(column);

    final NavigableMap<Long, T> values = Maps.newTreeMap(TimestampComparator.INSTANCE);
    for (final KijiCell<T> cell : mResult.<T>narrowView(column)) {
      values.put(cell.getTimestamp(), cell.getData());
    }
    return values;
  }

  /** {@inheritDoc} */
  @Override
  public <T> KijiCell<T> getCell(
      final String family,
      final String qualifier,
      final long version
  ) {
    final KijiColumnName column = KijiColumnName.create(family, qualifier);
    validateColumnRequest(column);
    final KijiResult<T> columnView = mResult.narrowView(column);
    for (final KijiCell<T> cell : columnView) {
      final long cellVersion = cell.getTimestamp();
      if (cellVersion == version) {
        return cell;
      } else if (cellVersion < version) {
        break;
      }
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public <T> KijiCell<T> getMostRecentCell(final String family, final String qualifier) {
    final KijiColumnName column = KijiColumnName.create(family, qualifier);
    validateColumnRequest(column);
    return Helpers.getFirst(mResult.<T>narrowView(column));
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, KijiCell<T>> getMostRecentCells(final String family) {
    final KijiColumnName column = KijiColumnName.create(family, null);
    validateColumnRequest(column);
    Preconditions.checkState(mLayout.getFamilyMap().get(family).isMapType(),
        "getMostRecentCells(String family) is only enabled on map type column families."
            + " The column family [%s], is a group type column family."
            + " Please use the getMostRecentCells(String family, String qualifier) method.",
        family);

    final NavigableMap<String, KijiCell<T>> qualifiers = Maps.newTreeMap();
    final Collection<Column> columnRequests = mResult.getDataRequest().getColumns();

    final Column familyRequest = mResult.getDataRequest().getRequestForColumn(family, null);
    if (familyRequest != null) {
      String previousQualifier = null;
      for (final KijiCell<T> cell : mResult.<T>narrowView(column)) {
        final String qualifier = cell.getColumn().getQualifier();
        if (!qualifier.equals(previousQualifier)) {
          qualifiers.put(qualifier, cell);
          previousQualifier = qualifier;
        }
      }
    } else {
      final List<KijiColumnName> requestedColumns = Lists.newArrayList();
      for (final Column columnRequest : columnRequests) {
        if (columnRequest.getFamily().equals(family)) {
          requestedColumns.add(columnRequest.getColumnName());
        }
      }

      for (final KijiColumnName requestedColumn : requestedColumns) {
        final KijiCell<T> cell = Helpers.getFirst(mResult.<T>narrowView(requestedColumn));
        if (cell != null) {
          qualifiers.put(cell.getColumn().getQualifier(), cell);
        }
      }
    }
    return qualifiers;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, NavigableMap<Long, KijiCell<T>>> getCells(final String family) {
    final KijiColumnName column = KijiColumnName.create(family, null);
    validateColumnRequest(column);
    Preconditions.checkState(mLayout.getFamilyMap().get(family).isMapType(),
        "getCells(String family) is only enabled on map type column families."
            + " The column family [%s], is a group type column family."
            + " Please use the getCells(String family, String qualifier) method.", family);

    final NavigableMap<String, NavigableMap<Long, KijiCell<T>>> qualifiers = Maps.newTreeMap();
    for (final KijiCell<T> cell : mResult.<T>narrowView(column)) {
      NavigableMap<Long, KijiCell<T>> columnValues =
          qualifiers.get(cell.getColumn().getQualifier());
      if (columnValues == null) {
        columnValues = Maps.newTreeMap(TimestampComparator.INSTANCE);
        qualifiers.put(cell.getColumn().getQualifier(), columnValues);
      }
      columnValues.put(cell.getTimestamp(), cell);
    }
    return qualifiers;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<Long, KijiCell<T>> getCells(final String family, final String qualifier) {
    final KijiColumnName column = KijiColumnName.create(family, qualifier);
    validateColumnRequest(column);

    final NavigableMap<Long, KijiCell<T>> cells = Maps.newTreeMap(TimestampComparator.INSTANCE);
    for (final KijiCell<T> cell : mResult.<T>narrowView(column)) {
      cells.put(cell.getTimestamp(), cell);
    }
    return cells;
  }

  /** {@inheritDoc} */
  @Override
  public KijiPager getPager(
      final String family,
      final String qualifier
  ) throws KijiColumnPagingNotEnabledException {
    final KijiColumnName column = KijiColumnName.create(family, qualifier);

    final Column columnRequest = mResult.getDataRequest().getRequestForColumn(column);

    Preconditions.checkNotNull(columnRequest,
        "Requested column %s is not included in the data request %s.",
        column, mResult.getDataRequest());

    if (!columnRequest.isPagingEnabled()) {
      throw new KijiColumnPagingNotEnabledException(
          String.format("Requested column %s does not have paging enabled in data request %s.",
          column, mResult.getDataRequest()));
    }

    return new KijiResultPager(mResult.narrowView(column), mLayout);
  }

  /** {@inheritDoc} */
  @Override
  public KijiPager getPager(final String family) throws KijiColumnPagingNotEnabledException {
    final KijiColumnName column = KijiColumnName.create(family, null);
    Preconditions.checkState(mLayout.getFamilyMap().get(family).isMapType(),
        "getPager(String family) is only enabled on map type column families. "
            + "The column family '%s' is a group type column family. "
            + "Please use the getPager(String family, String qualifier) method.", family);

    final Column columnRequest = mResult.getDataRequest().getColumn(column);
    Preconditions.checkNotNull(columnRequest,
        "Requested column %s is not included in the data request %s.",
        column, mResult.getDataRequest());

    if (!columnRequest.isPagingEnabled()) {
      throw new KijiColumnPagingNotEnabledException(
          String.format("Requested column %s does not have paging enabled in data request %s.",
              column, mResult.getDataRequest()));
    }

    return new KijiResultQualifierPager(mResult.narrowView(column), mLayout);
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterator<KijiCell<T>> iterator(final String family, final String qualifier) {
    return this.<T>asIterable(family, qualifier).iterator();
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterator<KijiCell<T>> iterator(final String family) {
    final FamilyLayout familyLayout = mLayout.getFamilyMap().get(family);
    Preconditions.checkArgument(familyLayout != null, "Column %s has no data request.", family);
    Preconditions.checkState(familyLayout.isMapType(),
        "iterator(String family) is only enabled on map type column families."
            + " The column family [%s], is a group type column family."
            + " Please use the iterator(String family, String qualifier) method.", family);

    return iterator(family, null);
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterable<KijiCell<T>> asIterable(
      final String family,
      final String qualifier
  ) {
    final KijiColumnName column = KijiColumnName.create(family, qualifier);
    Preconditions.checkArgument(containsColumnRequest(column),
        "Column %s has no data request.", column, mResult.getDataRequest());

    return mResult.narrowView(column);
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterable<KijiCell<T>> asIterable(final String family) {
    return asIterable(family, null);
  }

  /**
   * Validates that the given {@code KijiColumnName} is requested in the {@code KijiDataRequest}
   * for this {@code KijiRowData}.  This means different things for different column types:
   *
   * <h4>Fully Qualified Column</h4>
   * <p>
   *   The column or its family must be in the data request.  The requested column or family may not
   *   be paged.
   * </p>
   *
   * <h4>Family Column</h4>
   * <p>
   *   The family, or at least one fully qualified column belonging to the family must be in the
   *   data request.  The family column request or the qualified column requests belonging to the
   *   family may not be paged.
   * </p>
   *
   * @param column The column for which to validate a request exists.
   */
  private void validateColumnRequest(final KijiColumnName column) {
    if (!containsColumnRequest(column)) {
      throw new NullPointerException(
          String.format("Requested column %s is not included in the data request %s.",
              column, mResult.getDataRequest()));
    }
  }

  /**
   * Returns whether the data request contains the column. This means different things for different
   * column types:
   *
   * <h4>Fully Qualified Column</h4>
   * <p>
   *   The column or its family must be in the data request.  The requested column or family may not
   *   be paged.
   * </p>
   *
   * <h4>Family Column</h4>
   * <p>
   *   The family, or at least one fully qualified column belonging to the family must be in the
   *   data request.  The family column request or the qualified column requests belonging to the
   *   family may not be paged.
   * </p>
   *
   * @param column The column for which to validate a request exists.
   * @return Whether the data request contains a request for the column.
   */
  private boolean containsColumnRequest(final KijiColumnName column) {
    final KijiDataRequest dataRequest = mResult.getDataRequest();
    final Column exactRequest = dataRequest.getColumn(column);
    if (exactRequest != null) {
      Preconditions.checkArgument(!exactRequest.isPagingEnabled(),
          "Paging is enabled for column %s in data request %s.", exactRequest, dataRequest);
      return true;
    } else if (column.isFullyQualified()) {
      // The column is fully qualified, but a request doesn't exist for the qualified column.
      // Check if the family is requested, and validate it.
      final Column familyRequest =
          dataRequest.getColumn(KijiColumnName.create(column.getFamily(), null));
      if (familyRequest == null) {
        return false;
      }
      Preconditions.checkArgument(
          !familyRequest.isPagingEnabled(),
          "Paging is enabled for column %s in data request %s.", familyRequest, dataRequest);
      return true;
    } else {
      boolean requestContainsColumns = false;
      for (final Column columnRequest : dataRequest.getColumns()) {
        if (columnRequest.getColumnName().getFamily().equals(column.getFamily())) {
          Preconditions.checkArgument(
              !columnRequest.isPagingEnabled(),
              "Paging is enabled for column %s in data request %s.", columnRequest, dataRequest);
          requestContainsColumns = true;
        }
      }
      return requestContainsColumns;
    }
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mLayout, mResult);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final KijiResultRowData other = (KijiResultRowData) obj;
    return Objects.equal(this.mLayout, other.mLayout) && Objects.equal(this.mResult, other.mResult);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("mLayout", mLayout)
        .add("mResult", mResult)
        .toString();
  }
}
