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
import java.util.Iterator;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiResult;

/**
 * A {@link KijiResult} which proxies all calls to a pair of paged and materialized Kiji results.
 *
 * @param <T> The type of {@code KijiCell} values in the view.
 */
@NotThreadSafe
@ApiAudience.Private
public final class DefaultKijiResult<T> implements KijiResult<T> {

  private final KijiDataRequest mDataRequest;
  private final KijiResult<T> mMaterializedResult;
  private final KijiResult<T> mPagedResult;

  /**
   * Construct a new {@code DefaultKijiResult} with the provided data request, paged kiji result,
   * and materialized kiji result.
   *
   * @param dataRequest The data request for the result.
   * @param materializedResult The materialized result.
   * @param pagedResult The paged result.
   */
  private DefaultKijiResult(
      final KijiDataRequest dataRequest,
      final KijiResult<T> materializedResult,
      final KijiResult<T> pagedResult
  ) {
    mDataRequest = dataRequest;
    mMaterializedResult = materializedResult;
    mPagedResult = pagedResult;
  }

  /**
   * Create a new {@code DefaultKijiResult} with the provided data request, paged result,
   * and materialized result.
   *
   * @param dataRequest The data request for the result.
   * @param materializedResult The materialized result.
   * @param pagedResult The paged result.
   * @param <T> The type of {@code KijiCell} values in the result.
   * @return A {@code KijiResult} wrapping the provided materialized and paged results.
   */
  public static <T> KijiResult<T> create(
      final KijiDataRequest dataRequest,
      final KijiResult<T> materializedResult,
      final KijiResult<T> pagedResult
  ) {
    return new DefaultKijiResult<T>(dataRequest, materializedResult, pagedResult);
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mPagedResult.getEntityId();
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    return mDataRequest;
  }

  /** {@inheritDoc} */
  @Override
  public Iterator<KijiCell<T>> iterator() {
    return Iterables.concat(mMaterializedResult, mPagedResult).iterator();
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public <U extends T> KijiResult<U> narrowView(final KijiColumnName column) {
    final KijiDataRequest narrowRequest = narrowRequest(column, mDataRequest);

    if (narrowRequest.isEmpty()) {
      return new EmptyKijiResult<U>(mMaterializedResult.getEntityId(), narrowRequest);
    }

    boolean containsPagedColumns = false;
    boolean containsUnpagedColumns = false;
    for (Column columnRequest : narrowRequest.getColumns()) {
      if (columnRequest.isPagingEnabled()) {
        containsPagedColumns = true;
      } else {
        containsUnpagedColumns = true;
      }
      if (containsPagedColumns && containsUnpagedColumns) {
        return DefaultKijiResult.create(
            narrowRequest,
            mMaterializedResult.<U>narrowView(column),
            mPagedResult.<U>narrowView(column));
      }
    }

    if (containsPagedColumns) {
      return mPagedResult.narrowView(column);
    } else {
      return mMaterializedResult.narrowView(column);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    mPagedResult.close();
  }

  // -----------------------------------------------------------------------------------------------
  // Helper methods
  // -----------------------------------------------------------------------------------------------

  /**
   * Narrow a {@link KijiDataRequest} to a column.  Will return a new data request. The column may
   * be fully qualified or a family.
   *
   * @param column to narrow data request.
   * @param dataRequest to narrow.
   * @return a data request narrowed to the specified column.
   */
  public static KijiDataRequest narrowRequest(
      final KijiColumnName column,
      final KijiDataRequest dataRequest
  ) {
    final List<Column> columnRequests = getColumnRequests(column, dataRequest);

    final KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.withTimeRange(dataRequest.getMinTimestamp(), dataRequest.getMaxTimestamp());
    for (Column columnRequest : columnRequests) {
      builder.newColumnsDef(columnRequest);
    }

    return builder.build();
  }

  /**
   * Retrieve the column requests corresponding to a Kiji column in a {@code KijiDataRequest}.
   *
   * <p>
   * If the requested column is fully qualified, and the request contains a family request
   * containing the column, a new {@code Column} request will be created which corresponds to
   * the requested family narrowed to the qualifier.
   * </p>
   *
   * @param column a fully qualified {@link KijiColumnName}
   * @param dataRequest the data request to get column request from.
   * @return the column request.
   */
  private static List<Column> getColumnRequests(
      final KijiColumnName column,
      final KijiDataRequest dataRequest
  ) {
    final Column exactRequest = dataRequest.getColumn(column);
    if (exactRequest != null) {
      return ImmutableList.of(exactRequest);
    }

    if (column.isFullyQualified()) {
      // The column is fully qualified, but a request doesn't exist for the qualified column.
      // Check if the family is requested, and if so create a new qualified-column request from it.
      final Column familyRequest =
          dataRequest.getRequestForColumn(KijiColumnName.create(column.getFamily(), null));
      if (familyRequest == null) {
        return ImmutableList.of();
      }
      ColumnsDef columnDef = ColumnsDef
          .create()
          .withFilter(familyRequest.getFilter())
          .withPageSize(familyRequest.getPageSize())
          .withMaxVersions(familyRequest.getMaxVersions())
          .add(column.getFamily(), column.getQualifier(), familyRequest.getReaderSpec());

      return ImmutableList.of(
          KijiDataRequest.builder().addColumns(columnDef).build().getColumn(column));
    } else {
      // The column is a family, but a request doesn't exist for the entire family add all requests
      // for individual columns in the family.
      ImmutableList.Builder<Column> columnRequests = ImmutableList.builder();
      for (Column columnRequest : dataRequest.getColumns()) {
        if (columnRequest.getColumnName().getFamily().equals(column.getFamily())) {
          columnRequests.add(columnRequest);
        }
      }
      return columnRequests.build();
    }
  }
}
