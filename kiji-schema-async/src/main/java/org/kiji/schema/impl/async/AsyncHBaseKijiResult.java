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

package org.kiji.schema.impl.async;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterables;
import org.hbase.async.KeyValue;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiResult;
import org.kiji.schema.impl.EmptyKijiResult;
import org.kiji.schema.impl.hbase.HBaseKijiResult;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;

/**
 * A {@link KijiResult} backed by AsyncHBase.
 *
 * @param <T> The type of {@code KijiCell} values in the view.
 */
@ApiAudience.Private
public final class AsyncHBaseKijiResult<T> implements KijiResult<T> {

  private final KijiDataRequest mDataRequest;
  private final AsyncHBaseMaterializedKijiResult<T> mMaterializedResult;
  private final AsyncHBasePagedKijiResult<T> mPagedResult;

  /**
   * Initialize a new {@link AsyncHBaseKijiResult}.
   *
   * @param dataRequest The data request which defines the columns held by this Kiji result.
   * @param materializedResult The materialized (unpaged) portion of this Kiji result.
   * @param pagedResult The paged portion of this Kiji result.
   */
  private AsyncHBaseKijiResult(
      final KijiDataRequest dataRequest,
      final AsyncHBaseMaterializedKijiResult<T> materializedResult,
      final AsyncHBasePagedKijiResult<T> pagedResult
  ) {
    mDataRequest = dataRequest;
    mMaterializedResult = materializedResult;
    mPagedResult = pagedResult;
  }

  /**
   * Create a new {@link AsyncHBaseKijiResult}.
   *
   * @param entityId EntityId of the row from which to read cells.
   * @param dataRequest KijiDataRequest defining the values to retrieve.
   * @param unpagedRawResult The unpaged results from the row.
   * @param table The table being viewed.
   * @param layout The layout of the table.
   * @param columnTranslator A column name translator for the table.
   * @param decoderProvider A cell decoder provider for the table.
   * @param <T> The type of value in the {@code KijiCell} of this view.
   * @return an {@code AsyncKijiResult}.
   * @throws IOException if error while decoding cells.
   */
  public static <T> KijiResult<T> create(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final List<KeyValue> unpagedRawResult,
      final AsyncHBaseKijiTable table,
      final KijiTableLayout layout,
      final HBaseColumnNameTranslator columnTranslator,
      final CellDecoderProvider decoderProvider
  ) throws IOException {
    final Collection<Column> columnRequests = dataRequest.getColumns();
    final KijiDataRequestBuilder unpagedRequestBuilder = KijiDataRequest.builder();
    final KijiDataRequestBuilder pagedRequestBuilder = KijiDataRequest.builder();
    unpagedRequestBuilder.withTimeRange(
        dataRequest.getMinTimestamp(),
        dataRequest.getMaxTimestamp());
    pagedRequestBuilder.withTimeRange(dataRequest.getMinTimestamp(), dataRequest.getMaxTimestamp());

    for (Column columnRequest : columnRequests) {
      if (columnRequest.isPagingEnabled()) {
        pagedRequestBuilder.newColumnsDef(columnRequest);
      } else {
        unpagedRequestBuilder.newColumnsDef(columnRequest);
      }
    }

    final CellDecoderProvider requestDecoderProvider =
        decoderProvider.getDecoderProviderForRequest(dataRequest);

    final KijiDataRequest unpagedRequest = unpagedRequestBuilder.build();
    final KijiDataRequest pagedRequest = pagedRequestBuilder.build();

    if (unpagedRequest.isEmpty() && pagedRequest.isEmpty()) {
      return new EmptyKijiResult<T>(entityId, dataRequest);
    }

    final AsyncHBaseMaterializedKijiResult<T> materializedKijiResult;
    if (!unpagedRequest.isEmpty()) {
      materializedKijiResult =
          AsyncHBaseMaterializedKijiResult.create(
              entityId,
              unpagedRequest,
              unpagedRawResult,
              layout,
              columnTranslator,
              requestDecoderProvider);
    } else {
      materializedKijiResult = null;
    }

    final AsyncHBasePagedKijiResult<T> pagedKijiResult;
    if (!pagedRequest.isEmpty()) {
      pagedKijiResult =
          new AsyncHBasePagedKijiResult<T>(
              entityId,
              pagedRequest,
              table,
              layout,
              columnTranslator,
              requestDecoderProvider);
    } else {
      pagedKijiResult = null;
    }

    if (unpagedRequest.isEmpty()) {
      return pagedKijiResult;
    } else if (pagedRequest.isEmpty()) {
      return materializedKijiResult;
    } else {
      return new AsyncHBaseKijiResult<T>(dataRequest, materializedKijiResult, pagedKijiResult);
    }
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
    final KijiDataRequest narrowRequest = HBaseKijiResult.narrowRequest(column, mDataRequest);

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
        return new AsyncHBaseKijiResult<U>(
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
}
