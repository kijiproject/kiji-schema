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

package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.Iterator;
import java.util.SortedMap;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiResult;
import org.kiji.schema.impl.DefaultKijiResult;
import org.kiji.schema.layout.CassandraColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;

/**
 * A paged {@link KijiResult} on a Cassandra Kiji table.
 *
 * @param <T> The value type of cells in the result.
 */
@ApiAudience.Private
public class CassandraPagedKijiResult<T> implements KijiResult<T> {
  private final EntityId mEntityId;
  private final KijiDataRequest mDataRequest;
  private final CassandraKijiTable mTable;
  private final KijiTableLayout mLayout;
  private final CassandraColumnNameTranslator mColumnTranslator;
  private final CellDecoderProvider mDecoderProvider;
  private final SortedMap<KijiColumnName, Iterable<KijiCell<T>>> mColumnResults;

  /**
   * Create a materialized {@code KijiResult} for a get on a Cassandra Kiji table.
   *
   * @param entityId The entity ID of the row to get.
   * @param dataRequest The data request defining the columns to get. All columns must be non-paged.
   * @param table The Cassandra Kiji table.
   * @param layout The layout of the table.
   * @param translator A column name translator for the table.
   * @param decoderProvider A decoder provider for the table.
   */
  public CassandraPagedKijiResult(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final CassandraKijiTable table,
      final KijiTableLayout layout,
      final CassandraColumnNameTranslator translator,
      final CellDecoderProvider decoderProvider
  ) {
    mEntityId = entityId;
    mDataRequest = dataRequest;
    mTable = table;
    mLayout = layout;
    mColumnTranslator = translator;
    mDecoderProvider = decoderProvider;

    final ImmutableSortedMap.Builder<KijiColumnName, Iterable<KijiCell<T>>> columnResults =
        ImmutableSortedMap.naturalOrder();

    for (Column columnRequest : mDataRequest.getColumns()) {
      final PagedColumnIterable columnIterable = new PagedColumnIterable(columnRequest);
      columnResults.put(columnRequest.getColumnName(), columnIterable);
    }

    mColumnResults = columnResults.build();
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
    return Iterables.concat(mColumnResults.values()).iterator();
  }

  /** {@inheritDoc} */
  @Override
  public <U extends T> KijiResult<U> narrowView(final KijiColumnName column) {
    final KijiDataRequest narrowRequest = DefaultKijiResult.narrowRequest(column, mDataRequest);

    return new CassandraPagedKijiResult<U>(
        mEntityId,
        narrowRequest,
        mTable,
        mLayout,
        mColumnTranslator,
        mDecoderProvider);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException { }

  /**
   * An iterable which starts a Cassandra scan for each requested iterator.
   */
  private final class PagedColumnIterable implements Iterable<KijiCell<T>> {
    private final Column mColumnRequest;

    /**
     * Creates an iterable which creates Cassandra queries for each requested iterator.
     *
     * @param columnRequest of column to scan.
     */
    private PagedColumnIterable(final Column columnRequest) {
      mColumnRequest = columnRequest;
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<KijiCell<T>> iterator() {
      return CassandraKijiResult.unwrapFuture(
          CassandraKijiResult.<T>getColumn(
              mTable.getURI(),
              mEntityId,
              mColumnRequest,
              mDataRequest,
              mLayout,
              mColumnTranslator,
              mDecoderProvider,
              mTable.getAdmin()));
    }
  }
}
