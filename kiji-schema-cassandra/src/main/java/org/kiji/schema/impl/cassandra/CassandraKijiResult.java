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
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiURI;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.cassandra.CassandraColumnName;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.impl.DefaultKijiResult;
import org.kiji.schema.impl.EmptyKijiResult;
import org.kiji.schema.impl.MaterializedKijiResult;
import org.kiji.schema.layout.CassandraColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.layout.impl.ColumnId;

/**
 * A utility class which can create a {@link KijiResult} view on a Cassandra Kiji table.
 */
@ApiAudience.Private
public final class CassandraKijiResult {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiResult.class);

  /**
   * Create a new {@link KijiResult} backed by Cassandra.
   *
   * @param entityId EntityId of the row from which to read cells.
   * @param dataRequest KijiDataRequest defining the values to retrieve.
   * @param table The table being viewed.
   * @param layout The layout of the table.
   * @param columnTranslator A column name translator for the table.
   * @param decoderProvider A cell decoder provider for the table.
   * @param <T> The type of value in the {@code KijiCell} of this view.
   * @return an {@code KijiResult}.
   * @throws IOException On error while decoding cells.
   */
  public static <T> KijiResult<T> create(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final CassandraKijiTable table,
      final KijiTableLayout layout,
      final CassandraColumnNameTranslator columnTranslator,
      final CellDecoderProvider decoderProvider
  ) throws IOException {
    final KijiDataRequestBuilder unpagedRequestBuilder = KijiDataRequest.builder();
    final KijiDataRequestBuilder pagedRequestBuilder = KijiDataRequest.builder();
    unpagedRequestBuilder.withTimeRange(
        dataRequest.getMinTimestamp(),
        dataRequest.getMaxTimestamp());
    pagedRequestBuilder.withTimeRange(dataRequest.getMinTimestamp(), dataRequest.getMaxTimestamp());

    for (final Column columnRequest : dataRequest.getColumns()) {
      if (columnRequest.getFilter() != null) {
        throw new UnsupportedOperationException(
            String.format("Cassandra Kiji does not support filters on column requests: %s.",
                columnRequest));
      }
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

    final MaterializedKijiResult<T> materializedKijiResult;
    if (!unpagedRequest.isEmpty()) {
      materializedKijiResult =
          createMaterialized(
              table.getURI(),
              entityId,
              unpagedRequest,
              layout,
              columnTranslator,
              requestDecoderProvider,
              table.getAdmin());
    } else {
      materializedKijiResult = null;
    }

    final CassandraPagedKijiResult<T> pagedKijiResult;
    if (!pagedRequest.isEmpty()) {
      pagedKijiResult =
          new CassandraPagedKijiResult<T>(
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
      return DefaultKijiResult.create(dataRequest, materializedKijiResult, pagedKijiResult);
    }
  }

  /**
   * Create a materialized {@code KijiResult} for a get on a Cassandra Kiji table.
   *
   * @param tableURI The table URI.
   * @param entityId The entity ID of the row to get.
   * @param dataRequest The data request defining the columns to get. All columns must be non-paged.
   * @param layout The layout of the table.
   * @param translator A column name translator for the table.
   * @param decoderProvider A decoder provider for the table.
   * @param admin The Cassandra connection.
   * @param <T> The value type of cells in the result.
   * @return A materialized {@code KijiResult} for the row.
   */
  public static <T> MaterializedKijiResult<T> createMaterialized(
      final KijiURI tableURI,
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final KijiTableLayout layout,
      final CassandraColumnNameTranslator translator,
      final CellDecoderProvider decoderProvider,
      final CassandraAdmin admin
  ) {

    SortedMap<KijiColumnName, ListenableFuture<Iterator<KijiCell<T>>>> resultFutures =
        Maps.newTreeMap();

    for (final Column columnRequest : dataRequest.getColumns()) {
      Preconditions.checkArgument(
          !columnRequest.isPagingEnabled(),
          "CassandraMaterializedKijiResult can not be created with a paged data request: %s.",
          dataRequest);

      resultFutures.put(
          columnRequest.getColumnName(),
          CassandraKijiResult.<T>getColumn(
              tableURI,
              entityId,
              columnRequest,
              dataRequest,
              layout,
              translator,
              decoderProvider,
              admin));
    }

    SortedMap<KijiColumnName, List<KijiCell<T>>> results = Maps.newTreeMap();
    for (Map.Entry<KijiColumnName, ListenableFuture<Iterator<KijiCell<T>>>> entry
        : resultFutures.entrySet()) {

      results.put(
          entry.getKey(),
          Lists.newArrayList(CassandraKijiResult.unwrapFuture(entry.getValue())));
    }

    return MaterializedKijiResult.create(entityId, dataRequest, layout, results);
  }

  // CSOFF: ParameterNumber
  /**
   * Query Cassandra for a Kiji qualified-column or column-family in a Kiji row. The result is a
   * future containing an iterator over the result cells.
   *
   * @param tableURI The table URI.
   * @param entityId The entity ID of the row in the Kiji table.
   * @param columnRequest The requested column.
   * @param dataRequest The data request defining the request options.
   * @param layout The table's layout.
   * @param translator A column name translator for the table.
   * @param decoderProvider A decoder provider for the table.
   * @param admin The Cassandra connection to use for querying.
   * @param <T> The value type of the column.
   * @return A future containing an iterator of cells in the column.
   */
  public static <T> ListenableFuture<Iterator<KijiCell<T>>> getColumn(
      final KijiURI tableURI,
      final EntityId entityId,
      final Column columnRequest,
      final KijiDataRequest dataRequest,
      final KijiTableLayout layout,
      final CassandraColumnNameTranslator translator,
      final CellDecoderProvider decoderProvider,
      final CassandraAdmin admin
  ) {
    final KijiColumnName column = columnRequest.getColumnName();
    final CassandraColumnName cassandraColumn;
    try {
      cassandraColumn = translator.toCassandraColumnName(column);
    } catch (NoSuchColumnException e) {
      throw new IllegalArgumentException(
          String.format("No such column '%s' in table %s.", column, tableURI));
    }

    final ColumnId localityGroupId =
        layout.getFamilyMap().get(column.getFamily()).getLocalityGroup().getId();
    final CassandraTableName table =
        CassandraTableName.getLocalityGroupTableName(tableURI, localityGroupId);

    if (column.isFullyQualified()) {

      final Statement statement =
          CQLUtils.getQualifiedColumnGetStatement(
              layout,
              table,
              entityId,
              cassandraColumn,
              dataRequest,
              columnRequest);

      return Futures.transform(
          admin.executeAsync(statement),
          RowDecoders.<T>getQualifiedColumnDecoderFunction(column, decoderProvider));
    } else {

      if (columnRequest.getMaxVersions() != 0) {
        LOG.warn("Cassandra Kiji can not efficiently get a column family with max versions"
                + " (column family: {}, max version: {}). Filtering versions on the client.",
            column, columnRequest.getMaxVersions());
      }

      if (dataRequest.getMaxTimestamp() != Long.MAX_VALUE
          || dataRequest.getMinTimestamp() != Long.MIN_VALUE) {
        LOG.warn("Cassandra Kiji can not efficiently restrict a timestamp on a column family: "
                + " (column family: {}, data request: {}). Filtering timestamps on the client.",
            column, dataRequest);
      }

      final Statement statement =
          CQLUtils.getColumnFamilyGetStatement(
              layout,
              table,
              entityId,
              cassandraColumn,
              columnRequest);

      return Futures.transform(
          admin.executeAsync(statement),
          RowDecoders.<T>getColumnFamilyDecoderFunction(
              table,
              column,
              columnRequest,
              dataRequest,
              layout,
              translator,
              decoderProvider));
    }
  }
  // CSON: ParameterNumber

  /**
   * Unwrap a Cassandra listenable future.
   *
   * @param future The future to unwrap.
   * @param <T> The value type of the future.
   * @return The future's value.
   */
  public static <T> T unwrapFuture(final ListenableFuture<T> future) {
    // See DefaultResultSetFuture#getUninterruptibly
    try {
      return Uninterruptibles.getUninterruptibly(future);
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof DriverException) {
        throw (DriverException) cause;
      } else {
        throw new DriverInternalError("Unexpected exception thrown", cause);
      }
    }
  }

  /**
   * Constructor for non-instantiable helper class.
   */
  private CassandraKijiResult() { }
}
