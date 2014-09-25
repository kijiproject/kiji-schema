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
import java.util.Set;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.mortbay.io.RuntimeIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.commons.IteratorUtils;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiResultScanner;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.impl.cassandra.RowDecoders.TokenRowKeyComponents;
import org.kiji.schema.impl.cassandra.RowDecoders.TokenRowKeyComponentsComparator;
import org.kiji.schema.layout.CassandraColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.layout.impl.ColumnId;

/**
 * {@inheritDoc}
 *
 * Cassandra implementation of {@code KijiResultScanner}.
 *
 * @param <T> type of {@code KijiCell} value returned by scanned {@code KijiResult}s.
 */
public class CassandraKijiResultScanner<T> implements KijiResultScanner<T> {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiResultScanner.class);
  private final Iterator<KijiResult<T>> mIterator;

 /*
  * ## Implementation Notes
  *
  * To perform a scan over a Kiji table, Cassandra Kiji creates an entityID scan over all locality
  * group tables in the scan, and then for each entity Id, creates a separate KijiResult. Creating
  * each Kiji result requires more requests to create the paged and non-paged columns.
  *
  * This has the downside of creating multiple separate CQL queries per Kiji row instead of a
  * finite number of scans which work through the data. This is the simplest way to implement
  * scanning at this point in time, but it may be possible in the future to use CQL scans.  CQL
  * scans have many downsides, though; often ALLOW FILTERING clause is needed, and we have
  * experienced poor performance and timeouts when using it.
  *
  * TODO: we could optimize this by using table scans to pull in the materialized rows as part of a
  * scan instead of issuing separate get requests for each row.  However, it is not clear that this
  * will be faster given the idiosyncrasies of Cassandra scans.
  */

  /**
   * Create a {@link KijiResultScanner} over a Cassandra Kiji table with the provided options.
   *
   * @param request The data request defining the columns to scan.
   * @param options Scan options optionally including start and stop tokens.
   * @param table The table to scan.
   * @param layout The layout of the table.
   * @param decoderProvider A cell decoder provider for the table.
   * @param translator A column name translator for the table.
   * @throws IOException On unrecoverable IO error.
   */
  public CassandraKijiResultScanner(
      final KijiDataRequest request,
      final CassandraKijiScannerOptions options,
      final CassandraKijiTable table,
      final KijiTableLayout layout,
      final CellDecoderProvider decoderProvider,
      final CassandraColumnNameTranslator translator

  ) throws IOException {

    final Set<ColumnId> localityGroups = Sets.newHashSet();

    for (final Column columnRequest : request.getColumns()) {
      final ColumnId localityGroupId =
          layout.getFamilyMap().get(columnRequest.getFamily()).getLocalityGroup().getId();
      localityGroups.add(localityGroupId);
    }

    final KijiURI tableURI = table.getURI();
    final List<CassandraTableName> tableNames = Lists.newArrayList();
    for (final ColumnId localityGroup : localityGroups) {
      final CassandraTableName tableName =
          CassandraTableName.getLocalityGroupTableName(tableURI, localityGroup);
      tableNames.add(tableName);
    }

    mIterator = Iterators.transform(
        getEntityIDs(tableNames, options, table, layout),
        new Function<EntityId, KijiResult<T>>() {
          /** {@inheritDoc} */
          @Override
          public KijiResult<T> apply(final EntityId entityId) {
            try {
              return CassandraKijiResult.create(
                  entityId,
                  request,
                  table,
                  layout,
                  translator,
                  decoderProvider);
            } catch (IOException e) {
              throw new RuntimeIOException(e);
            }
          }
        });
  }

  /**
   * Get an iterator of the entity IDs in a list of Cassandra Kiji tables that correspond to a
   * subset of cassandra tables in a Kiji table.
   *
   * @param tables The Cassandra tables to get Entity IDs from.
   * @param options The scan options. May specify start and stop tokens.
   * @param table The Kiji Cassandra table which the Cassandra tables belong to.
   * @param layout The layout of the Kiji Cassandra table.
   * @return An iterator of Entity IDs.
   */
  public static Iterator<EntityId> getEntityIDs(
      final List<CassandraTableName> tables,
      final CassandraKijiScannerOptions options,
      final CassandraKijiTable table,
      final KijiTableLayout layout
  ) {

    final List<ResultSetFuture> localityGroupFutures =
        FluentIterable
            .from(tables)
            .transform(
                new Function<CassandraTableName, Statement>() {
                  /** {@inheritDoc} */
                  @Override
                  public Statement apply(final CassandraTableName tableName) {
                    return CQLUtils.getEntityIDScanStatement(layout, tableName, options);
                  }
                })
            .transform(
                new Function<Statement, ResultSetFuture>() {
                  /** {@inheritDoc} */
                  @Override
                  public ResultSetFuture apply(final Statement statement) {
                    return table.getAdmin().executeAsync(statement);
                  }
                })
            // Force futures to execute by sending results to a list
            .toList();

    // We can use the DISTINCT optimization iff the entity ID contains only hashed components
    RowKeyFormat2 keyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();
    final boolean deduplicateComponents =
        keyFormat.getRangeScanStartIndex() != keyFormat.getComponents().size();

    if (deduplicateComponents) {
      LOG.warn("Scanning a Cassandra Kiji table with non-hashed entity ID components is"
              + " inefficient.  Consider hashing all entity ID components. Table: {}.",
          table.getURI());
    }

    final List<Iterator<TokenRowKeyComponents>> tokenRowKeyStreams =
        FluentIterable
            .from(localityGroupFutures)
            .transform(
                new Function<ResultSetFuture, Iterator<Row>>() {
                  /** {@inheritDoc} */
                  @Override
                  public Iterator<Row> apply(final ResultSetFuture future) {
                    return CassandraKijiResult.unwrapFuture(future).iterator();
                  }
                })
            .transform(
                new Function<Iterator<Row>, Iterator<TokenRowKeyComponents>>() {
                  /** {@inheritDoc} */
                  @Override
                  public Iterator<TokenRowKeyComponents> apply(final Iterator<Row> rows) {
                    return Iterators.transform(rows, RowDecoders.getRowKeyDecoderFunction(layout));
                  }
                })
            .transform(
                new Function<Iterator<TokenRowKeyComponents>, Iterator<TokenRowKeyComponents>>() {
                  /** {@inheritDoc} */
                  @Override
                  public Iterator<TokenRowKeyComponents> apply(
                      final Iterator<TokenRowKeyComponents> components
                  ) {
                    if (deduplicateComponents) {
                      return IteratorUtils.deduplicatingIterator(components);
                    } else {
                      return components;
                    }
                  }
                })
            .toList();

    return
        Iterators.transform(
            IteratorUtils.deduplicatingIterator(
                Iterators.mergeSorted(
                    tokenRowKeyStreams,
                    TokenRowKeyComponentsComparator.getInstance())),
            RowDecoders.getEntityIdFunction(table));
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public boolean hasNext() {
    return mIterator.hasNext();
  }

  @Override
  public KijiResult<T> next() {
    return mIterator.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
