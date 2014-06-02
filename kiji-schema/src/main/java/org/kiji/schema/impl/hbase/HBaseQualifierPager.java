/**
 * (c) Copyright 2013 WibiData, Inc.
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

package org.kiji.schema.impl.hbase;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiColumnPagingNotEnabledException;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.filter.Filters;
import org.kiji.schema.filter.KijiColumnFilter;
import org.kiji.schema.filter.KijiColumnRangeFilter;
import org.kiji.schema.filter.StripValueColumnFilter;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.impl.KijiPaginationFilter;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.Debug;

/**
 * Pages through the many qualifiers of a map-type family.
 *
 * <p>
 *   The max-versions parameter on a map-type family applies on a per-qualifier basis.
 *   This does not limit the total number of versions returned for the entire map-type family.
 * </p>
 */
@ApiAudience.Private
public final class HBaseQualifierPager implements Iterator<String[]>, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseQualifierPager.class);

  /** Entity ID of the row being paged through. */
  private final EntityId mEntityId;

  /** HBase KijiTable to read from. */
  private final HBaseKijiTable mTable;

  /** Name of the map-type family being paged through. */
  private final KijiColumnName mFamily;

  /** Full data request. */
  private final KijiDataRequest mDataRequest;

  /** Column data request for the map-type family to page through. */
  private final KijiDataRequest.Column mColumnRequest;

  /** True only if there is another page of data to read through {@link #next()}. */
  private boolean mHasNext;

  /**
   * Highest qualifier (according to the HBase bytes comparator) returned so far.
   * This is the low bound (exclusive) for qualifiers to retrieve next.
   */
  private String mMinQualifier = null;

  /**
   * Initializes a qualifier pager.
   *
   * @param entityId The entityId of the row.
   * @param dataRequest The requested data.
   * @param table The Kiji table that this row belongs to.
   * @param family Iterate through the qualifiers from this map-type family.
   * @throws KijiColumnPagingNotEnabledException If paging is not enabled for the specified family.
   */
  public HBaseQualifierPager(
      EntityId entityId,
      KijiDataRequest dataRequest,
      HBaseKijiTable table,
      KijiColumnName family)
      throws KijiColumnPagingNotEnabledException {

    Preconditions.checkArgument(!family.isFullyQualified(),
        "Must use HBaseQualifierPager on a map-type family, but got '{}'.", family);
    mFamily = family;

    mDataRequest = dataRequest;
    mColumnRequest = mDataRequest.getColumn(family.getFamily(), null);
    if (!mColumnRequest.isPagingEnabled()) {
      throw new KijiColumnPagingNotEnabledException(
        String.format("Paging is not enabled for column [%s].", family));
    }

    mEntityId = entityId;
    mTable = table;
    mHasNext = true;  // there might be no page to read, but we don't know until we issue an RPC

    // Only retain the table if everything else ran fine:
    mTable.retain();
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    return mHasNext;
  }

  /** {@inheritDoc} */
  @Override
  public String[] next() {
    return next(mColumnRequest.getPageSize());
  }

  /**
   * Fetches another page of qualifiers.
   *
   * @param pageSize Maximum number of qualifiers to retrieve in the page.
   * @return the next page of qualifiers.
   */
  public String[] next(int pageSize) {
    if (!mHasNext) {
      throw new NoSuchElementException();
    }
    Preconditions.checkArgument(pageSize > 0, "Page size must be >= 1, got %s", pageSize);

    // Clone the column data request template, adjusting the filters to restrict the range
    // and the number of qualifiers to fetch.

    final KijiColumnFilter filter = Filters.and(
        new KijiColumnRangeFilter(mMinQualifier, false, null, false),  // qualifier > mMinQualifier
        mColumnRequest.getFilter(),  // user filter
        new KijiPaginationFilter(pageSize),  // Select at most 1 version per qualifier
        new StripValueColumnFilter());  // discard the cell content, we just need the qualifiers

    final KijiDataRequest nextPageDataRequest = KijiDataRequest.builder()
        .withTimeRange(mDataRequest.getMinTimestamp(), mDataRequest.getMaxTimestamp())
        .addColumns(ColumnsDef.create()
            .withFilter(filter)
            .withMaxVersions(1)  // HBase pagination filter forces max-versions to 1
            .add(mFamily))
        .build();

    LOG.debug("HBaseMapPager data request: {} and page size {}", nextPageDataRequest, pageSize);

    final KijiTableLayout layout = mTable.getLayout();
    final HBaseColumnNameTranslator translator = HBaseColumnNameTranslator.from(layout);
    final HBaseDataRequestAdapter adapter =
        new HBaseDataRequestAdapter(nextPageDataRequest, translator);
    try {
      final Get hbaseGet = adapter.toGet(mEntityId, layout);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sending HBase Get: {} with filter {}",
            hbaseGet, Debug.toDebugString(hbaseGet.getFilter()));
      }
      final Result result = doHBaseGet(hbaseGet);
      LOG.debug("Got {} cells over {} requested", result.size(), pageSize);

      final KeyValue[] kvs = result.raw();
      final String[] qualifiers = new String[kvs.length];
      for (int i = 0; i < kvs.length; ++i) {
        final HBaseColumnName hbaseColumn =
            new HBaseColumnName(kvs[i].getFamily(), kvs[i].getQualifier());
        final KijiColumnName kijiColumn = translator.toKijiColumnName(hbaseColumn);
        qualifiers[i] = kijiColumn.getQualifier();
      }

      // There is an HBase bug that leads to less KeyValue being returned than expected.
      // An empty result appears to be a reliable way to detect the end of the iteration.
      if (result.isEmpty()) {
        mHasNext = false;
      } else {
        // Update the low qualifier bound for the next iteration:
        mMinQualifier = qualifiers[qualifiers.length - 1];
      }

      return qualifiers;

    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }
  }

  /**
   * Sends an HBase Get request.
   *
   * @param get HBase Get request.
   * @return the HBase Result.
   * @throws IOException on I/O error.
   */
  private Result doHBaseGet(Get get) throws IOException {
    final HTableInterface htable = mTable.openHTableConnection();
    try {
      return htable.get(get);
    } finally {
      htable.close();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void remove() {
    throw new UnsupportedOperationException("KijiPager.remove() is not supported.");
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mTable.release();
  }
}
