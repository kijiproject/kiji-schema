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

import java.io.IOException;
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
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiPager;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.util.DebugResourceTracker;

/**
 * Pages through the versions of a fully-qualified column.
 *
 * <p>
 *   Each page of versions is fetched by a Get RPC to the region server.
 *   The page size is translated into the Get's max-versions.
 *   The page offset is translated into the Get's max-timestamp.
 * </p>
 */
@ApiAudience.Private
public final class HBaseVersionPager implements KijiPager {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseVersionPager.class);

  /** Entity ID of the row being paged through. */
  private final EntityId mEntityId;

  /** Data request template for the column being paged through. */
  private final KijiDataRequest mDataRequest;

  /** Data request details for the fully-qualified column. */
  private final KijiDataRequest.Column mColumnRequest;

  /** HBase KijiTable to read from. */
  private final HBaseKijiTable mTable;

  /** Provider for cell decoders. */
  private final CellDecoderProvider mCellDecoderProvider;

  /** Name of the column being paged through. */
  private final KijiColumnName mColumnName;

  /** Default page size for this column. */
  private final int mDefaultPageSize;

  /** Total number of versions to return for the entire column. */
  private final int mTotalVersions;

  /** Number of versions returned so far. */
  private int mVersionsCount = 0;

  /** Max timestamp bound on the versions to fetch. */
  private long mPageMaxTimestamp;

  /** True only if there is another page of data to read through {@link #next()}. */
  private boolean mHasNext;


  /**
   * Initializes an HBaseVersionPager.
   *
   * <p>
   *   A fully-qualified column may contain a lot of cells (ie. a lot of versions).
   *   Fetching all these versions in a single Get is not always realistic.
   *   This version pager allows one to fetch subsets of the versions at a time.
   * </p>
   *
   * <p>
   *   To get a pager for a column with paging enabled,
   *   use {@link KijiRowData#getPager(String, String)}.
   * </p>
   *
   * @param entityId The entityId of the row.
   * @param dataRequest The requested data.
   * @param table The Kiji table that this row belongs to.
   * @param colName Name of the paged column.
   * @param cellDecoderProvider Provider for cell decoders.
   * @throws KijiColumnPagingNotEnabledException If paging is not enabled for the specified column.
   */
  protected HBaseVersionPager(
      EntityId entityId,
      KijiDataRequest dataRequest,
      HBaseKijiTable table,
      KijiColumnName colName,
      CellDecoderProvider cellDecoderProvider)
      throws KijiColumnPagingNotEnabledException {
    Preconditions.checkArgument(colName.isFullyQualified());

    Column columnRequest = dataRequest.getColumn(colName.getFamily(), colName.getQualifier());
    if (columnRequest == null) {
      // There is no data request for this fully-qualified column.
      // However, paging is allowed if this column belongs to a map-type family with paging enabled.
      columnRequest = dataRequest.getColumn(colName.getFamily(), null);
      Preconditions.checkArgument(columnRequest != null,
          "Couldn't create pager: "
          + "No data request for column {} from table {}.",
          colName, table.getURI());
      Preconditions.checkArgument(
          table.getLayout().getFamilyMap().get(colName.getFamily()).isMapType(),
          "Couldn't create pager: "
          + "Can only generate version pagers from a column family data request for map families. "
          + "Requested paging on qualifier {} from group family {} in table {}.",
          colName.getQualifier(), colName.getFamily(), table.getURI());
    }
    mColumnRequest = columnRequest;

    if (!mColumnRequest.isPagingEnabled()) {
      throw new KijiColumnPagingNotEnabledException(
        String.format("Paging is not enabled for column '%s' from table %s.",
            colName, table.getURI()));
    }

    // Construct a data request for only this column.
    final KijiDataRequestBuilder builder = KijiDataRequest.builder()
        .withTimeRange(dataRequest.getMinTimestamp(), dataRequest.getMaxTimestamp());
    builder.newColumnsDef(mColumnRequest);

    mColumnName = colName;
    mDataRequest = builder.build();
    mDefaultPageSize = mColumnRequest.getPageSize();
    mEntityId = entityId;
    mTable = table;
    mCellDecoderProvider = cellDecoderProvider;
    mHasNext = true;  // there might be no page to read, but we don't know until we issue an RPC

    mPageMaxTimestamp = mDataRequest.getMaxTimestamp();
    mTotalVersions = mColumnRequest.getMaxVersions();
    mVersionsCount = 0;

    // Only retain the table if everything else ran fine:
    mTable.retain();
    DebugResourceTracker.get().registerResource(this);
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    return mHasNext;
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowData next() {
    return next(mDefaultPageSize);
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowData next(int pageSize) {
    Preconditions.checkArgument(pageSize > 0, "Page size must be >= 1, got %s", pageSize);
    if (!mHasNext) {
      throw new NoSuchElementException();
    }

    final int maxVersions = Math.min(mTotalVersions - mVersionsCount, pageSize);

    // Clone the column data request template, but adjust the max-timestamp and the max-versions:
    final KijiDataRequest nextPageDataRequest = KijiDataRequest.builder()
        .withTimeRange(mDataRequest.getMinTimestamp(), mPageMaxTimestamp)
        .addColumns(ColumnsDef.create()
            .withFilter(mColumnRequest.getFilter())
            .withMaxVersions(maxVersions)
            .add(mColumnName))
        .build();

    final KijiTableLayout layout = mTable.getLayout();
    final HBaseDataRequestAdapter adapter =
        new HBaseDataRequestAdapter(nextPageDataRequest, HBaseColumnNameTranslator.from(layout));
    try {
      final Get hbaseGet = adapter.toGet(mEntityId, layout);
      LOG.debug("Sending HBase Get: {}", hbaseGet);
      final Result result = doHBaseGet(hbaseGet);
      LOG.debug("{} cells were requested, {} cells were received.", pageSize, result.size());

      if (result.size() < maxVersions) {
        // We got fewer versions than the number we expected, that means there are no more
        // versions to page through:
        mHasNext = false;
      } else {
        // track how far we have gone:
        final KeyValue last = result.raw()[result.raw().length - 1];
        mPageMaxTimestamp = last.getTimestamp();  // max-timestamp is exclusive
        mVersionsCount += result.raw().length;

        if ((mPageMaxTimestamp <= mDataRequest.getMinTimestamp())
            || (mVersionsCount >= mTotalVersions)) {
          mHasNext = false;
        }
      }

      return new HBaseKijiRowData(
          mTable, nextPageDataRequest, mEntityId, result, mCellDecoderProvider);
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
    DebugResourceTracker.get().unregisterResource(this);
    mTable.release();
  }
}
