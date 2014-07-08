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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiColumnPagingNotEnabledException;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiPager;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.layout.CassandraColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.util.DebugResourceTracker;

/**
 * Pages through the versions of a fully-qualified column.
 *
 */
@ApiAudience.Private
public final class CassandraVersionPager implements KijiPager {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraVersionPager.class);

  /** Entity ID of the row being paged through. */
  private final EntityId mEntityId;

  /** Data request for the column in question. */
  private final KijiDataRequest mDataRequest;

  /** HBase KijiTable to read from. */
  private final CassandraKijiTable mTable;

  /** Provider for cell decoders. */
  private final CellDecoderProvider mCellDecoderProvider;

  /** Default page size for this column. */
  private final int mDefaultPageSize;

  /** True only if there is another page of data to read through {@link #next()}. */
  private boolean mHasNext;

  /** Iterator over all of the rows (across multiple pages) for this column. */
  private Iterator<Row> mRowIterator;

  /**
   * Initializes a CassandraVersionPager.
   *
   * <p>
   *   A fully-qualified column may contain a lot of cells (ie. a lot of versions).
   *   Fetching all these versions at once is not always realistic.
   *   This version pager allows one to fetch subsets of the versions at a time.
   * </p>
   *
   * <p>
   *   To get a pager for a column with paging enabled,
   *   use {@link org.kiji.schema.KijiRowData#getPager(String, String)}.
   * </p>
   *
   * @param entityId The entityId of the row.
   * @param dataRequest The requested data.
   * @param table The Kiji table that this row belongs to.
   * @param colName Name of the paged column.
   * @param cellDecoderProvider Provider for cell decoders.
   * @throws org.kiji.schema.KijiColumnPagingNotEnabledException If paging is not enabled for the
   *     specified column.
   */
  protected CassandraVersionPager(
      EntityId entityId,
      KijiDataRequest dataRequest,
      CassandraKijiTable table,
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

    if (!columnRequest.isPagingEnabled()) {
      throw new KijiColumnPagingNotEnabledException(
        String.format("Paging is not enabled for column '%s' from table %s.",
            colName, table.getURI()));
    }

    // Construct a data request for only this column.
    mDataRequest = KijiDataRequest.builder()
        .withTimeRange(dataRequest.getMinTimestamp(), dataRequest.getMaxTimestamp())
        .addColumns(ColumnsDef.create()
            .withFilter(columnRequest.getFilter())
            .withMaxVersions(columnRequest.getMaxVersions())
            .withPageSize(columnRequest.getPageSize())
            .add(colName))
        .build();

    mDefaultPageSize = columnRequest.getPageSize();
    mEntityId = entityId;
    mTable = table;
    mCellDecoderProvider = cellDecoderProvider;
    mHasNext = true;  // there might be no page to read, but we don't know until we issue an RPC

    // Only retain the table if everything else ran fine:
    mTable.retain();

    // Populate the row iterator with a Cassandra SELECT query.
    final KijiTableLayout layout = mTable.getLayout();
    final CassandraColumnNameTranslator translator = CassandraColumnNameTranslator.from(layout);
    CassandraDataRequestAdapter adapter =
        new CassandraDataRequestAdapter(mDataRequest, translator);

    // Should be only a single ResultSet here, because this was a data request for a single column.
    try {
      List<ResultSet> results = adapter.doPagedGet(mTable, entityId);
      assert(results.size() == 1);
      mRowIterator = results.get(0).iterator();

      // TODO: If timestamp for first item in iterator is earlier than min timestamp, mHasNext=false

    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }
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

    // Iterate through a number of cells equal to the page size to create a new KijiRowData.
    HashSet<Row> rowsThisPage = new HashSet<Row>();

    // TODO: Also check that we haven't returned more data than the user-requested max versions.
    for (int rowNum = 0; rowNum < pageSize; rowNum++) {
      // Check whether there is enough data left here to complete the page.
      if (!mRowIterator.hasNext()) {
        break;
      }
      rowsThisPage.add(mRowIterator.next());
    }

    assert(mHasNext);
    if (!mRowIterator.hasNext()) {
      mHasNext = false;
    }

    try {
      return new CassandraKijiRowData(
          mTable, mDataRequest, mEntityId, rowsThisPage, mCellDecoderProvider);
    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
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
