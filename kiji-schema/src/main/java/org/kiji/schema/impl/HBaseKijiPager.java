/**
 * (c) Copyright 2012 WibiData, Inc.
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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiPager;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.filter.KijiPaginationFilter;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * <p>Implementation of a KijiPager for HBase.</p>
 */
@ApiAudience.Private
public final class HBaseKijiPager implements KijiPager {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseKijiPager.class);
  /** The entity id for the row. */
  private final EntityId mEntityId;
  /** The request used to retrieve this Kiji row data for this paged column. */
  private final KijiDataRequest mColumnDataRequest;
  /** The KijiTable we are reading from. */
  private final HBaseKijiTable mTable;
  /** The layout for the table this row data came from. */
  private final KijiTableLayout mLayout;
  /** This KijiColumnName of the column being paged through. */
  private final KijiColumnName mColumnName;
  /** The default page size for this column. */
  private final int mDefaultPageSize;
  /** The max number of versions to return from each column. */
  private final int mMaxVersions;
  /** The offset into cells to begin the page. */
  private int mOffset;
  /** Whether or not an exception will be thrown if .next() is called. */
  private boolean mHasNext;


  /**
   * Initializes a HBaseKijiPager. To get a pager for a column with paging enabled, use the
   * .getPager() method on a {@link KijiRowData}.
   *
   * @param entityId The entityId of the row.
   * @param dataRequest The requested data.
   * @param tableLayout Layout of the table the row belongs to.
   * @param table The Kiji table that this row belongs to.
   * @param colName Name of the paged column.
   * @throws KijiColumnPagingNotEnabledException If paging is not enabled for the specified column.
   */
  protected HBaseKijiPager(EntityId entityId, KijiDataRequest dataRequest,
    KijiTableLayout tableLayout, HBaseKijiTable table, KijiColumnName colName)
    throws KijiColumnPagingNotEnabledException {
    KijiDataRequest.Column columnRequest = dataRequest.getColumn(colName.getFamily(),
      colName.getQualifier());

    if (!columnRequest.isPagingEnabled()) {
      throw new KijiColumnPagingNotEnabledException(
        String.format("Paging is not enabled for column [%s].", colName));
    }

    // Construct a data request for only this column.
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.withTimeRange(dataRequest.getMinTimestamp(), dataRequest.getMaxTimestamp())
      .newColumnsDef(columnRequest);
    mColumnName = colName;
    mColumnDataRequest = builder.build();
    mDefaultPageSize = columnRequest.getPageSize();
    mEntityId = entityId;
    mTable = table; //This should also be closed/released once done.
    mLayout = tableLayout;
    mHasNext = true;
    mMaxVersions = columnRequest.getMaxVersions();
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
    // Initialize a data request builder from the paged column.
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
      builder.withTimeRange(mColumnDataRequest.getMinTimestamp(),
        mColumnDataRequest.getMaxTimestamp()).newColumnsDef()
      .withFilter(new KijiPaginationFilter(pageSize, mOffset, // Add a pagination filter.
        mColumnDataRequest.getColumn(mColumnName.getFamily(),
        mColumnName.getQualifier()).getFilter())) // Pass in user defined filters.
      .withMaxVersions(mMaxVersions).add(mColumnName);

    KijiDataRequest nextPageDataRequest = builder.build();
    LOG.debug("DataRequest in pager is: [{}]", nextPageDataRequest.toString());
    HBaseDataRequestAdapter adapter = new HBaseDataRequestAdapter(nextPageDataRequest);
    try {
      Get hbaseGet = adapter.toGet(mEntityId, mLayout);
      incrementOffset(pageSize);
      Result nextResultPage = mTable.getHTable().get(hbaseGet);
      // If we retrieved less results than expected, we are out of pages.
      if (nextResultPage.size() < pageSize) {
        mHasNext = false;
      }
      return new HBaseKijiRowData(mEntityId, nextPageDataRequest, mTable, nextResultPage);
    } catch (IOException ioe) {
      LOG.error("Unable to get next page of results: {}", ioe);
      return new HBaseKijiRowData(mEntityId, nextPageDataRequest, mTable, new Result());
    }

  }

   /** {@inheritDoc} */
  @Override
  public void remove() {
    throw new UnsupportedOperationException("KijiPagers do not support remove().");
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mTable.close();
  }

  /**
   * Increments the current offset for a page.
   *
   * @param pageSize The page size to increment the offset by.
   */
  private synchronized void incrementOffset(final int pageSize) {
    mOffset += pageSize;
  }
}
