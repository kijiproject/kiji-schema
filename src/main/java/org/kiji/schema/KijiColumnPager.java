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

package org.kiji.schema;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.impl.HBaseDataRequestAdapter;
import org.kiji.schema.layout.ColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;


/**
 * Fetches new pages of column data in Kiji, and maintains state for which page each column is on.
 */
@ApiAudience.Private
public final class KijiColumnPager {
  private static final Logger LOG = LoggerFactory.getLogger(KijiColumnPager.class);

  /** The entity id for the row we are reading from. */
  private final EntityId mEntityId;

  /** The data request used to fetch the row. */
  private final KijiDataRequest mDataRequest;

  /** The layout of the table being read from. */
  private final KijiTableLayout mTableLayout;

  /** The HTable being read from. */
  private final HTableInterface mHTable;

  /** A map from column name to the current page index. */
  private final Map<KijiColumnName, Integer> mPageIndices;

  /**
   * Creates a new KijiColumnPager.
   *
   * @param entityId The entity id of the row having columns paged.
   * @param dataRequest The data request that was used to fetch the row data.
   * @param tableLayout The layout of the Kiji table being read from.
   * @param htable An HTable connection for the table that stores the Kiji table data.
   */
  public KijiColumnPager(EntityId entityId, KijiDataRequest dataRequest,
      KijiTableLayout tableLayout, HTableInterface htable) {
    assert null != entityId;
    assert null != dataRequest;
    assert null != tableLayout;
    assert null != htable;

    mEntityId = entityId;
    mDataRequest = dataRequest;
    mTableLayout = tableLayout;
    mHTable = htable;
    mPageIndices = Collections.synchronizedMap(new HashMap<KijiColumnName, Integer>());
  }

  /**
   * Gets the next page of data in a column with paging enabled.
   *
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @return The next page of data (or null if there is no more data).
   * @throws KijiColumnPagingNotEnabledException If paging is not enabled on the requested column.
   * @throws IOException If there is an I/O error.
   */
  public NavigableMap<Long, byte[]> getNextPage(String family, String qualifier)
      throws IOException {
    final KijiColumnName kijiColumnName = new KijiColumnName(family, qualifier);

    // Send the request for the next page of data.
    Result nextPageResult = getNextPage(kijiColumnName);
    if (null == nextPageResult || nextPageResult.isEmpty()) {
      LOG.debug("No more pages of data in column " + kijiColumnName);
      return null;
    }

    // Figure out the HBase column name.
    final ColumnNameTranslator translator = new ColumnNameTranslator(mTableLayout);
    HBaseColumnName hbaseColumnName = translator.toHBaseColumnName(kijiColumnName);

    final NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap =
        nextPageResult.getMap().get(hbaseColumnName.getFamily());
    if (null == familyMap) {
      // No more results in the family.
      LOG.debug("No more pages of data in column " + kijiColumnName);
      return null;
    }
    final NavigableMap<Long, byte[]> hbaseCells = familyMap.get(hbaseColumnName.getQualifier());
    if (null == hbaseCells) {
      // No more results in the column.
      LOG.debug("No more pages of data in column " + kijiColumnName);
      return null;
    }
    LOG.debug("Next page of data retrieved for column " + kijiColumnName);

    return hbaseCells;
  }

  /**
   * Gets the next page of data in a family with paging enabled.
   *
   * @param family A column family.
   * @return The next page of data (or null if there is no more data).
   * @throws KijiColumnPagingNotEnabledException If paging is not enabled on the requested column.
   * @throws IOException If there is an I/O error.
   */
  public NavigableMap<String, NavigableMap<Long, byte[]>> getNextPage(String family)
      throws IOException {
    final KijiColumnName kijiFamily = new KijiColumnName(family);
    if (kijiFamily.isFullyQualified()) {
      throw new IllegalArgumentException("Family name (" + family + ") had a colon ':' in it");
    }

    // Send the request for the next page of data.
    Result nextPageResult = getNextPage(kijiFamily);
    if (null == nextPageResult || nextPageResult.isEmpty()) {
      LOG.debug("No more pages of data in column " + family);
      return null;
    }

    // Figure out the HBase column name.
    final ColumnNameTranslator translator = new ColumnNameTranslator(mTableLayout);
    HBaseColumnName hbaseColumnName = translator.toHBaseColumnName(kijiFamily);

    final NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap =
        nextPageResult.getMap().get(hbaseColumnName.getFamily());
    if (null == familyMap) {
      // No more results in the family.
      LOG.debug("No more pages of data in column " + kijiFamily);
      return null;
    }

    // Construct the map of qualifiers to cells to return.
    NavigableMap<String, NavigableMap<Long, byte[]>> returnValue =
        new TreeMap<String, NavigableMap<Long, byte[]>>();
    for (Map.Entry<byte[], NavigableMap<Long, byte[]>> hbaseEntry : familyMap.entrySet()) {
      KijiColumnName kijiColumnName = translator.toKijiColumnName(
          new HBaseColumnName(hbaseColumnName.getFamily(), hbaseEntry.getKey()));
      returnValue.put(kijiColumnName.getQualifier(), hbaseEntry.getValue());
    }
    return returnValue;
  }

  /**
   * Gets the next page of results for a column or family.
   *
   * @param kijiColumnName The column to get the next page of data for.
   * @return The HBase result containing the next page of data, or null if there is no more data.
   * @throws KijiColumnPagingNotEnabledException If paging is not enabled on the requested column.
   * @throws IOException If there is an I/O error.
   */
  private Result getNextPage(KijiColumnName kijiColumnName) throws IOException {
    // Figure out the page size for the column.
    KijiDataRequest.Column requestedColumn =
        mDataRequest.getColumn(kijiColumnName.getFamily(), kijiColumnName.getQualifier());
    if (null == requestedColumn) {
      throw new NoSuchColumnException("No such column: " + kijiColumnName);
    }
    if (!requestedColumn.isPagingEnabled()) {
      throw new KijiColumnPagingNotEnabledException(
          "Paging was not enabled on column " + kijiColumnName);
    }

    // Construct a KijiDataRequest for just the single column.
    KijiDataRequest nextPageRequest = new KijiDataRequest()
        .addColumn(requestedColumn)
        .withTimeRange(mDataRequest.getMinTimestamp(), mDataRequest.getMaxTimestamp());

    // Turn it into an HBase Get.
    HBaseDataRequestAdapter hbaseDataRequestAdapter = new HBaseDataRequestAdapter(nextPageRequest);
    int nextPageIndex = incrementPageIndex(kijiColumnName);
    Get nextPageGet;
    try {
      nextPageGet = hbaseDataRequestAdapter.toGet(mEntityId, mTableLayout, nextPageIndex);
    } catch (KijiDataRequestException e) {
      throw new InternalKijiError(
          "An invalid KijiDataRequest was constructed for fetching a new page: "
          + nextPageRequest.toString());
    }

    if (null == nextPageGet) {
      LOG.debug("No more pages of data in column " + kijiColumnName);
      return null;
    }

    // Send the request for the next page of data.
    return mHTable.get(nextPageGet);
  }

  /**
   * Increments the state of the current page index for a column.
   *
   * @param kijiColumnName A kiji column name.
   * @return The incremented page index.
   */
  private synchronized int incrementPageIndex(KijiColumnName kijiColumnName) {
    if (mPageIndices.containsKey(kijiColumnName)) {
      // Increment the page index.
      mPageIndices.put(kijiColumnName, mPageIndices.get(kijiColumnName) + 1);
    } else {
      // Assume we were on page zero before, now we're on page index 1.
      mPageIndices.put(kijiColumnName, 1);
    }
    return mPageIndices.get(kijiColumnName);
  }
}
