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
import java.util.Map;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.filter.KijiColumnFilter;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.hbase.HBaseScanOptions;
import org.kiji.schema.layout.ColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;


/**
 * Wraps a KijiDataRequest to expose methods that generate meaningful objects in HBase
 * land, like {@link org.apache.hadoop.hbase.client.Put}s and {@link
 * org.apache.hadoop.hbase.client.Get}s.
 */
@ApiAudience.Private
public class HBaseDataRequestAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseDataRequestAdapter.class);

  /** The wrapped KijiDataRequest. */
  private final KijiDataRequest mKijiDataRequest;

  /**
   * Wraps a KijiDataRequest.
   *
   * @param kijiDataRequest The Kiji data request to wrap.
   */
  public HBaseDataRequestAdapter(KijiDataRequest kijiDataRequest) {
    mKijiDataRequest = kijiDataRequest;
  }

  /**
   * Constructs an HBase Scan that describes the data requested in the KijiDataRequest.
   *
   * @param tableLayout The layout of the Kiji table to read from.  This is required for
   *     determining the mapping between Kiji columns and HBase columns.
   * @return An HBase Scan descriptor, or null if no data was requested.
   * @throws IOException If there is an error.
   */
  public Scan toScan(KijiTableLayout tableLayout) throws IOException {
    return toScan(tableLayout, new HBaseScanOptions());
  }

  /**
   * Constructs an HBase Scan that describes the data requested in the KijiDataRequest.
   *
   * @param tableLayout The layout of the Kiji table to read from.  This is required for
   *     determining the mapping between Kiji columns and HBase columns.
   * @param scanOptions Custom options for this scan.
   * @return An HBase Scan descriptor, or null if no data was requested.
   * @throws IOException If there is an error.
   */
  public Scan toScan(KijiTableLayout tableLayout, HBaseScanOptions scanOptions) throws IOException {
    if (mKijiDataRequest.isEmpty()) {
      return null;
    }
    Scan scan = new Scan(toGet(new HBaseEntityId(new byte[0]), tableLayout));
    configureScan(scan, scanOptions);
    return scan;
  }

  /**
   * Like toScan(), but mutates a given Scan object to include everything in the data
   * request instead of returning a new one.
   *
   * <p>Any existing request settings in the Scan object will be preserved.</p>
   *
   * @param scan The existing scan object to apply the data request to.
   * @param tableLayout The layout of the Kiji table the scan will read from.
   * @throws IOException If there is an error.
   */
  public void applyToScan(Scan scan, KijiTableLayout tableLayout) throws IOException {
    Scan newScan = toScan(tableLayout);

    // Don't bother adding new columns if no columns are requested by the scan.
    if (newScan == null) {
      return;
    }

    // It's okay to put columns into the Scan that are already there.
    for (Map.Entry<byte[], NavigableSet<byte[]>> columnRequest
             : newScan.getFamilyMap().entrySet()) {
      byte[] family = columnRequest.getKey();
      if (null == columnRequest.getValue()) {
        // Request all columns in the family.
        scan.addFamily(family);
      } else {
        // Calls to Scan.addColumn() will invalidate any previous calls to Scan.addFamily(),
        // so we only do it if:
        //   1. No data from the family has been added to the request yet, OR
        //   2. Only specific columns from the family have been requested so far.
        if (!scan.getFamilyMap().containsKey(family)
            || null != scan.getFamilyMap().get(family)) {
          for (byte[] qualifier : columnRequest.getValue()) {
            scan.addColumn(family, qualifier);
          }
        }
      }
    }
  }

  /**
   * Constructs an HBase Get that describes the data requested in the KijiDataRequest for
   * a particular entity/row.
   *
   * @param entityId The row to build an HBase Get request for.
   * @param tableLayout The layout of the Kiji table to read from.  This is required for
   *     determining the mapping between Kiji columns and HBase columns.
   * @return An HBase Get descriptor, or null if no data was requested.
   * @throws IOException If there is an error.
   */
  public Get toGet(EntityId entityId, KijiTableLayout tableLayout) throws IOException {
    return toGet(entityId, tableLayout, 0);
  }

  /**
   * Constructs an HBase Get that describes the data requested in the KijiDataRequest for
   * a particular entity/row.
   *
   * @param entityId The row to build an HBase Get request for.
   * @param tableLayout The layout of the Kiji table to read from.  This is required for
   *     determining the mapping between Kiji columns and HBase columns.
   * @param pageIndex Which page of column data to retrieve (zero means the first page).
   * @return An HBase Get descriptor, or null if no data was requested.
   * @throws IOException If there is an error.
   */
  public Get toGet(EntityId entityId, KijiTableLayout tableLayout, int pageIndex)
      throws IOException {
    if (mKijiDataRequest.isEmpty()) {
      return null;
    }

    Get get = new Get(entityId.getHBaseRowKey());
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    ColumnNameTranslator columnTranslator = new ColumnNameTranslator(tableLayout);

    // There's a shortcoming in the HBase API that doesn't allow us to specify per-column
    // filters for timestamp ranges and max versions.  We need to generate a request that
    // will include all versions that we need, and add filters for the individual columns.
    int largestMaxVersions = 0;

    for (KijiDataRequest.Column columnRequest : mKijiDataRequest.getColumns()) {
      KijiColumnName kijiColumnName = columnRequest.getColumnName();
      HBaseColumnName hbaseColumnName = columnTranslator.toHBaseColumnName(kijiColumnName);
      if (!kijiColumnName.isFullyQualified()) {
        // The request is for all column in a Kiji family.
        get.addFamily(hbaseColumnName.getFamily());
      } else {
        // The request is for a particular Kiji column "family:qualifier".
        if (columnRequest.getPageSize() * pageIndex > columnRequest.getMaxVersions()) {
          LOG.debug("No more pages in column: " + kijiColumnName);
          continue;
        }

        // Calls to Get.addColumn() will invalidate any previous calls to Get.addFamily(),
        // so we only do it if:
        //   1. No data from the family has been added to the request yet, OR
        //   2. Only specific columns from the family have been requested so far.
        if (!get.getFamilyMap().containsKey(hbaseColumnName.getFamily())
            || null != get.getFamilyMap().get(hbaseColumnName.getFamily())) {
          get.addColumn(hbaseColumnName.getFamily(), hbaseColumnName.getQualifier());
        }
      }
      filterList.addFilter(toFilter(columnRequest, columnTranslator, tableLayout, pageIndex));
      largestMaxVersions = Math.max(largestMaxVersions, columnRequest.getMaxVersions());
    }

    if (!get.hasFamilies()) {
      // There was nothing added to the request. Return null to signal that no get() RPC
      // is necessary.
      return null;
    }

    get.setFilter(filterList);
    get.setTimeRange(mKijiDataRequest.getMinTimestamp(), mKijiDataRequest.getMaxTimestamp());
    get.setMaxVersions(largestMaxVersions);
    return get;
  }

  /**
   * Configures a Scan with the options specified on HBaseScanOptions.
   * Whenever an option is not specified on <code>scanOptions</code>,
   * the hbase default will be used instead.
   *
   * @param scan The Scan to configure.
   * @param scanOptions The options to configure this Scan with.
   */
  private void configureScan(Scan scan, HBaseScanOptions scanOptions) {
    if (null != scanOptions.getClientBufferSize()) {
      scan.setBatch(scanOptions.getClientBufferSize());
    }
    if (null != scanOptions.getServerPrefetchSize()) {
      scan.setCaching(scanOptions.getServerPrefetchSize());
    }
    if (null != scanOptions.getCacheBlocks()) {
      scan.setCacheBlocks(scanOptions.getCacheBlocks());
    }
  }

  /**
   * Constructs and returns the HBase filter that returns only the
   * data in a given Kiji column request.
   *
   * @param columnRequest A kiji column request.
   * @param columnNameTranslator A column name translator.
   * @param tableLayout A kiji table
   * @param pageIndex Specifies which page of cells to request (zero is the first page).
   * @return An HBase filter that retrieves only the data for the column request.
   * @throws IOException If there is an error.
   */
  private Filter toFilter(
      KijiDataRequest.Column columnRequest,
      ColumnNameTranslator columnNameTranslator,
      KijiTableLayout tableLayout,
      int pageIndex) throws IOException {

    KijiColumnName kijiColumnName = columnRequest.getColumnName();
    HBaseColumnName hbaseColumnName = columnNameTranslator.toHBaseColumnName(kijiColumnName);

    // Build up the filter we'll return from this method.
    FilterList requestFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);

    // Only read cells from this locality group.
    Filter localityGroupFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(hbaseColumnName.getFamily()));
    requestFilter.addFilter(localityGroupFilter);

    if (!kijiColumnName.isFullyQualified()) {
      // Allow all cells from this Kiji family.
      Filter mapPrefixFilter = new ColumnPrefixFilter(hbaseColumnName.getQualifier());
      requestFilter.addFilter(mapPrefixFilter);
    } else {
      // Allow cells only from this Kiji family:qualifier.
      Filter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
          new BinaryComparator(hbaseColumnName.getQualifier()));
      requestFilter.addFilter(qualifierFilter);
    }

    // Also add the user-specified column filter if requested.
    KijiColumnFilter userColumnFilter = columnRequest.getFilter();
    if (null != userColumnFilter) {

      Filter hBaseFilter = userColumnFilter.toHBaseFilter(kijiColumnName, columnNameTranslator);
      requestFilter.addFilter(hBaseFilter);
    }

    if (columnRequest.isPagingEnabled()) {
      // Finally, limit the cells to the current page of results.
      final int pageSize = columnRequest.getPageSize();
      final int limit = !kijiColumnName.isFullyQualified()
          ? pageSize  // Don't do any thing special for requests of a whole family.
          : Math.min(columnRequest.getMaxVersions() - pageIndex * pageSize, pageSize);
      final int offset = pageIndex * pageSize;
      requestFilter.addFilter(new ColumnPaginationFilter(limit, offset));
    } else if (kijiColumnName.isFullyQualified()) {
      // Limit the max versions. This optimization only works for requests for particular
      // columns within a family. We can't do this for "give me all the cells in a family"
      // requests because there's no HBase filter that does "give me N versions from each
      // qualifier." That's okay though; it will get filtered by HBaseKijiRowData.
      requestFilter.addFilter(new ColumnPaginationFilter(columnRequest.getMaxVersions(), 0));
    }

    return requestFilter;
  }

}
