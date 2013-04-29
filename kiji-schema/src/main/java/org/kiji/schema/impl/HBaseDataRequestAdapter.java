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
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.HBaseEntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.filter.KijiColumnFilter;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.hbase.HBaseScanOptions;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.ColumnNameTranslator;

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
   * @return An HBase Scan descriptor.
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
   * @return An HBase Scan descriptor.
   * @throws IOException If there is an error.
   */
  public Scan toScan(KijiTableLayout tableLayout, HBaseScanOptions scanOptions) throws IOException {
    final Scan scan = new Scan(toGet(HBaseEntityId.fromHBaseRowKey(new byte[0]), tableLayout));
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
    final Scan newScan = toScan(tableLayout);

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
   * @return An HBase Get descriptor.
   * @throws IOException If there is an error.
   */
  public Get toGet(EntityId entityId, KijiTableLayout tableLayout)
      throws IOException {

    final Get get = new Get(entityId.getHBaseRowKey());
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    ColumnNameTranslator columnTranslator = new ColumnNameTranslator(tableLayout);

    // There's a shortcoming in the HBase API that doesn't allow us to specify per-column
    // filters for timestamp ranges and max versions.  We need to generate a request that
    // will include all versions that we need, and add filters for the individual columns.

    // As of HBase 0.94, the ColumnPaginationFilter, which we had been using to permit per-column
    // maxVersions settings, no longer pages over multiple versions for the same column. We can
    // still use it, however, to limit fully-qualified columns with maxVersions = 1 to return only
    // the most recent version in the request's time range. All other columns will use the largest
    // maxVersions seen on any column request.

    // Fortunately, although we may retrieve more versions per column than we need from HBase, we
    // can still honor the user's requested maxVersions when returning the versions in
    // HBaseKijiRowData.
    int largestMaxVersions = 1;
    // If every column is paged, we should add a keyonly filter to a single column, so we can have
    // access to entityIds in our KijiRowData that is constructed.
    boolean completelyPaged = mKijiDataRequest.isPagingEnabled() ? true : false;
    for (KijiDataRequest.Column columnRequest : mKijiDataRequest.getColumns()) {
      if (!columnRequest.isPagingEnabled()) {
        completelyPaged = false;
        KijiColumnName kijiColumnName = columnRequest.getColumnName();
        HBaseColumnName hbaseColumnName = columnTranslator.toHBaseColumnName(kijiColumnName);
        if (!kijiColumnName.isFullyQualified()) {
          // The request is for all column in a Kiji family.
          get.addFamily(hbaseColumnName.getFamily());
        } else {
          // Calls to Get.addColumn() will invalidate any previous calls to Get.addFamily(),
          // so we only do it if:
          //   1. No data from the family has been added to the request yet, OR
          //   2. Only specific columns from the family have been requested so far.
          if (!get.getFamilyMap().containsKey(hbaseColumnName.getFamily())
              || null != get.getFamilyMap().get(hbaseColumnName.getFamily())) {
            get.addColumn(hbaseColumnName.getFamily(), hbaseColumnName.getQualifier());
          }
        }
        filterList.addFilter(toFilter(columnRequest, columnTranslator, tableLayout));
        largestMaxVersions = Math.max(largestMaxVersions, columnRequest.getMaxVersions());
      }
    }

    if (completelyPaged) {
      // If every column in our data request is paged, we should construct a get that reuqests
      // the least possible amount from each row.
      KijiDataRequest.Column sampleColumnRequest = mKijiDataRequest.getColumns().iterator().next();
      KijiColumnName kijiColumnName = sampleColumnRequest.getColumnName();
      HBaseColumnName hbaseColumnName = columnTranslator.toHBaseColumnName(kijiColumnName);
      if (!kijiColumnName.isFullyQualified()) {
        get.addFamily(hbaseColumnName.getFamily());
      } else {
        get.addColumn(hbaseColumnName.getFamily(), hbaseColumnName.getQualifier());
      }
      // If our data request has paging enabled on every column, we need to artificially construct a
      // Get, so that we have access to the entityId for each row. This will be used to construct
      // a KijiRowData.
      filterList.addFilter(new FirstKeyOnlyFilter());
      largestMaxVersions = sampleColumnRequest.getMaxVersions();
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
   * @return An HBase filter that retrieves only the data for the column request.
   * @throws IOException If there is an error.
   */
  private Filter toFilter(
      KijiDataRequest.Column columnRequest,
      ColumnNameTranslator columnNameTranslator,
      KijiTableLayout tableLayout) throws IOException {

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

    if (kijiColumnName.isFullyQualified() && !columnRequest.isPagingEnabled()
        && columnRequest.getMaxVersions() == 1) {
      // For fully qualified columns where maxVersions = 1, we can use the
      // ColumnPaginationFilter to restrict the number of versions returned to at most 1.
      // (Other columns' maxVersions will be filtered client-side in HBaseKijiRowData.)
      // Prior to HBase 0.94, we could use this optimization for all fully qualified
      // columns' maxVersions requests, due to different behavior in the
      // ColumnPaginationFilter.
      requestFilter.addFilter(new ColumnPaginationFilter(1, 0));
    }

    // Also add the user-specified column filter if requested.
    KijiColumnFilter userColumnFilter = columnRequest.getFilter();
    if (null != userColumnFilter) {

      Filter hBaseFilter = userColumnFilter.toHBaseFilter(kijiColumnName,
          new NameTranslatingFilterContext(columnNameTranslator));
      requestFilter.addFilter(hBaseFilter);
    }

    return requestFilter;
  }

  /**
   * A Context for KijiColumnFilters that translates column names to their HBase
   * representation.
   */
  private static final class NameTranslatingFilterContext extends KijiColumnFilter.Context {
    /** The translator to use. */
    private final ColumnNameTranslator mTranslator;

    /**
     * Initialize this context with the specified column name translator.
     *
     * @param translator the translator to use.
     */
    private NameTranslatingFilterContext(ColumnNameTranslator translator) {
      mTranslator = translator;
    }

    /** {@inheritDoc} */
    @Override
    public HBaseColumnName getHBaseColumnName(KijiColumnName kijiColumnName)
        throws NoSuchColumnException {
      return mTranslator.toHBaseColumnName(kijiColumnName);
    }
  }
}
