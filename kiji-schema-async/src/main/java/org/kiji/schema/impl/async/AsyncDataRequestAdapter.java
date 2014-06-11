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

package org.kiji.schema.impl.async;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator;
import org.hbase.async.BinaryComparator;
import org.hbase.async.Bytes;
import org.hbase.async.ColumnPaginationFilter;
import org.hbase.async.ColumnPrefixFilter;
import org.hbase.async.CompareFilter.CompareOp;
import org.hbase.async.FilterList.Operator;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.QualifierFilter;
import org.hbase.async.ScanFilter;
import org.hbase.async.Scanner;
import org.hbase.async.FilterList;
import org.hbase.async.FamilyFilter;
import org.hbase.async.FirstKeyOnlyFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.filter.KijiColumnFilter;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.hbase.HBaseScanOptions;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;

/**
 * Wraps a KijiDataRequest to expose methods that generate meaningful objects in HBase
 * land, like {@link org.apache.hadoop.hbase.client.Put}s and {@link
 * org.apache.hadoop.hbase.client.Get}s.
 */
@ApiAudience.Private
public final class AsyncDataRequestAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncDataRequestAdapter.class);

  /** The wrapped KijiDataRequest. */
  private final KijiDataRequest mKijiDataRequest;
  /** The translator for generating HBase column names. */
  private final HBaseColumnNameTranslator mColumnNameTranslator;
  /** Shared HBaseClient connection. */
  private final HBaseClient mHBClient;
  /** HBase table name */
  private final byte[] mTableName;

  /**
   * Creates a new AsyncDataRequestAdapter for a given data request using a given
   * KijiColumnNameTranslator.
   *
   * @param kijiDataRequest the data request to adapt for HBase.
   * @param translator the name translator for getting HBase column names.
   */
  public AsyncDataRequestAdapter(
      KijiDataRequest kijiDataRequest,
      HBaseColumnNameTranslator translator,
      HBaseClient hbClient,
      byte[] tableName
  ) {
    mKijiDataRequest = kijiDataRequest;
    mColumnNameTranslator = translator;
    mHBClient = hbClient;
    mTableName = tableName;
  }

  /**
   * Constructs an HBase Scan that describes the data requested in the KijiDataRequest.
   *
   * @param tableLayout The layout of the Kiji table to read from.  This is required for
   *     determining the mapping between Kiji columns and HBase columns.
   * @return An HBase Scan descriptor.
   * @throws IOException If there is an error.
   */
  public Scanner toScanner(KijiTableLayout tableLayout) throws IOException {
    return toScanner(tableLayout, new HBaseScanOptions());
  }

  /**
   * Constructs an AsyncHBase Scanner that describes the data requested in the KijiDataRequest.
   *
   * @param tableLayout The layout of the Kiji table to read from.  This is required for
   *     determining the mapping between Kiji columns and HBase columns.
   * @param scanOptions Custom options for this scan.
   * @return An HBase Scan descriptor.
   * @throws IOException If there is an error.
   */
  public Scanner toScanner(KijiTableLayout tableLayout, HBaseScanOptions scanOptions) throws IOException {
    // Unfortunately in HBase 95+, we can no longer create empty gets.
    // So create a fake one for this table and fill in the fields of a new scan.
    final Scanner scanner = mHBClient.newScanner(mTableName);
    setupFilters(scanner, tableLayout);
    configureScanner(scanner, scanOptions);
    return scanner;
  }

  /**
   * Like toScanner(), but mutates a given Scanner object to include everything in the data
   * request instead of returning a new one.
   *
   * <p>Any existing request settings in the Scan object will be preserved.</p>
   *
   * @param scanner The existing scanner object to apply the data request to.
   * @param tableLayout The layout of the Kiji table the scan will read from.
   * @throws IOException If there is an error.
   */
  public void applyToScan(Scanner scanner, KijiTableLayout tableLayout) throws IOException {
    // Currently AsyncHBase's Scanner does not have any 'add' methods, nor does it have 'get'
    // methods for families, columns, etc. If either of those is implemented in the future
    // this method can become supported
    throw new UnsupportedOperationException("applyToScan is not yet supported with asyncHBase");
    /*
    final Scanner newScanner = toScanner(tableLayout);

    // It's okay to put columns into the Scan that are already there.
    for (Map.Entry<byte[], NavigableSet<byte[]>> columnRequest
             : newScanner.getFamilyMap().entrySet()) {
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
    } */
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
  public GetRequest toGetRequest(EntityId entityId, KijiTableLayout tableLayout)
      throws IOException {
    // TODO: AsyncHBase's GetRequest does not yet support filters. Once they do,
    // implement toGetRequest
    throw new UnsupportedOperationException("toGetRequest is not yet supported for Async Kiji");
  }

  /**
   * Sets up the scanner to describes the data requested in the KijiDataRequest for
   * a particular entity/row.
   *
   * @param scanner The Scanner to set up.
   * @param tableLayout The layout of the Kiji table to read from.  This is required for
   *     determining the mapping between Kiji columns and HBase columns.
   * @throws IOException If there is an error.
   */
  private void setupFilters(Scanner scanner, KijiTableLayout tableLayout)
      throws IOException {

    // Context to translate user Kiji filters into HBase filters:
    final KijiColumnFilter.Context filterContext =
        new NameTranslatingFilterContext(mColumnNameTranslator);

    // Get request we are building and returning:
    /**final Get get = new Get(entityId.getHBaseRowKey());*/
    final Map<byte [], NavigableSet<byte []>> familyMap =
        new TreeMap<byte [], NavigableSet<byte []>>(new ByteArrayComparator());
    final List<ScanFilter> scanFilters = Lists.newArrayList();

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

    // Largest of the max-versions from all the requested columns.
    // Columns with paging are excluded (max-versions does not make sense when paging):
    int largestMaxVersions = 1;

    // If every column is paged, we should add a keyonly filter to a single column, so we can have
    // access to entityIds in our KijiRowData that is constructed.
    boolean completelyPaged = mKijiDataRequest.isPagingEnabled() ? true : false;

    for (KijiDataRequest.Column columnRequest : mKijiDataRequest.getColumns()) {
      final KijiColumnName kijiColumnName = columnRequest.getColumnName();
      final HBaseColumnName hbaseColumnName =
          mColumnNameTranslator.toHBaseColumnName(kijiColumnName);

      if (!columnRequest.isPagingEnabled()) {
        completelyPaged = false;

        // Do not include max-versions from columns with paging enabled:
        largestMaxVersions = Math.max(largestMaxVersions, columnRequest.getMaxVersions());
      }

      if (kijiColumnName.isFullyQualified()) {
        // Requests a fully-qualified column.
        // Adds this column to the Get request, and also as a filter.
        //
        // Filters are required here because we might end up requesting all cells from the
        // HBase family (ie. from the Kiji locality group), if a map-type family from that
        // locality group is also requested.

        addColumnIfNecessary(familyMap, hbaseColumnName);
        scanFilters.add(toFilter(columnRequest, hbaseColumnName, filterContext, scanner));

      } else {
        final FamilyLayout fLayout = tableLayout.getFamilyMap().get(kijiColumnName.getFamily());
        if (fLayout.isGroupType()) {
          // Requests all columns in a Kiji group-type family.
          // Expand the family request into individual column requests:
          for (String qualifier : fLayout.getColumnMap().keySet()) {
            final KijiColumnName fqKijiColumnName =
                KijiColumnName.create(kijiColumnName.getFamily(), qualifier);
            final HBaseColumnName fqHBaseColumnName =
                mColumnNameTranslator.toHBaseColumnName(fqKijiColumnName);
            addColumnIfNecessary(familyMap, fqHBaseColumnName);
            scanFilters.add(toFilter(columnRequest, fqHBaseColumnName, filterContext, scanner));
          }

        } else if (fLayout.isMapType()) {
          // Requests all columns in a Kiji map-type family.
          // We need to request all columns in the HBase family (ie. in the Kiji locality group)
          // and add a column prefix-filter to select only the columns from that Kiji family:
          familyMap.remove(hbaseColumnName.getFamily());
          familyMap.put(hbaseColumnName.getFamily(), null);
          scanFilters.add(toFilter(columnRequest, hbaseColumnName, filterContext, scanner));

        } else {
          throw new InternalKijiError("Family is neither group-type nor map-type");
        }
      }
    }

    final FilterList filterList;
    if (completelyPaged) {
      // All requested columns have paging enabled.
      Preconditions.checkState(largestMaxVersions == 1);

      // We just need to know whether a row has data in at least one of the requested columns.
      // Stop at the first valid key using AND(columnFilters, FirstKeyOnlyFilter):
      scanFilters.add(new FirstKeyOnlyFilter());
      filterList = new FilterList(scanFilters, Operator.MUST_PASS_ONE); //TODO: Also add FirstKeyOnlyFilter?
      scanner.setFilter(filterList);
      /**get.setFilter(new FilterList(
          FilterList.Operator.MUST_PASS_ALL, columnFilters, new FirstKeyOnlyFilter()));*/
    } else if (!scanFilters.isEmpty()) {
      filterList = new FilterList(scanFilters, Operator.MUST_PASS_ONE); //TODO:is this right? double chekc
      scanner.setFilter(filterList);
    }

    // Filters for each requested column: OR(<filter-for-column-1>, <filter-for-column2>, ...)
    setFamiliesForScanner(scanner, familyMap);
    scanner.setTimeRange(mKijiDataRequest.getMinTimestamp(), mKijiDataRequest.getMaxTimestamp());
    scanner.setMaxVersions(largestMaxVersions);
  }


  public static String familiesToString(byte[][] families, byte[][][] qualifiers) {
    String s = "";
    for (int i = 0; i < families.length; i++) {
      s += Bytes.pretty(families[i]);
      if (qualifiers[i] != null) {
        for (int j = 0; j < qualifiers[i].length; j++) {
          s += Bytes.pretty(qualifiers[i][j]);
        }
      }
    }
    return s;
  }

  public static String convertMaptoByteArrays(
      Map<byte [], NavigableSet<byte []>> familyMap
  ) {
    byte[][] families = new byte[familyMap.keySet().size()][];
    ArrayList<byte[]> familiesWithColumns = Lists.newArrayList();
    ArrayList<byte[]> familiesWithoutColumns = Lists.newArrayList();
    int x = 0;
    // First split the families into those with specified columns and those without
    for (byte[] key : familyMap.keySet()) {
      NavigableSet value = familyMap.get(key);
      if (value == null) {
        familiesWithoutColumns.add(key);
      }
      else {
        familiesWithColumns.add(key);
        families[x] = key;
        x++;
      }
    }

    // Now we can create associated qualifiers only for families that specify columns
    byte[][][] qualifiers = new byte[familyMap.keySet().size()][][];
    for(int i = 0; i < familiesWithColumns.size(); i++) {
      NavigableSet<byte []> value = familyMap.get(familiesWithColumns.get(i));
      int j = 0;
      qualifiers[i] = new byte [value.size()][];
      for (byte[] col : value) {
        qualifiers[i][j] = col;
        j++;
      }
    }

    // Finally fill in the rest of the families with no columns. This will allow Scanner to know
    // get the entire family for these values.
    for (int i = 0; i < familiesWithoutColumns.size(); i++) {
      families[i+familiesWithColumns.size()] = familiesWithoutColumns.get(i);
    }

    return AsyncDataRequestAdapter.familiesToString(families, qualifiers);

  }
  /**
   * Converts the Map of families and columns to a 2d byte array for families,
   * with families with specified columns first and families with no specified
   * columns last, and a 3d byte array for qualifiers in order of the families
   * with specified columns. These arrays are then passed to scanner.setFamilies().
   *
   * @param scanner
   * @param familyMap
   */
  public static void setFamiliesForScanner(
      Scanner scanner,
      Map<byte [], NavigableSet<byte []>> familyMap
  ) {

    byte[][] families = new byte[familyMap.keySet().size()][];
    ArrayList<byte[]> familiesWithColumns = Lists.newArrayList();
    ArrayList<byte[]> familiesWithoutColumns = Lists.newArrayList();
    int x = 0;
    // First split the families into those with specified columns and those without
    for (byte[] key : familyMap.keySet()) {
      NavigableSet value = familyMap.get(key);
      if (value == null) {
        familiesWithoutColumns.add(key);
      }
      else {
        familiesWithColumns.add(key);
        families[x] = key;
        x++;
      }
    }

    // Now we can create associated qualifiers only for families that specify columns
    byte[][][] qualifiers = new byte[familyMap.keySet().size()][][];
    for(int i = 0; i < familiesWithColumns.size(); i++) {
      NavigableSet<byte []> value = familyMap.get(familiesWithColumns.get(i));
      int j = 0;
      qualifiers[i] = new byte [value.size()][];
      for (byte[] col : value) {
        qualifiers[i][j] = col;
        j++;
      }
    }

    // Finally fill in the rest of the families with no columns. This will allow Scanner to know
    // get the entire family for these values.
    for (int i = 0; i < familiesWithoutColumns.size(); i++) {
      families[i+familiesWithColumns.size()] = familiesWithoutColumns.get(i);
    }


    scanner.setFamilies(families, qualifiers);
  }

  /**
   * Adds the column from the specific family with the specified qualifier.
   * Overrides previous calls to addFamily for this family.
   *
   * @param familyMap Adds the column to this Map.
   * @param family The family to add to the familyMap.
   * @param qualifier The qualifier to add to the familyMap.
   */
  private static void addColumn(
      Map<byte [], NavigableSet<byte []>> familyMap,
      byte [] family,
      byte [] qualifier) {
    NavigableSet<byte []> set = familyMap.get(family);
    if(set == null) {
      set = new TreeSet<byte []>(new ByteArrayComparator());
    }
    set.add(qualifier);
    familyMap.put(family, set);
  }

  /**
   * Adds a fully-qualified column to a Family Map, if necessary.
   *
   * <p>
   *   If the entire HBase family is already requested, the column does not need to be added.
   * </p>
   *
   * @param familyMap Adds the column to this familyMap.
   * @param column Fully-qualified HBase column to add to the familyMap.
   */
  private static void addColumnIfNecessary(
      Map<byte [], NavigableSet<byte []>> familyMap,
      HBaseColumnName column) {
    // Calls to Get.addColumn() invalidate previous calls to Get.addFamily(),
    // so we only do it if:
    //   1. No data from the family has been added to the request yet,
    // OR
    //   2. Only specific columns from the family have been requested so far.
    // Note: the Get family-map uses null values to indicate requests for an entire HBase family.
    if (!familyMap.keySet().contains(column.getFamily())
        || (familyMap.get(column.getFamily()) != null)) {
      addColumn(familyMap, column.getFamily(), column.getQualifier());
    }
  }


  /**
   * Configures a Scan with the options specified on HBaseScanOptions.
   * Whenever an option is not specified on <code>scanOptions</code>,
   * the hbase default will be used instead.
   *
   * @param scanner The Scan to configure.
   * @param scanOptions The options to configure this Scan with.
   */
  private void configureScanner(Scanner scanner, HBaseScanOptions scanOptions) {
    // TODO(gabe): Fix these
    if (null != scanOptions.getClientBufferSize()) {
      scanner.setMaxNumKeyValues(scanOptions.getClientBufferSize());
    }
    // TODO(gabe): Ask Asynchbase list about this?
    if (null != scanOptions.getServerPrefetchSize()) {
      scanner.setMaxNumRows(scanOptions.getServerPrefetchSize());
    }
    if (null != scanOptions.getCacheBlocks()) {
      //scan.setCacheBlocks(scanOptions.getCacheBlocks());
      scanner.setServerBlockCache(scanOptions.getCacheBlocks());
    }
  }

  /**
   * Constructs and returns the AsyncHBase scan filter that returns only the
   * data in a given Kiji column request.
   *
   * @param columnRequest A kiji column request.
   * @param hbaseColumnName HBase column name.
   * @param filterContext Context to translate Kiji column filters to AsyncHBase scan filters.
   * @param scanner AsyncHBase Scanner that is having the filters applied.
   * @return An AsyncHBase scan filter that retrieves only the data for the column request.
   * @throws IOException If there is an error.
   */
  private static FilterList toFilter(
      KijiDataRequest.Column columnRequest,
      HBaseColumnName hbaseColumnName,
      KijiColumnFilter.Context filterContext,
      Scanner scanner)
      throws IOException {

    final KijiColumnName kijiColumnName = columnRequest.getColumnName();

    // Builds an HBase filter for the specified column:
    //     (HBase-family = Kiji-locality-group)
    // AND (HBase-qualifier = Kiji-family:qualifier / prefixed by Kiji-family:)
    // AND (ColumnPaginationFilter(limit=1))  // when paging or if max-versions is 1
    // AND (custom user filter)
    // AND (FirstKeyOnlyFilter)  // only when paging
    //
    // Note:
    //     We cannot use KeyOnlyFilter as this filter uses Filter.transform() which applies
    //     unconditionally on all the KeyValue in the HBase Result.
    final List<ScanFilter> scanFilters = Lists.newArrayList();
    // Only let cells from the locality-group (ie. HBase family) the column belongs to, ie:
    //     HBase-family = Kiji-locality-group
    scanFilters.add(
        new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(hbaseColumnName.getFamily())));

    if (kijiColumnName.isFullyQualified()) {
      // Only let cells from the fully-qualified column ie.:
      //     HBase-qualifier = Kiji-family:qualifier
      scanFilters.add(new QualifierFilter(
              CompareOp.EQUAL,
              new BinaryComparator(hbaseColumnName.getQualifier())));
    } else {
      // Only let cells from the map-type family ie.:
      //     HBase-qualifier starts with "Kiji-family:"
      scanFilters.add(new ColumnPrefixFilter(hbaseColumnName.getQualifier()));
    }

    if (columnRequest.isPagingEnabled()
        || (kijiColumnName.isFullyQualified() && (columnRequest.getMaxVersions() == 1))) {
      // For fully qualified columns where maxVersions = 1, we can use the
      // ColumnPaginationFilter to restrict the number of versions returned to at most 1.
      //
      // Other columns' maxVersions will be filtered client-side in HBaseKijiRowData.
      //
      // Prior to HBase 0.94, we could use this optimization for all fully qualified
      // columns' maxVersions requests, due to different behavior in the
      // ColumnPaginationFilter.
      //
      // Note: we could also use this for a map-type family if max-versions == 1,
      //     by setting limit = Integer.MAX_VALUE.
      final int limit = 1;
      final int offset = 0;
      scanFilters.add(new ColumnPaginationFilter(limit,offset));
    }

    // Add the optional user-specified column filter, if specified:
    // TODO: Implement toHBaseFilter
    /*
    if (columnRequest.getFilter() != null) {
      filter.addFilter(
          columnRequest.getFilter().toHBaseFilter(kijiColumnName, filterContext));
    } */

    // If column has paging enabled, we just want to know about the existence of a cell:
    if (columnRequest.isPagingEnabled()) {
      scanFilters.add(new FirstKeyOnlyFilter());

      // TODO(SCHEMA-334) KeyOnlyFilter uses Filter.transform() which applies unconditionally.
      //     There is a chance that Filter.transform() may apply conditionally in the future,
      //     in which case we may re-introduce the KeyOnlyFilter.
      //     An alternative is to provide a custom HBase filter to handle Kiji data requests
      //     efficiently.
    }

    return new FilterList(scanFilters, Operator.MUST_PASS_ALL);
  }

  /**
   * A Context for KijiColumnFilters that translates column names to their HBase
   * representation.
   */
  private static final class NameTranslatingFilterContext extends KijiColumnFilter.Context {
    /** The translator to use. */
    private final HBaseColumnNameTranslator mTranslator;

    /**
     * Initialize this context with the specified column name translator.
     *
     * @param translator the translator to use.
     */
    private NameTranslatingFilterContext(final HBaseColumnNameTranslator translator) {
      mTranslator = translator;
    }

    /** {@inheritDoc} */
    @Override
    public HBaseColumnName getHBaseColumnName(
        final KijiColumnName kijiColumnName
    ) throws NoSuchColumnException {
      return mTranslator.toHBaseColumnName(kijiColumnName);
    }
  }
}
