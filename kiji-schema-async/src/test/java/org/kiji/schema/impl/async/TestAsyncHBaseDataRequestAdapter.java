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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hbase.HConstants;
import org.hbase.async.BinaryComparator;
import org.hbase.async.ColumnPaginationFilter;
import org.hbase.async.ColumnPrefixFilter;
import org.hbase.async.CompareFilter.CompareOp;
import org.hbase.async.FamilyFilter;
import org.hbase.async.FilterList;
import org.hbase.async.FilterList.Operator;
import org.hbase.async.QualifierFilter;
import org.hbase.async.ScanFilter;
import org.hbase.async.Scanner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiResult.Helpers;
import org.kiji.schema.KijiResultScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestAsyncHBaseDataRequestAdapter extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestAsyncHBaseDataRequestAdapter.class);

  private KijiTableLayout mTableLayout;
  private EntityIdFactory mEntityIdFactory;
  private HBaseColumnNameTranslator mColumnNameTranslator;
  private AsyncHBaseKiji mAsyncHBaseKiji;

  @Before
  public void setupLayout() throws Exception {
    final KijiTableLayout tableLayout =
        KijiTableLayouts.getTableLayout(KijiTableLayouts.FULL_FEATURED);
    getKiji().createTable(tableLayout.getDesc());

    mAsyncHBaseKiji = new AsyncHBaseKiji(getKiji().getURI());
    mTableLayout = mAsyncHBaseKiji.getMetaTable().getTableLayout("user");
    mEntityIdFactory = EntityIdFactory.getFactory(mTableLayout);
    mColumnNameTranslator = HBaseColumnNameTranslator.from(mTableLayout);
  }

  @After
  public final void cleanupEnvironment() throws IOException{
    mAsyncHBaseKiji.release();
  }

  private byte[] kijiToTableName(Kiji kiji, String tableName) {
    return KijiManagedHBaseTableName.getKijiTableName(
        kiji.getURI().getInstance(),
        tableName).toBytes();
  }

  @Test
  public void testDataRequestToScan() throws IOException {
    final byte[] tableName = kijiToTableName(mAsyncHBaseKiji, mTableLayout.getName());
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(1).add("info", "name");
    builder.newColumnsDef().withMaxVersions(2).addFamily("purchases");
    builder.withTimeRange(1L, 3L);
    KijiDataRequest request = builder.build();

    Scanner expectedScan = mAsyncHBaseKiji.getHBaseClient().newScanner(tableName);
    HBaseColumnName hbaseColumn = mColumnNameTranslator.toHBaseColumnName(
        KijiColumnName.create("info:name"));
    byte[][] families = new byte[2][];
    byte[][][] qualifiers = new byte[2][1][];
    families[0] = hbaseColumn.getFamily();
    qualifiers[0][0] = hbaseColumn.getQualifier();
    HBaseColumnName hPurchasesColumn = mColumnNameTranslator.toHBaseColumnName(
        KijiColumnName.create("purchases"));
    families[1] = hPurchasesColumn.getFamily();
    qualifiers[1] = null;
    expectedScan.setFamilies(families, qualifiers);
    List<ScanFilter> filterList = Lists.newArrayList();

    // The Scan object created by HBaseDataRequestAdapter has a filter attached
    // to it which corresponds to the set of filters associated with each input
    // column (either explicitly, or implicitly by the logic of HBaseDataRequestAdapter):
    //
    // Each column (e.g., info:name) has a top-level AND(...) filter containing:
    // * A FamilyFilter that refers to the translated HBase family (loc group) name for the column
    // * A QualifierFilter that refers to the translated qualifier name
    // * If maxVersions is 1 for the column, a ColumnPaginationFilter(1, 0) to enforce that.
    //
    // Each column family has a top-level AND(...) filter containing:
    // * A FamilyFilter as above
    // * A ColumnPrefixFilter to filter/include only the map-type family within the locality group
    //
    // These are joined together with a request-level OR(...) filter; so in effect every
    // cell included must pass all the filters associated with one of the columns requested.
    List<ScanFilter> infoNameFilterList = Lists.newArrayList();
    ScanFilter infoLgFilter = new FamilyFilter(CompareOp.EQUAL,
        new BinaryComparator(hbaseColumn.getFamily()));
    infoNameFilterList.add(infoLgFilter);
    ScanFilter infoNameQualifierFilter = new QualifierFilter(CompareOp.EQUAL,
        new BinaryComparator(hbaseColumn.getQualifier()));
    infoNameFilterList.add(infoNameQualifierFilter);
    infoNameFilterList.add(new ColumnPaginationFilter(1, 0));

    List<ScanFilter> purchaseFilterList = Lists.newArrayList();

    ScanFilter familyFilter = new FamilyFilter(CompareOp.EQUAL,
        new BinaryComparator(hPurchasesColumn.getFamily()));
    ScanFilter mapPrefixFilter = new ColumnPrefixFilter(hPurchasesColumn.getQualifier());
    purchaseFilterList.add(familyFilter);
    purchaseFilterList.add(mapPrefixFilter);

    FilterList infoNameFilter = new FilterList(infoNameFilterList, Operator.MUST_PASS_ALL);
    FilterList purchasesFilter = new FilterList(purchaseFilterList, Operator.MUST_PASS_ALL);
    filterList.add(infoNameFilter);
    filterList.add(purchasesFilter);

    expectedScan.setFilter(new FilterList(filterList, Operator.MUST_PASS_ONE));
    expectedScan.setMaxVersions(2);
    expectedScan.setTimeRange(1L, 3L);

    AsyncHBaseDataRequestAdapter asyncDataRequest = AsyncHBaseDataRequestAdapter.create(
        request,
        HBaseColumnNameTranslator.from(mTableLayout),
        mAsyncHBaseKiji.getHBaseClient(),
        tableName);
    Scanner tempScanner = asyncDataRequest.toScanner(mTableLayout);
    assertEquals(expectedScan.toString(), tempScanner.toString());
  }

  @Test
  public void testDataRequestToScanEmpty() throws IOException {
    KijiDataRequest request = KijiDataRequest.builder().build();
    final byte[] tableName = kijiToTableName(mAsyncHBaseKiji, mTableLayout.getName());
    AsyncHBaseDataRequestAdapter asyncDataRequest =
        AsyncHBaseDataRequestAdapter.create(
            request,
            mColumnNameTranslator,
            mAsyncHBaseKiji.getHBaseClient(),
            tableName);
    Scanner expectedScanner = mAsyncHBaseKiji.getHBaseClient().newScanner(tableName);
    assertEquals(expectedScanner.toString(), asyncDataRequest.toScanner(mTableLayout).toString());
  }

  /**
   * Tests that combining column requests with different max-versions works properly.
   * This test focuses on the case where one column has max-versions == 1,
   * which relies on the ColumnPagingFilter.
   *
   * No paging involved in this test.
   */
  @Test
  public void testMaxVersionsEqualsOne() throws Exception {
    final Kiji kiji = new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(KijiTableLayouts.ROW_DATA_TEST))
            .withRow("row0")
                .withFamily("family")
                    .withQualifier("qual0")
                        .withValue(1L, "value-1")
                        .withValue(2L, "value-2")
                        .withValue(3L, "value-3")
                    .withQualifier("qual1")
                        .withValue(1L, "value-1")
                        .withValue(2L, "value-2")
                        .withValue(3L, "value-3")
                    .withQualifier("qual2")
                        .withValue(1L, "value-1")
                        .withValue(2L, "value-2")
                        .withValue(3L, "value-3")
        .build();
    final AsyncHBaseKiji asyncKiji = new AsyncHBaseKiji(kiji.getURI());
    final AsyncHBaseKijiTable table = (AsyncHBaseKijiTable) asyncKiji.openTable("row_data_test_table");
    try {
      final AsyncHBaseKijiTableReader reader = (AsyncHBaseKijiTableReader) table.openTableReader();
      try {
        final KijiDataRequest dataRequest = KijiDataRequest.builder()
            .addColumns(ColumnsDef.create()
                .withMaxVersions(1)
                .add("family", "qual0"))
            .addColumns(ColumnsDef.create()
                .withMaxVersions(2)
                .add("family", "qual1"))
            .addColumns(ColumnsDef.create()
                .withMaxVersions(3)
                .add("family", "qual2"))
            .build();
        final KijiResult<Object> row = reader.getResult(table.getEntityId("row0"), dataRequest);
        final KijiResult<Object> result1 = row.narrowView(KijiColumnName.create("family", "qual0"));
        final KijiResult<Object> result2 = row.narrowView(KijiColumnName.create("family", "qual1"));
        final KijiResult<Object> result3 = row.narrowView(KijiColumnName.create("family", "qual2"));
        assertEquals(1, ImmutableList.copyOf(Helpers.getVersions(result1)).size());
        assertEquals(2, ImmutableList.copyOf(Helpers.getVersions(result2)).size());
        assertEquals(3, ImmutableList.copyOf(Helpers.getVersions(result3)).size());
      } finally {
        reader.close();
      }
    } finally {
      table.release();
      asyncKiji.release();
    }
  }

  /**
   * Tests that requesting an entire group-type family properly expands to all declared columns.
   */
  @Test
  public void testExpandGroupTypeFamily() throws Exception {
    final Kiji kiji = new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(KijiTableLayouts.ROW_DATA_TEST))
            .withRow("row0")
                .withFamily("family")
                    .withQualifier("qual0")
                        .withValue(1L, "value")
                    .withQualifier("qual1")
                        .withValue(1L, "value")
        .build();

    final Kiji asyncKiji = new AsyncHBaseKijiFactory().open(kiji.getURI());
    final AsyncHBaseKijiTable table = (AsyncHBaseKijiTable) asyncKiji.openTable("row_data_test_table");
    try {
      final AsyncHBaseKijiTableReader reader = (AsyncHBaseKijiTableReader) table.openTableReader();
      try {
        final KijiDataRequest dataRequest = KijiDataRequest.builder()
            .addColumns(ColumnsDef.create()
                .addFamily("family"))
            .build();
        final KijiResult<Object> row = reader.getResult(table.getEntityId("row0"), dataRequest);
        final KijiResult<Object> result1 = row.narrowView(KijiColumnName.create("family", "qual0"));
        final KijiResult<Object> result2 = row.narrowView(KijiColumnName.create("family", "qual1"));
        assertEquals(1, ImmutableList.copyOf(Helpers.getValues(result1)).size());
        assertEquals(1, ImmutableList.copyOf(Helpers.getValues(result2)).size());
      } finally {
        reader.close();
      }
    } finally {
      table.release();
      asyncKiji.release();
    }
  }

  /**
   * Simple function to convert an object to a String.
   */
  private static final class ToString<T> implements Function<T, String> {
    @Override
    public String apply(final T input) {
      return input.toString();
    }
  }

  /**
   * Test a partially paged data request.
   * Ensures that combining data requests with paged and non-paged columns work.
   * In particular, paging-related filters should not have side-effects on non-paged columns.
   *
   * Here we test that enabling paging on a column X:Y doesn't affect cells from other columns,
   * ie. we can still read the cells from non-paged columns.
   */
  @Test
  public void testScanPartiallyPaged() throws Exception {
    final Kiji kiji = new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(KijiTableLayouts.ROW_DATA_TEST))
            .withRow("row0")
                .withFamily("family")
                    .withQualifier("qual0")
                        .withValue(1L, "value1")
                        .withValue(2L, "value2")
                        .withValue(3L, "value3")
                .withFamily("map")
                    .withQualifier("int1")
                        .withValue(0L, 10)
                        .withValue(1L, 11)
                    .withQualifier("int2")
                        .withValue(0L, 20)
                        .withValue(1L, 21)
            .withRow("row1")
                .withFamily("family")
                    .withQualifier("qual0")
                        .withValue("value1")
            .withRow("row2")
                .withFamily("map")
                    .withQualifier("int2")
                        .withValue(2)
            .withRow("row3")
                .withFamily("family")
                    .withQualifier("qual1")
                        .withValue("value1")
        .build();
    final Kiji asyncKiji = new AsyncHBaseKijiFactory().open(kiji.getURI());
    final KijiTable table = asyncKiji.openTable("row_data_test_table");
    try {
      final AsyncHBaseKijiTableReader reader = (AsyncHBaseKijiTableReader) table.openTableReader();
      try {
        final KijiDataRequest dataRequest = KijiDataRequest.builder()
            .addColumns(ColumnsDef.create()
                .withMaxVersions(HConstants.ALL_VERSIONS)
                .add("family", "qual0"))
            .addColumns(ColumnsDef.create()
                .withMaxVersions(HConstants.ALL_VERSIONS)
                .withPageSize(1)
                .addFamily("map"))
            .build();
        final KijiResultScanner<Utf8> scanner =
            reader.getKijiResultScanner(dataRequest, new KijiScannerOptions());
        try {
          int nrows = 0;
          boolean foundRow0 = false;
          final Function<Utf8, String> toString = new ToString<Utf8>();
          while(scanner.hasNext()) {
            final KijiResult<Utf8> result = scanner.next();

            LOG.debug("Scanning row: {}", result);

            // All rows but "row3" should be scanned through:
            assertFalse(result.getEntityId().getComponentByIndex(0).equals("row3"));

            // Validate "row0", which contains both paged and non-paged cells:
            if (result.getEntityId().getComponentByIndex(0).equals("row0")) {
              foundRow0 = true;

              KijiResult<Utf8> narrowResult1 =
                  result.narrowView(KijiColumnName.create("family", "qual0"));
              // Make sure we can still read the columns that are not paged:
              assertEquals(ImmutableList.builder()
                  .add(new Utf8("value3"))
                  .add(new Utf8("value2"))
                  .add(new Utf8("value1"))
                  .build(),
                  ImmutableList.copyOf(Helpers.getValues(narrowResult1)));

              // The values for "map:*" should ideally not be retrieved.
              // We cannot use KeyOnlyFilter, but we can use FirstKeyOnlyFilter to limit
              // the number of KeyValues fetched:
              KijiResult<Utf8> narrowResult2 =
                  result.narrowView(KijiColumnName.create("map"));
              try {
                assertEquals(
                    ImmutableList.builder()
                        .add(11)
                        .build(),
                    ImmutableList.builder()
                        .add(narrowResult2.iterator().next().getData())
                        .build());
              } finally {
                narrowResult2.close();
              }
            }

            nrows += 1;
          }
          assertEquals(3, nrows);
          assertTrue(foundRow0);
        } finally {
          scanner.close();
        }
      } finally {
        reader.close();
      }
    } finally {
      table.release();
      asyncKiji.release();
    }
  }

  /**
   * Test a partially paged data request.
   * Ensures that combining data requests with paged and non-paged columns work.
   * In particular, paging-related filters should not have side-effects on non-paged columns.
   *
   * Here we test that paging on a column X:Y will not short-circuit the KeyValue scanner
   * and still return the requested KeyValues in columns > X:Y.
   */
  @Test
  public void testScanPartiallyPagedWithFirstKeyOnlyFilter() throws Exception {
    final Kiji kiji = new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(KijiTableLayouts.ROW_DATA_TEST))
            .withRow("row0")
                .withFamily("family")
                    .withQualifier("qual0")
                        .withValue(1L, "value1")
                        .withValue(2L, "value2")
                        .withValue(3L, "value3")
                .withFamily("map")
                    .withQualifier("int1")
                        .withValue(0L, 10)
                        .withValue(1L, 11)
                    .withQualifier("int2")
                        .withValue(0L, 20)
                        .withValue(1L, 21)
            .withRow("row1")
                .withFamily("family")
                    .withQualifier("qual0")
                        .withValue("value1")
            .withRow("row2")
                .withFamily("map")
                    .withQualifier("int2")
                        .withValue(2)
            .withRow("row3")
                .withFamily("family")
                    .withQualifier("qual1")
                        .withValue("value1")
        .build();

    final Kiji asyncKiji = new AsyncHBaseKijiFactory().open(kiji.getURI());
    final KijiTable table = asyncKiji.openTable("row_data_test_table");
    try {
      final AsyncHBaseKijiTableReader reader = (AsyncHBaseKijiTableReader) table.openTableReader();
      try {
        final KijiDataRequest dataRequest = KijiDataRequest.builder()
            .addColumns(ColumnsDef.create()
                .withMaxVersions(HConstants.ALL_VERSIONS)
                .withPageSize(1)
                .add("family", "qual0"))
            .addColumns(ColumnsDef.create()
                .withMaxVersions(HConstants.ALL_VERSIONS)
                .addFamily("map"))
            .build();
        final KijiResultScanner<Object> scanner = reader.getKijiResultScanner(
            dataRequest,
            new KijiScannerOptions());
        try {
          int nrows = 0;
          boolean foundRow0 = false;
          while (scanner.hasNext()) {
            final KijiResult<Object> result = scanner.next();
            LOG.debug("Scanning row: {}", result);

            // All rows but "row3" should be scanned through:
            assertFalse(result.getEntityId().getComponentByIndex(0).equals("row3"));

            // Validate "row0", which contains both paged and non-paged cells:
            if (result.getEntityId().getComponentByIndex(0).equals("row0")) {
              foundRow0 = true;

              // The values for "family:qual0" should ideally not be retrieved.
              // We cannot use KeyOnlyFilter, but we can use FirstKeyOnlyFilter to limit
              // the number of KeyValues fetched:
              KijiResult<Utf8> narrowResult1 =
                  result.narrowView(KijiColumnName.create("family", "qual0"));
              try {
                assertEquals(
                    new Utf8("value3"),
                    narrowResult1.iterator().next().getData());
              } finally {
                narrowResult1.close();
              }
              KijiResult<Integer> narrowResult2 =
                  result.narrowView(KijiColumnName.create("map"));
              // Make sure we can still read the columns that are not paged:
              Iterator<Integer> resultsIter = Helpers.getValues(narrowResult2).iterator();
              try {
                assertEquals(11, resultsIter.next().intValue());
                assertEquals(10, resultsIter.next().intValue());
                assertEquals(21, resultsIter.next().intValue());
                assertEquals(20, resultsIter.next().intValue());
              } finally {
                narrowResult2.close();
              }
            }

            nrows += 1;
          }
          assertEquals(3, nrows);
          assertTrue(foundRow0);
        } finally {
          scanner.close();
        }
      } finally {
        reader.close();
      }
    } finally {
      table.release();
      asyncKiji.release();
    }
  }

  /**
   * Tests a fully paged data request on fully-qualified columns.
   *
   * Scanning through rows with paging enabled returns rows where the only cells are visible
   * through paging (ie. the Result returned by the scanner would theoretically be empty).
   */
  @Test
  public void testScanCompletelyPaged() throws Exception {
    final Kiji kiji = new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(KijiTableLayouts.ROW_DATA_TEST))
            .withRow("row0")
                .withFamily("family")
                    .withQualifier("qual0")
                        .withValue("value0")
            .withRow("row1")
                .withFamily("family")
                    .withQualifier("qual1")
                        .withValue("value1")
            .withRow("row2")
                .withFamily("family")
                    .withQualifier("qual2")
                        .withValue("value2")
        .build();
    final Kiji asyncKiji = new AsyncHBaseKijiFactory().open(kiji.getURI());
    final KijiTable table = asyncKiji.openTable("row_data_test_table");
    try {
      final AsyncHBaseKijiTableReader reader = (AsyncHBaseKijiTableReader) table.openTableReader();
      try {
        final KijiDataRequest dataRequest = KijiDataRequest.builder()
            .addColumns(ColumnsDef.create()
                .withPageSize(1)
                .add("family", "qual0")
                .add("family", "qual1"))
            .build();
        final KijiResultScanner<Object> scanner =
            reader.getKijiResultScanner(dataRequest, new KijiScannerOptions());
        try {
          int nrows = 0;
          while (scanner.hasNext()) {
            final KijiResult<Object> result = scanner.next();
            LOG.debug("Scanning row: {}", result);

            // All rows but "row2" should be scanned through:
            assertFalse(result.getEntityId().getComponentByIndex(0).equals("row2"));
            nrows += 1;
          }
          assertEquals(2, nrows);
        } finally {
          scanner.close();
        }
      } finally {
        reader.close();
      }
    } finally {
      table.release();
      asyncKiji.release();
    }
  }

  /**
   * Tests a fully paged data request on a map-type family.
   *
   * Scanning through rows with paging enabled returns rows where the only cells are visible
   * through paging (ie. the Result returned by the scanner would theoretically be empty).
   */
  @Test
  public void testScanCompletelyPagedMapFamily() throws Exception {
    final Kiji kiji = new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(KijiTableLayouts.ROW_DATA_TEST))
            .withRow("row0")
                .withFamily("map")
                    .withQualifier("qual")
                        .withValue(314)
            .withRow("row1")
                .withFamily("map")
                    .withQualifier("qual")
                        .withValue(314)
            .withRow("row2")
                .withFamily("family")
                    .withQualifier("qual2")
                        .withValue("value2")
        .build();

    final Kiji asyncKiji = new AsyncHBaseKijiFactory().open(kiji.getURI());
    final KijiTable table = asyncKiji.openTable("row_data_test_table");
    try {
      final AsyncHBaseKijiTableReader reader = (AsyncHBaseKijiTableReader) table.openTableReader();
      try {
        final KijiDataRequest dataRequest = KijiDataRequest.builder()
            .addColumns(ColumnsDef.create()
                .withPageSize(1)
                .addFamily("map"))
            .build();
        final KijiResultScanner<Object> scanner =
            reader.getKijiResultScanner(dataRequest, new KijiScannerOptions());
        try {
          int nrows = 0;
          while (scanner.hasNext()) {
            final KijiResult<Object> result = scanner.next();
            LOG.debug("Scanning row: {}", result);

            // All rows but "row2" should be scanned through:
            assertFalse(result.getEntityId().getComponentByIndex(0).equals("row2"));
            nrows += 1;
          }
          assertEquals(2, nrows);
        } finally {
          scanner.close();
        }
      } finally {
        reader.close();
      }
    } finally {
      table.release();
      asyncKiji.release();
    }
  }
}
