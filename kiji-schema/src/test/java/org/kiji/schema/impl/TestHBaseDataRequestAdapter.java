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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;

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
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.impl.ColumnNameTranslator;

public class TestHBaseDataRequestAdapter extends KijiClientTest {
  private KijiTableLayout mTableLayout;
  private EntityIdFactory mEntityIdFactory;
  private ColumnNameTranslator mColumnNameTranslator;

  @Before
  public void setupLayout() throws Exception {
    final KijiTableLayout tableLayout =
        KijiTableLayouts.getTableLayout(KijiTableLayouts.FULL_FEATURED);
    getKiji().createTable(tableLayout.getDesc());

    mTableLayout = getKiji().getMetaTable().getTableLayout("user");
    mEntityIdFactory = EntityIdFactory.getFactory(mTableLayout);
    mColumnNameTranslator = new ColumnNameTranslator(mTableLayout);
  }

  @Test
  public void testDataRequestToScan() throws IOException {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(1).add("info", "name");
    builder.newColumnsDef().withMaxVersions(2).addFamily("purchases");
    builder.withTimeRange(1L, 3L);
    KijiDataRequest request = builder.build();

    Scan expectedScan = new Scan();
    HBaseColumnName hbaseColumn = mColumnNameTranslator.toHBaseColumnName(
        new KijiColumnName("info:name"));
    expectedScan.addColumn(hbaseColumn.getFamily(), hbaseColumn.getQualifier());
    HBaseColumnName hPurchasesColumn = mColumnNameTranslator.toHBaseColumnName(
        new KijiColumnName("purchases"));
    expectedScan.addFamily(hPurchasesColumn.getFamily());

    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);

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
    FilterList infoNameFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    Filter infoLgFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(hbaseColumn.getFamily()));
    infoNameFilter.addFilter(infoLgFilter);
    Filter infoNameQualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(hbaseColumn.getQualifier()));
    infoNameFilter.addFilter(infoNameQualifierFilter);
    infoNameFilter.addFilter(new ColumnPaginationFilter(1, 0));

    FilterList purchasesFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    Filter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(hPurchasesColumn.getFamily()));
    Filter mapPrefixFilter = new ColumnPrefixFilter(hPurchasesColumn.getQualifier());
    purchasesFilter.addFilter(familyFilter);
    purchasesFilter.addFilter(mapPrefixFilter);

    filterList.addFilter(infoNameFilter);
    filterList.addFilter(purchasesFilter);
    expectedScan.setFilter(filterList);
    expectedScan.setMaxVersions(2);
    expectedScan.setTimeRange(1L, 3L);

    HBaseDataRequestAdapter hbaseDataRequest = new HBaseDataRequestAdapter(request);
    assertEquals(expectedScan.toString(), hbaseDataRequest.toScan(mTableLayout).toString());
  }

  @Test
  public void testDataRequestToScanEmpty() throws IOException {
    KijiDataRequest request = KijiDataRequest.builder().build();
    HBaseDataRequestAdapter hbaseDataRequest = new HBaseDataRequestAdapter(request);
    assertFalse(hbaseDataRequest.toScan(mTableLayout).hasFamilies());
  }

  @Test
  public void testDataRequestToGet() throws IOException {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(1).add("info", "name");
    builder.newColumnsDef().withMaxVersions(2).addFamily("purchases");
    builder.withTimeRange(1L, 3L);
    KijiDataRequest request = builder.build();

    EntityId entityId = mEntityIdFactory.getEntityId("entity");
    Get expectedGet = new Get(entityId.getHBaseRowKey());
    HBaseColumnName hbaseColumn = mColumnNameTranslator.toHBaseColumnName(
        new KijiColumnName("info:name"));
    expectedGet.addColumn(hbaseColumn.getFamily(), hbaseColumn.getQualifier());
    HBaseColumnName hPurchasesColumn = mColumnNameTranslator.toHBaseColumnName(
        new KijiColumnName("purchases"));
    expectedGet.addFamily(hPurchasesColumn.getFamily());

    // See comments in testDataRequestToScan() describing this functionality.
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);

    FilterList infoNameFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    Filter infoLgFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(hbaseColumn.getFamily()));
    infoNameFilter.addFilter(infoLgFilter);
    Filter infoNameQualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(hbaseColumn.getQualifier()));
    infoNameFilter.addFilter(infoNameQualifierFilter);
    infoNameFilter.addFilter(new ColumnPaginationFilter(1, 0));

    FilterList purchasesFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    Filter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(hPurchasesColumn.getFamily()));
    Filter mapPrefixFilter = new ColumnPrefixFilter(hPurchasesColumn.getQualifier());
    purchasesFilter.addFilter(familyFilter);
    purchasesFilter.addFilter(mapPrefixFilter);

    filterList.addFilter(infoNameFilter);
    filterList.addFilter(purchasesFilter);
    expectedGet.setFilter(filterList);

    expectedGet.setMaxVersions(2);
    expectedGet.setTimeRange(1L, 3L);

    HBaseDataRequestAdapter hbaseDataRequest = new HBaseDataRequestAdapter(request);
    assertEquals(expectedGet.toString(),
        hbaseDataRequest.toGet(entityId, mTableLayout).toString());
  }

  @Test
  public void testDataRequestToGetEmpty() throws IOException {
    KijiDataRequest request = KijiDataRequest.builder().build();
    HBaseDataRequestAdapter hbaseDataRequest = new HBaseDataRequestAdapter(request);
    assertFalse(
        hbaseDataRequest.toGet(mEntityIdFactory.getEntityId("entity"), mTableLayout).hasFamilies());
  }
}
