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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.impl.HBaseDataRequestAdapter;
import org.kiji.schema.layout.ColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;


public class TestHBaseDataRequestAdapter extends KijiClientTest {
  private KijiTableLayout mTableLayout;
  private EntityIdFactory mEntityIdFactory;
  private ColumnNameTranslator mColumnNameTranslator;

  @Before
  public void setupLayout() throws Exception {
    final KijiTableLayout tableLayout =
        KijiTableLayouts.getTableLayout(KijiTableLayouts.FULL_FEATURED);
    getKiji().getAdmin().createTable(tableLayout.getName(), tableLayout, false);

    mTableLayout = getKiji().getMetaTable().getTableLayout("user");
    mEntityIdFactory = EntityIdFactory.create(mTableLayout.getDesc().getKeysFormat());
    mColumnNameTranslator = new ColumnNameTranslator(mTableLayout);
  }

  @Test
  public void testDataRequestToScan() throws IOException {
    KijiDataRequest request = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("info", "name").withMaxVersions(1))
        .addColumn(new KijiDataRequest.Column("purchases").withMaxVersions(2))
        .withTimeRange(1L, 3L);

    Scan expectedScan = new Scan();
    HBaseColumnName hbaseColumn = mColumnNameTranslator.toHBaseColumnName(
        new KijiColumnName("info:name"));
    expectedScan.addColumn(hbaseColumn.getFamily(), hbaseColumn.getQualifier());
    HBaseColumnName hPurchasesColumn = mColumnNameTranslator.toHBaseColumnName(
        new KijiColumnName("purchases"));
    expectedScan.addFamily(hPurchasesColumn.getFamily());
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    FilterList requestFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    Filter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(hPurchasesColumn.getFamily()));
    Filter mapPrefixFilter = new ColumnPrefixFilter(hPurchasesColumn.getQualifier());
    requestFilter.addFilter(familyFilter);
    requestFilter.addFilter(mapPrefixFilter);
    filterList.addFilter(requestFilter);
    expectedScan.setFilter(filterList);
    expectedScan.setMaxVersions(2);
    expectedScan.setTimeRange(1L, 3L);

    HBaseDataRequestAdapter hbaseDataRequest = new HBaseDataRequestAdapter(request);
    assertEquals(expectedScan.toString(), hbaseDataRequest.toScan(mTableLayout).toString());
  }

  @Test
  public void testDataRequestToScanEmpty() throws IOException {
    KijiDataRequest request = new KijiDataRequest();
    HBaseDataRequestAdapter hbaseDataRequest = new HBaseDataRequestAdapter(request);
    assertNull(hbaseDataRequest.toScan(mTableLayout));
  }

  @Test
  public void testDataRequestToGet() throws IOException {
    KijiDataRequest request = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("info", "name").withMaxVersions(1))
        .addColumn(new KijiDataRequest.Column("purchases").withMaxVersions(2))
        .withTimeRange(1L, 3L);

    EntityId entityId = mEntityIdFactory.fromKijiRowKey("entity");
    Get expectedGet = new Get(entityId.getHBaseRowKey());
    HBaseColumnName hbaseColumn = mColumnNameTranslator.toHBaseColumnName(
        new KijiColumnName("info:name"));
    expectedGet.addColumn(hbaseColumn.getFamily(), hbaseColumn.getQualifier());
    HBaseColumnName hPurchasesColumn = mColumnNameTranslator.toHBaseColumnName(
        new KijiColumnName("purchases"));
    expectedGet.addFamily(hPurchasesColumn.getFamily());
    Filter mapPrefixFilter = new ColumnPrefixFilter(hPurchasesColumn.getQualifier());
    expectedGet.setFilter(mapPrefixFilter);
    expectedGet.setMaxVersions(2);
    expectedGet.setTimeRange(1L, 3L);

    HBaseDataRequestAdapter hbaseDataRequest = new HBaseDataRequestAdapter(request);
    assertEquals(expectedGet.toString(),
        hbaseDataRequest.toGet(entityId, mTableLayout).toString());
  }

  @Test
  public void testDataRequestToGetEmpty() throws IOException {
    KijiDataRequest request = new KijiDataRequest();
    HBaseDataRequestAdapter hbaseDataRequest = new HBaseDataRequestAdapter(request);
    assertNull(
        hbaseDataRequest.toGet(mEntityIdFactory.fromKijiRowKey("entity"), mTableLayout));
  }
}
