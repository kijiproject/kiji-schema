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


import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import static org.kiji.schema.util.GetEquals.eqGet;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.impl.HashedEntityId;
import org.kiji.schema.layout.ColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestKijiColumnPager extends KijiClientTest {
  private ColumnNameTranslator mColumnNameTranslator;
  private EntityId mEntityId;
  private KijiDataRequest mDataRequest;
  private HTableInterface mHTable;
  private KijiColumnPager mPager;

  @Before
  public void setup() throws Exception {
    final KijiTableLayout tableLayout = getKiji().getMetaTable()
        .updateTableLayout("user", KijiTableLayouts.getLayout(KijiTableLayouts.PAGING_TEST));

    mColumnNameTranslator = new ColumnNameTranslator(tableLayout);

    mEntityId = HashedEntityId.fromKijiRowKey(
        Bytes.toBytes("Garrett"), tableLayout.getDesc().getKeysFormat());

    mDataRequest = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("info", "name"))
        .addColumn(new KijiDataRequest.Column("info", "location")
            .withMaxVersions(5)
            .withPageSize(2))
        .addColumn(new KijiDataRequest.Column("jobs")
            .withMaxVersions(4)
            .withPageSize(2));

    mHTable = createMock(HTableInterface.class);

    mPager = new KijiColumnPager(mEntityId, mDataRequest, tableLayout, mHTable);
  }

  @Test(expected=NoSuchColumnException.class)
  public void testNoSuchColumn() throws IOException {
    mPager.getNextPage("doesn't", "exist");
  }

  @Test(expected=KijiColumnPagingNotEnabledException.class)
  public void testPagingNotEnabledOnColumn() throws IOException {
    mPager.getNextPage("info", "name");
  }

  @Test
  public void testGetColumnPage() throws IOException {
    //
    // Set the expected calls to the HTable.
    //
    final byte[] row = mEntityId.getHBaseRowKey();
    final HBaseColumnName hColumn =
        mColumnNameTranslator.toHBaseColumnName(new KijiColumnName("info", "location"));

    // Expect a request to the HTable for page 2 and 3.
    Get expectedGet = new Get(row);
    expectedGet.addColumn(hColumn.getFamily(), hColumn.getQualifier());
    expectedGet.setMaxVersions(5);
    // TODO: Our current HBase Get mock comparators don't check the filters, since
    // none of the HBase filters have their equals() methods implemented. For now, we'll
    // just check that the correct columns and maxVersions were set. We'll rely on the
    // integration test to make sure the filters were set correctly.

    Result cannedResult1 = new Result(new KeyValue[] {
      new KeyValue(row, hColumn.getFamily(), hColumn.getQualifier(), 4L, e("San Jose, CA")),
      new KeyValue(row, hColumn.getFamily(), hColumn.getQualifier(), 3L, e("Los Altos, CA")),
    });
    Result cannedResult2 = new Result(new KeyValue[] {
      new KeyValue(row, hColumn.getFamily(), hColumn.getQualifier(), 2L, e("Seattle, WA")),
    });
    expect(mHTable.get(eqGet(expectedGet)))
        .andReturn(cannedResult1)
        .andReturn(cannedResult2);
    replay(mHTable);

    //
    // Get some pages!
    //
    NavigableMap<Long, byte[]> cells;

    cells = mPager.getNextPage("info", "location");
    assertNotNull("Missing 2nd page of data.", cells);
    assertEquals(2, cells.size());

    cells = mPager.getNextPage("info", "location");
    assertNotNull("Missing 3rd page of data.", cells);
    assertEquals(1, cells.size());

    assertNull("Should be no more data.", mPager.getNextPage("info", "location"));

    //
    // Verify mock expectations.
    //
    verify(mHTable);
  }

  @Test
  public void testGetFamilyPage() throws IOException {
    //
    // Set the expected calls to the HTable.
    //
    final byte[] row = mEntityId.getHBaseRowKey();
    final HBaseColumnName hColumn =
        mColumnNameTranslator.toHBaseColumnName(new KijiColumnName("jobs"));
    final HBaseColumnName wibidata =
        mColumnNameTranslator.toHBaseColumnName(new KijiColumnName("jobs:wibidata"));
    final HBaseColumnName google =
        mColumnNameTranslator.toHBaseColumnName(new KijiColumnName("jobs:google"));

    // Expect a request to the HTable for page 2 and 3 (empty).
    Get expectedGet = new Get(row);
    expectedGet.addFamily(hColumn.getFamily());
    expectedGet.setMaxVersions(4);
    Result cannedResult1 = new Result(new KeyValue[] {
      new KeyValue(row, wibidata.getFamily(), wibidata.getQualifier(), 4L, e("Engineer")),
      new KeyValue(row, google.getFamily(), google.getQualifier(), 3L, e("Engineer")),
    });
    Result cannedResult2 = new Result(new KeyValue[] {}); // no more results!
    expect(mHTable.get(eqGet(expectedGet)))
        .andReturn(cannedResult1)
        .andReturn(cannedResult2);
    replay(mHTable);

    //
    // Get some pages!
    //
    NavigableMap<String, NavigableMap<Long, byte[]>> cells;

    cells = mPager.getNextPage("jobs");
    assertNotNull("Missing 2nd page of data.", cells);
    assertEquals(2, cells.size());

    assertNull("Should be no more data.", mPager.getNextPage("jobs"));

    //
    // Verify mock expectations.
    //
    verify(mHTable);
  }
}
