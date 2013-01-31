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

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.SchemaStorage;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.layout.ColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.impl.CellSpec;

public class TestKijiColumnPager extends KijiClientTest {
  private ColumnNameTranslator mColumnNameTranslator;
  private EntityId mEntityId;
  private KijiDataRequest mDataRequest;
  private HTableInterface mHTable;
  private KijiColumnPager mPager;

  /** Cell encoders. */
  private KijiCellEncoder mStringCellEncoder;

  private void initEncoder() throws Exception {
    final CellSchema stringCellSchema = CellSchema.newBuilder()
        .setStorage(SchemaStorage.HASH)
        .setType(SchemaType.INLINE)
        .setValue("\"string\"")
        .build();
    final CellSpec stringCellSpec = new CellSpec()
        .setCellSchema(stringCellSchema)
        .setSchemaTable(getKiji().getSchemaTable());
    mStringCellEncoder = new AvroCellEncoder(stringCellSpec);
  }

  protected byte[] encode(String str) throws IOException {
    return mStringCellEncoder.encode(str);
  }

  @Before
  public void setup() throws Exception {
    final KijiTableLayout tableLayout =
        KijiTableLayouts.getTableLayout(KijiTableLayouts.PAGING_TEST);
    getKiji().createTable(tableLayout.getName(), tableLayout);

    mColumnNameTranslator = new ColumnNameTranslator(tableLayout);

    mEntityId = HashedEntityId.fromKijiRowKey(
        Bytes.toBytes("Garrett"), tableLayout.getDesc().getKeysFormat());

    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.addColumns().add("info", "name");
    builder.addColumns().withMaxVersions(5).withPageSize(2).add("info", "location");
    builder.addColumns().withMaxVersions(4).withPageSize(2).addFamily("jobs");
    mDataRequest = builder.build();

    mHTable = createMock(HTableInterface.class);

    mPager = new KijiColumnPager(mEntityId, mDataRequest, tableLayout, mHTable);

    initEncoder();
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
      new KeyValue(row, hColumn.getFamily(), hColumn.getQualifier(), 4L, encode("San Jose, CA")),
      new KeyValue(row, hColumn.getFamily(), hColumn.getQualifier(), 3L, encode("Los Altos, CA")),
    });
    Result cannedResult2 = new Result(new KeyValue[] {
      new KeyValue(row, hColumn.getFamily(), hColumn.getQualifier(), 2L, encode("Seattle, WA")),
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
      new KeyValue(row, wibidata.getFamily(), wibidata.getQualifier(), 4L, encode("Engineer")),
      new KeyValue(row, google.getFamily(), google.getQualifier(), 3L, encode("Engineer")),
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
