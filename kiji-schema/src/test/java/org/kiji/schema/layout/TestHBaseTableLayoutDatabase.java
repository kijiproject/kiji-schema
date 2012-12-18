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

package org.kiji.schema.layout;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import static org.kiji.schema.util.GetEquals.eqGet;
import static org.kiji.schema.util.PutEquals.eqPut;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCellFormat;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.impl.HBaseTableLayoutDatabase;


public class TestHBaseTableLayoutDatabase extends KijiClientTest {
  /** A simple layout file example (bare minimum). */
  public static final String SIMPLE_LAYOUT_RESOURCE = "com/Kijidata/core/layout/simple-layout.xml";

  private HTableInterface mHTable;
  private String mFamily;
  private HBaseTableLayoutDatabase mDb;

  @Before
  public void setupDb() throws IOException {
    mHTable = createMock(HTableInterface.class);
    mFamily = "layout";
    mDb = new HBaseTableLayoutDatabase(mHTable, mFamily, getKiji().getSchemaTable());
  }

  @Test
  public void testSetLayout() throws Exception {
    final TableLayoutDesc layoutDesc = KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE);
    final KijiTableLayout layout = new KijiTableLayout(layoutDesc, null);

    final Get expectedGet =
        new Get(Bytes.toBytes(layout.getDesc().getName()))
            .addColumn(Bytes.toBytes(mFamily),
                Bytes.toBytes(HBaseTableLayoutDatabase.QUALIFIER_LAYOUT))
            .setMaxVersions(1);
    final Result expectedGetResult = new Result(Collections.<KeyValue>emptyList());
    expect(mHTable.get(eqGet(expectedGet))).andReturn(expectedGetResult);

    final Put expectedPut =
        new Put(Bytes.toBytes(layout.getDesc().getName()))
            .add(Bytes.toBytes(mFamily),
                Bytes.toBytes(HBaseTableLayoutDatabase.QUALIFIER_UPDATE),
                getCellEncoder().encode(
                    new KijiCell<TableLayoutDesc>(TableLayoutDesc.SCHEMA$, layoutDesc),
                    KijiCellFormat.HASH))
            .add(Bytes.toBytes(mFamily),
                Bytes.toBytes(HBaseTableLayoutDatabase.QUALIFIER_LAYOUT),
                getCellEncoder().encode(
                    new KijiCell<TableLayoutDesc>(TableLayoutDesc.SCHEMA$, layout.getDesc()),
                    KijiCellFormat.HASH))
            .add(Bytes.toBytes(mFamily),
                Bytes.toBytes(HBaseTableLayoutDatabase.QUALIFIER_LAYOUT_ID),
                Bytes.toBytes("1"));
    mHTable.put(eqPut(expectedPut));

    replay(mHTable);

    mDb.updateTableLayout(layout.getDesc().getName(), layoutDesc);

    verify(mHTable);
  }

  @Test
  public void testGetLayout() throws Exception {
    final KijiTableLayout version1 =
        new KijiTableLayout(KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE), null);

    final Get expectedGet = new Get(Bytes.toBytes(version1.getDesc().getName()))
        .addColumn(Bytes.toBytes(mFamily), Bytes.toBytes(HBaseTableLayoutDatabase.QUALIFIER_LAYOUT))
        .setMaxVersions(1);
    final List<KeyValue> kvs = new ArrayList<KeyValue>();
    final KijiCell<TableLayoutDesc> cell =
        new KijiCell<TableLayoutDesc>(TableLayoutDesc.SCHEMA$, version1.getDesc());
    kvs.add(new KeyValue(Bytes.toBytes(version1.getDesc().getName()),
        Bytes.toBytes(mFamily), Bytes.toBytes(HBaseTableLayoutDatabase.QUALIFIER_LAYOUT), 1L,
        getCellEncoder().encode(cell, KijiCellFormat.HASH)));
    final Result cannedResult = new Result(kvs);
    expect(mHTable.get(eqGet(expectedGet))).andReturn(cannedResult);

    replay(mHTable);

    assertEquals(version1, mDb.getTableLayout(version1.getDesc().getName().toString()));

    verify(mHTable);
  }

  @Test
  public void testGetMulitipleLayouts() throws Exception {
    final KijiTableLayout layout1 =
        new KijiTableLayout(KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE), null);
    final KijiTableLayout layout2 =
        new KijiTableLayout(KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE), null);
    final KijiTableLayout layout3 =
        new KijiTableLayout(KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE), null);

    layout1.getDesc().setVersion("1.1");
    layout2.getDesc().setVersion("2.2");
    layout3.getDesc().setVersion("3.3");


    final Get expectedGet =
        new Get(Bytes.toBytes(layout1.getDesc().getName()))
            .addColumn(Bytes.toBytes(mFamily),
                Bytes.toBytes(HBaseTableLayoutDatabase.QUALIFIER_LAYOUT))
            .setMaxVersions(2);

    final List<KeyValue> kvs = new ArrayList<KeyValue>();
    final KijiCell<TableLayoutDesc> cell3 =
        new KijiCell<TableLayoutDesc>(TableLayoutDesc.SCHEMA$, layout3.getDesc());
    kvs.add(new KeyValue(Bytes.toBytes(layout3.getDesc().getName()),
            Bytes.toBytes(mFamily), Bytes.toBytes(HBaseTableLayoutDatabase.QUALIFIER_LAYOUT),
            3L, getCellEncoder().encode(cell3, KijiCellFormat.HASH)));
    KijiCell<TableLayoutDesc> cell2 = new KijiCell<TableLayoutDesc>(
        TableLayoutDesc.SCHEMA$, layout2.getDesc());
    kvs.add(new KeyValue(Bytes.toBytes(layout2.getDesc().getName()),
            Bytes.toBytes(mFamily), Bytes.toBytes(HBaseTableLayoutDatabase.QUALIFIER_LAYOUT),
            2L, getCellEncoder().encode(cell2, KijiCellFormat.HASH)));
    Result cannedResult = new Result(kvs);
    expect(mHTable.get(eqGet(expectedGet))).andReturn(cannedResult);

    replay(mHTable);

    NavigableMap<Long, KijiTableLayout> timedLayouts =
        mDb.getTimedTableLayoutVersions(layout1.getDesc().getName().toString(), 2);
    Set<Long> timestamps = timedLayouts.keySet();
    Iterator<Long> iterator = timestamps.iterator();

    assertEquals(2, timedLayouts.size());
    long time2 = iterator.next();
    long time3 = iterator.next();
    assertEquals(2L, time2);
    assertEquals(layout2, timedLayouts.get(time2));
    assertEquals(3L, time3);
    assertEquals(layout3, timedLayouts.get(time3));

    verify(mHTable);
  }

}
