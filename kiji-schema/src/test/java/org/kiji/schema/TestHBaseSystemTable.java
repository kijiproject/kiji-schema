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

import static org.kiji.schema.util.GetEquals.eqGet;
import static org.kiji.schema.util.PutEquals.eqPut;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.impl.HBaseSystemTable;

public class TestHBaseSystemTable {
  private HTableInterface mHtable;

  @Before
  public void setup() {
    mHtable = createMock(HTableInterface.class);
  }

  @Test
  public void testSetDataVersion() throws IOException {
    Put expected = new Put(Bytes.toBytes(HBaseSystemTable.KEY_DATA_VERSION));
    expected.add(Bytes.toBytes(HBaseSystemTable.VALUE_COLUMN_FAMILY),
        new byte[0],
        Bytes.toBytes("100"));
    mHtable.put(eqPut(expected));
    mHtable.close();

    replay(mHtable);
    HBaseSystemTable systemTable = new HBaseSystemTable(mHtable);
    systemTable.setDataVersion("100");
    systemTable.close();
    verify(mHtable);
  }

  @Test
  public void testGetDataVersion() throws IOException {
    Get expected = new Get(Bytes.toBytes(HBaseSystemTable.KEY_DATA_VERSION));
    expected.addColumn(Bytes.toBytes(HBaseSystemTable.VALUE_COLUMN_FAMILY), new byte[0]);
    Result result = new Result(new KeyValue[] {
        new KeyValue(Bytes.toBytes(HBaseSystemTable.KEY_DATA_VERSION),
        Bytes.toBytes(HBaseSystemTable.VALUE_COLUMN_FAMILY), new byte[0], Bytes.toBytes("100")),
    });
    expect(mHtable.get(eqGet(expected))).andReturn(result);
    mHtable.close();

    replay(mHtable);
    HBaseSystemTable systemTable = new HBaseSystemTable(mHtable);
    assertEquals("100", systemTable.getDataVersion());
    systemTable.close();
    verify(mHtable);
  }

  // TODO: KIJI-75 Add tests for the getValue/setValue.
}
