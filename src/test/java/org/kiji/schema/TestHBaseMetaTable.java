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

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.impl.HBaseMetaTable;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayoutDatabase;
import org.kiji.schema.layout.KijiTableLayouts;


public class TestHBaseMetaTable {
  private HTableInterface mHTable;
  private KijiTableLayoutDatabase mTableLayoutDatabase;
  private KijiTableKeyValueDatabase mTableKeyValueDatabase;

  @Before
  public void setup() throws IOException {
    mHTable = createMock(HTableInterface.class);
    mTableLayoutDatabase = createMock(KijiTableLayoutDatabase.class);
    mTableKeyValueDatabase = createMock(KijiTableKeyValueDatabase.class);
  }

  @Test
  public void testLayouts() throws Exception {
    final TableLayoutDesc desc = KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE);
    final KijiTableLayout layout = new KijiTableLayout(desc, null);

    expect(mTableLayoutDatabase.updateTableLayout("table", desc)).andReturn(layout);
    expect(mTableLayoutDatabase.getTableLayout("table")).andReturn(layout);
    mHTable.close();

    replay(mHTable);
    replay(mTableLayoutDatabase);

    HBaseMetaTable table = new HBaseMetaTable(mHTable, mTableLayoutDatabase,
        mTableKeyValueDatabase);

    assertEquals(layout, table.updateTableLayout("table", desc));
    assertEquals(layout, table.getTableLayout("table"));
    table.close();

    verify(mHTable);
    verify(mTableLayoutDatabase);
  }


  //test keySet, getValue, putValue, removeValues, getTableNameToKeysMap.
}
