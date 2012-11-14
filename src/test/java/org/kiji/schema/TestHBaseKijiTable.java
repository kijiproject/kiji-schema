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
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.impl.HTableFactory;
import org.kiji.schema.layout.KijiTableLayouts;


public class TestHBaseKijiTable extends KijiClientTest {
  private HBaseKijiTable mKijiTable;
  private HTable mHTable;

  @Before
  public void setup() throws Exception {
    getKiji().getMetaTable()
        .updateTableLayout("table", KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE));

    // Create a mock HTable instance.
    mHTable = createMock(HTable.class);
    mHTable.close();  // <-- Expect it to be closed eventually.
    replay(mHTable);

    mKijiTable = new HBaseKijiTable(getKiji(), "table", new HTableFactory() {
      @Override
      public HTable create(Configuration conf, String hbaseTableName) throws IOException {
        return mHTable;
      }
    });
  }

  @After
  public void teardown() throws IOException {
    IOUtils.closeQuietly(mKijiTable);
    if (mHTable != null) {
      verify(mHTable);
    }
  }

  @Test(expected=NullPointerException.class)
  public void testNullEntityId() throws IOException {
    String s = null;
    mKijiTable.getEntityId(s);
  }
}
