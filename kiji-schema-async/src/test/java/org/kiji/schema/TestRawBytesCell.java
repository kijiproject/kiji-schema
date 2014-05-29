/**
 * (c) Copyright 2013 WibiData, Inc.
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

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.layout.KijiTableLayouts;

/** Test support for raw bytes cell encoding and decoding. */
public class TestRawBytesCell extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestRawBytesCell.class);
  private static final String RAW_BYTES_LAYOUT =
      "org/kiji/schema/layout/TestRawBytesCell.layout.json";

  private Kiji mKiji;
  private KijiTable mTable;

  @Before
  public void setupTest() throws IOException {
    mKiji = getKiji();
    mKiji.createTable(KijiTableLayouts.getLayout(RAW_BYTES_LAYOUT));
    mTable = mKiji.openTable("table");
  }

  @After
  public final void teardownTest() throws IOException {
    mTable.release();
    mTable = null;
    mKiji = null;
  }

  /** Test writing a protocol buffer, then decoding it. */
  @Test
  public void testWriteThenRead() throws Exception {
    final KijiTableWriter writer = mTable.getWriterFactory().openTableWriter();
    try {
      final EntityId eid = mTable.getEntityId("row");
      writer.put(eid, "family", "column", new byte[]{3, 1, 4, 1, 5, 9});
    } finally {
      writer.close();
    }

    final KijiTableReader reader = mTable.getReaderFactory().openTableReader();
    try {
      final EntityId eid = mTable.getEntityId("row");
      final KijiDataRequest dataRequest = KijiDataRequest.builder()
          .addColumns(ColumnsDef.create().addFamily("family"))
          .build();

      final KijiRowData row = reader.get(eid, dataRequest);
      final byte[] bytes = row.getMostRecentValue("family", "column");
      Assert.assertArrayEquals(new byte[]{3, 1, 4, 1, 5, 9}, bytes);
    } finally {
      reader.close();
    }
  }
}
