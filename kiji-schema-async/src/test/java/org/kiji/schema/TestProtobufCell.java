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

import org.kiji.proto.test.TestProtocolBuffers.AnotherOne;
import org.kiji.proto.test.TestProtocolBuffers.Record;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.layout.KijiTableLayouts;

/** Test support for protocol buffer encoding and decoding. */
public class TestProtobufCell extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestProtobufCell.class);
  private static final String PROTOBUF_LAYOUT =
      "org/kiji/schema/layout/TestProtobufCell.layout.json";

  private Kiji mKiji;
  private KijiTable mTable;

  @Before
  public void setupTest() throws IOException {
    mKiji = getKiji();
    mKiji.createTable(KijiTableLayouts.getLayout(PROTOBUF_LAYOUT));
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
    final AnotherOne anotherOne = AnotherOne.newBuilder().setId(10).build();

    final KijiTableWriter writer = mTable.getWriterFactory().openTableWriter();
    try {
      final EntityId eid = mTable.getEntityId("row");

      try {
        writer.put(eid, "family", "column", anotherOne);
        Assert.fail("Should not be able to write protobuf test.AnotherOne "
            + "where test.Record is required.");
      } catch (IOException ioe) {
        LOG.info("Expected error: {}", ioe.getMessage());
        Assert.assertTrue(ioe.getMessage().contains(
            "Protocol buffer of class 'org.kiji.proto.test.TestProtocolBuffers$AnotherOne' "
            + "(message name 'test.AnotherOne') "
            + "does not match expected class 'org.kiji.proto.test.TestProtocolBuffers$Record' "
            + "(message name 'test.Record')"));
      }

      writer.put(eid, "family", "column", Record.newBuilder().setId(10).build());
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
      final Record record = row.getMostRecentValue("family", "column");
      Assert.assertEquals(10, record.getId());
      LOG.info("Decoded protocol buffer: {}", record);
    } finally {
      reader.close();
    }
  }
}
