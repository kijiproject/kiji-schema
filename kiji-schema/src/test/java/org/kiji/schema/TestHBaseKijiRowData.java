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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.Node;
import org.kiji.schema.avro.SchemaStorage;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.impl.AvroCellEncoder;
import org.kiji.schema.impl.hbase.HBaseKijiRowData;
import org.kiji.schema.impl.hbase.HBaseKijiTable;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.KijiColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.impl.LayoutCapsule;
import org.kiji.schema.util.InstanceBuilder;

public class TestHBaseKijiRowData extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseKijiRowData.class);

  /** Test layout. */
  public static final String TEST_LAYOUT_V1 =
      "org/kiji/schema/layout/TestHBaseKijiRowData.test-layout-v1.json";

  /** Update for TEST_LAYOUT, with Test layout with column "family:qual0" removed. */
  public static final String TEST_LAYOUT_V2 =
      "org/kiji/schema/layout/TestHBaseKijiRowData.test-layout-v2.json";

  /** Layout for table 'writer_schema' to test when a column class is not found. */
  public static final String WRITER_SCHEMA_TEST =
      "org/kiji/schema/layout/TestHBaseKijiRowData.writer-schema.json";

  /** Test layout with version layout-1.3. */
  public static final String TEST_LAYOUT_V1_3 =
      "org/kiji/schema/layout/TestHBaseKijiRowData.layout-v1.3.json";

  private static final String TABLE_NAME = "row_data_test_table";

  private byte[] mHBaseFamily;
  private byte[] mHBaseQual0;
  private byte[] mHBaseQual1;
  private byte[] mHBaseQual2;
  private byte[] mHBaseQual3;
  private byte[] mHBaseNodequal0;
  private byte[] mHBaseNodequal1;
  private byte[] mHBaseEmpty;
  private byte[] mHBaseMapFamily;

  private EntityIdFactory mEntityIdFactory;

  /** Cell encoders. */
  private KijiCellEncoder mStringCellEncoder;
  private KijiCellEncoder mIntCellEncoder;
  private KijiCellEncoder mNodeCellEncoder;

  /** HBase KijiTable used for the test (named TABLE_NAME). */
  private HBaseKijiTable mTable;

  private final Node mNode0 = Node.newBuilder().setLabel("node0").build();
  private final Node mNode1 = Node.newBuilder().setLabel("node1").build();

  @Before
  public final void initDecoders() throws Exception {
    final CellSchema stringCellSchema = CellSchema.newBuilder()
        .setStorage(SchemaStorage.HASH)
        .setType(SchemaType.INLINE)
        .setValue("\"string\"")
        .build();
    final CellSpec stringCellSpec = CellSpec.create()
        .setCellSchema(stringCellSchema)
        .setSchemaTable(getKiji().getSchemaTable());
    mStringCellEncoder = new AvroCellEncoder(stringCellSpec);

    final CellSchema intCellSchema = CellSchema.newBuilder()
        .setStorage(SchemaStorage.HASH)
        .setType(SchemaType.INLINE)
        .setValue("\"int\"")
        .build();
    final CellSpec intCellSpec = CellSpec.create()
        .setCellSchema(intCellSchema)
        .setSchemaTable(getKiji().getSchemaTable());
    mIntCellEncoder = new AvroCellEncoder(intCellSpec);

    final CellSchema nodeCellSchema = CellSchema.newBuilder()
        .setStorage(SchemaStorage.HASH)
        .setType(SchemaType.CLASS)
        .setValue(Node.SCHEMA$.getFullName())
        .build();
    final CellSpec nodeCellSpec = CellSpec.create()
        .setCellSchema(nodeCellSchema)
        .setSchemaTable(getKiji().getSchemaTable());
    mNodeCellEncoder = new AvroCellEncoder(nodeCellSpec);
  }

  protected byte[] encodeStr(String str) throws IOException {
    return mStringCellEncoder.encode(str);
  }

  protected byte[] encodeInt(int integer) throws IOException {
    return mIntCellEncoder.encode(integer);
  }

  private byte[] encodeNode(Node node) throws IOException {
    return mNodeCellEncoder.encode(node);
  }

  @Before
  public final void setupTestHBaseKijiRowData() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(TEST_LAYOUT_V1));
    mTable = HBaseKijiTable.downcast(getKiji().openTable(TABLE_NAME));

    final LayoutCapsule capsule = mTable.getLayoutCapsule();
    final KijiColumnNameTranslator translator = capsule.getKijiColumnNameTranslator();
    HBaseColumnName hcolumn =
        translator.toHBaseColumnName(KijiColumnName.create("family", "empty"));
    mHBaseFamily = hcolumn.getFamily();
    mHBaseEmpty = hcolumn.getQualifier();
    mHBaseQual0 = translator.toHBaseColumnName(
        KijiColumnName.create("family:qual0")).getQualifier();
    mHBaseQual1 = translator.toHBaseColumnName(
        KijiColumnName.create("family:qual1")).getQualifier();
    mHBaseQual2 = translator.toHBaseColumnName(
        KijiColumnName.create("family:qual2")).getQualifier();
    mHBaseQual3 = translator.toHBaseColumnName(
        KijiColumnName.create("family:qual3")).getQualifier();
    mHBaseNodequal0 = translator.toHBaseColumnName(KijiColumnName.create("family:nodequal0"))
        .getQualifier();
    mHBaseNodequal1 = translator.toHBaseColumnName(KijiColumnName.create("family:nodequal1"))
        .getQualifier();
    mHBaseMapFamily = translator.toHBaseColumnName(KijiColumnName.create("map")).getFamily();

    mEntityIdFactory = EntityIdFactory.getFactory(capsule.getLayout());
  }

  @After
  public final void teardownTestHBaseKijiRowData() throws Exception {
    mTable.release();
    mTable = null;
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  public void testEntityId() throws IOException {
    final List<KeyValue> kvs = Lists.newArrayList();
    final EntityId foo = mEntityIdFactory.getEntityId("foo");
    kvs.add(new KeyValue(foo.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, Bytes.toBytes("bot")));
    kvs.add(new KeyValue(foo.getHBaseRowKey(), mHBaseFamily, mHBaseQual1, Bytes.toBytes("car")));
    final Result result = new Result(kvs);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().add("family", "qual0"))
        .build();

    final KijiRowData input = new HBaseKijiRowData(mTable, dataRequest, foo, result, null);
    assertEquals(foo, input.getEntityId());
  }

  @Test
  public void testReadInts() throws IOException {
    final List<KeyValue> kvs = Lists.newArrayList();
    final EntityId row0 = mEntityIdFactory.getEntityId("row0");
    final byte[] hbaseRowKey = row0.getHBaseRowKey();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual3, 1L, encodeInt(42)));
    final Result result = new Result(kvs);

    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().addFamily("family");
    KijiDataRequest dataRequest = builder.build();

    HBaseKijiRowData input = new HBaseKijiRowData(mTable, dataRequest, row0, result, null);
    input.getMap();
    final int integer = (Integer) input.getMostRecentValue("family", "qual3");
    assertEquals(42, integer);
  }

  @Test
  public void testGetReaderSchema() throws IOException {
    final Result result = new Result();
    final KijiDataRequest dataRequest = KijiDataRequest.builder().build();

    final KijiRowData input = new HBaseKijiRowData(
        mTable,
        dataRequest,
        RawEntityId.getEntityId(Bytes.toBytes("row-key")),
        result,
        null);

    assertEquals(Schema.create(Schema.Type.STRING), input.getReaderSchema("family", "empty"));
    assertEquals(Schema.create(Schema.Type.INT), input.getReaderSchema("family", "qual3"));
  }

  @Test
  public void testGetReaderSchemaNoSuchColumn() throws IOException {
    final Result result = new Result();
    final KijiDataRequest dataRequest = KijiDataRequest.builder().build();

    final KijiRowData input = new HBaseKijiRowData(
        mTable,
        dataRequest,
        RawEntityId.getEntityId(Bytes.toBytes("row-key")),
        result,
        null);

    try {
      input.getReaderSchema("this_family", "does_not_exist");
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("Table 'row_data_test_table' has no family 'this_family'.", nsce.getMessage());
    }

    try {
      input.getReaderSchema("family", "no_qualifier");
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("Table 'row_data_test_table' has no column 'family:no_qualifier'.",
          nsce.getMessage());
    }
  }

  /** Tests for KijiRowData.getReaderSchema() with layout-1.3 tables. */
  @Test
  public void testGetReaderSchemaLayout13() throws Exception {
    final Kiji kiji = new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(TEST_LAYOUT_V1_3))
        .build();
    final KijiTable table = kiji.openTable("table");
    try {
      final KijiTableReader reader = table.getReaderFactory().openTableReader();
      try {
        final EntityId eid = table.getEntityId("row");
        final KijiDataRequest dataRequest = KijiDataRequest.builder()
            .addColumns(ColumnsDef.create().addFamily("family"))
            .build();
        final KijiRowData row = reader.get(eid, dataRequest);
        assertEquals(
            Schema.Type.STRING,
            row.getReaderSchema("family", "qual0").getType());

      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }

  /**
   * This test was created in response to WIBI-41.  If your KijiDataRequest doesn't contain
   * one of the columns in the Result map, you used to a get a NullPointerException.
   */
  @Test
  public void testGetMap() throws IOException {
    final List<KeyValue> kvs = Lists.newArrayList();
    final EntityId eid = mEntityIdFactory.getEntityId("foo");
    kvs.add(new KeyValue(eid.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, Bytes.toBytes("bot")));
    kvs.add(new KeyValue(eid.getHBaseRowKey(), mHBaseFamily, mHBaseEmpty, Bytes.toBytes("car")));
    final Result result = new Result(kvs);

    final KijiDataRequest dataRequest = KijiDataRequest.builder().build();
    // We didn't request any data, so the map should be null.
    final HBaseKijiRowData input = new HBaseKijiRowData(
        mTable,
        dataRequest,
        eid,
        result,
        null);
    assertTrue(input.getMap().isEmpty());
  }

  @Test
  public void testReadWithMaxVersions() throws IOException {
    final List<KeyValue> kvs = Lists.newArrayList();
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    final byte[] hbaseRowKey = eid.getHBaseRowKey();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0, 3L, encodeStr("apple")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0, 2L, encodeStr("banana")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0, 1L, encodeStr("carrot")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1, 6L, encodeStr("antelope")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1, 5L, encodeStr("bear")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1, 4L, encodeStr("cat")));
    final Result result = new Result(kvs);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(1).add("family", "qual0"))
        .addColumns(ColumnsDef.create().withMaxVersions(2).add("family", "qual1"))
        .build();

    final KijiRowData input = new HBaseKijiRowData(mTable, dataRequest, eid, result, null);
    assertEquals(1, input.getValues("family", "qual0").size());
    assertEquals("apple", input.getMostRecentValue("family",  "qual0").toString());
    assertEquals(2, input.getValues("family", "qual1").size());
    assertEquals("antelope", input.getValues("family", "qual1").get(6L).toString());
    assertEquals("bear", input.getValues("family", "qual1").get(5L).toString());
  }

  @Test
  public void testTypedReadWithMaxVersions() throws IOException {
    final List<KeyValue> kvs = Lists.newArrayList();
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    final byte[] hbaseRowKey = eid.getHBaseRowKey();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0, 3L, encodeStr("apple")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0, 2L, encodeStr("banana")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0, 1L, encodeStr("carrot")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1, 6L, Bytes.toBytes("antelope")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1, 5L, Bytes.toBytes("bear")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1, 4L, Bytes.toBytes("cat")));
    final Result result = new Result(kvs);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(1).add("family", "qual0"))
        .addColumns(ColumnsDef.create().withMaxVersions(2).add("family", "qual1"))
        .build();
    final KijiRowData input = new HBaseKijiRowData(mTable, dataRequest, eid, result, null);
    assertEquals(1, input.getValues("family", "qual0").size());
    final NavigableMap<Long, CharSequence> typedValues = input.getValues("family", "qual0");
    assertEquals("apple", typedValues.get(3L).toString());
    assertEquals(2, input.getTimestamps("family", "qual1").size());
  }

  @Test
  public void testReadWithTimeRange() throws IOException {
    final List<KeyValue> kvs = Lists.newArrayList();
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    final byte[] hbaseRowKey = eid.getHBaseRowKey();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0, 3L, encodeStr("apple")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0, 2L, encodeStr("banana")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0, 1L, encodeStr("carrot")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1, 5L, encodeStr("bear")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1, 4L, encodeStr("cat")));
    final Result result = new Result(kvs);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .withTimeRange(2L, 6L)
        .addColumns(ColumnsDef.create().withMaxVersions(1).add("family", "qual0"))
        .addColumns(ColumnsDef.create().withMaxVersions(2).add("family", "qual1"))
        .build();
    final KijiRowData input = new HBaseKijiRowData(mTable, dataRequest, eid, result, null);
    assertEquals(1, input.getTimestamps("family", "qual0").size());
    assertEquals("apple", input.getMostRecentValue("family", "qual0").toString());
    assertEquals(2, input.getTimestamps("family", "qual1").size());
    assertEquals("bear", input.getMostRecentValue("family", "qual1").toString());
    assertEquals("cat", input.getValue("family", "qual1", 4L).toString());
  }

  /**
   * Logs the content of an HBaseKijiRowData (debug log-level).
   *
   * @param row HBaseKijiRowData to dump.
   */
  private static void logDebugRow(HBaseKijiRowData row) {
    if (!LOG.isDebugEnabled()) {
      return;
    }
    LOG.debug("Dumping content of HBaseKijiRowData {}", row);
    for (String family : row.getMap().keySet()) {
      for (String qualifier : row.getMap().get(family).keySet()) {
        for (Map.Entry<Long, byte[]> entry : row.getMap().get(family).get(qualifier).entrySet()) {
          final long timestamp = entry.getKey();
          final byte[] bytes = entry.getValue();
          LOG.debug("\tcolumn={}:{}\ttimestamp={}\tvalue={}",
              family, qualifier, timestamp, Bytes.toStringBinary(bytes));
        }
      }
    }
  }

  @Test
  public void testReadColumnTypes() throws IOException {
    final List<KeyValue> kvs = Lists.newArrayList();
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    kvs.add(new KeyValue(eid.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, encodeStr("value")));
    final Result result = new Result(kvs);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(1).add("family", "qual0"))
        .build();
    final HBaseKijiRowData input = new HBaseKijiRowData(mTable, dataRequest, eid, result, null);
    logDebugRow(input);

    assertFalse(input.containsColumn("not-a-family"));
    assertTrue(input.containsColumn("family"));
    assertTrue(input.containsColumn("family", "qual0"));
    assertEquals("value", input.getMostRecentValue("family", "qual0").toString());
    assertEquals("value", input.getMostRecentValue("family", "qual0").toString());
  }

  @Test
  public void testReadFamilyTypes() throws IOException {
    final List<KeyValue> kvs = Lists.newArrayList();
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    final byte[] hbaseRowKey = eid.getHBaseRowKey();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0, encodeStr("value0")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1, encodeStr("value1")));
    final Result result = new Result(kvs);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().add("family", "qual0"))
        .addColumns(ColumnsDef.create().add("family", "qual1"))
        .build();
    final KijiRowData input = new HBaseKijiRowData(mTable, dataRequest, eid, result, null);
    assertTrue(input.containsColumn("family", "qual0"));
    assertEquals("value0", input.getMostRecentValue("family", "qual0").toString());
    assertEquals("value0", input.getMostRecentValue("family", "qual0").toString());
    assertTrue(input.containsColumn("family", "qual1"));
    assertEquals("value1", input.getMostRecentValue("family", "qual1").toString());
    assertEquals("value1", input.getMostRecentValue("family", "qual1").toString());
  }

  @Test
  public void testReadMapFamilyTypes() throws IOException {
    final List<KeyValue> kvs = Lists.newArrayList();
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    final byte[] hbaseRowKey = eid.getHBaseRowKey();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseMapFamily, encodeStr("key0"), encodeStr("value0")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseMapFamily, encodeStr("key1"), encodeStr("value1")));
    final Result result = new Result(kvs);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().addFamily("map"))
        .build();
    final KijiRowData input = new HBaseKijiRowData(mTable, dataRequest, eid, result, null);
    final NavigableMap<String, NavigableMap<Long, CharSequence>> stringsByTime =
        input.getValues("map");

    // FIXME: Testing by logging?
    for (Map.Entry<String, NavigableMap<Long, CharSequence>> qualToOtherMap
        : stringsByTime.entrySet()) {
      LOG.debug("Qualifiers found: {}", qualToOtherMap.getKey());
    }
  }

  @Test
  public void testReadSpecificFamilyTypes() throws IOException {
    final List<KeyValue> kvs = Lists.newArrayList();
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    final byte[] hbaseRowKey = eid.getHBaseRowKey();

    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal0, encodeNode(mNode0)));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal1, encodeNode(mNode1)));
    final Result result = new Result(kvs);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(1).addFamily("family"))
        .build();
    final KijiRowData input = new HBaseKijiRowData(mTable, dataRequest, eid, result, null);
    assertTrue(input.containsColumn("family", "nodequal0"));
    assertTrue(input.containsColumn("family", "nodequal1"));

    final Node value0 = input.getMostRecentValue("family", "nodequal0");
    assertEquals("node0", value0.getLabel());
    final Node value1 = input.getMostRecentValue("family", "nodequal1");
    assertEquals("node1", value1.getLabel());
  }

  @Test
  public void testReadSpecificTimestampTypes() throws IOException {
    final List<KeyValue> kvs = Lists.newArrayList();
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    final byte[] hbaseRowKey = eid.getHBaseRowKey();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal0, 100L, encodeNode(mNode0)));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal0, 200L, encodeNode(mNode1)));
    final Result result = new Result(kvs);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(
            ColumnsDef.create().withMaxVersions(Integer.MAX_VALUE).add("family", "nodequal0"))
        .build();
    final KijiRowData input = new HBaseKijiRowData(mTable, dataRequest, eid, result, null);
    assertTrue(input.containsColumn("family", "nodequal0"));
    final NavigableMap<Long, Node> values = input.getValues("family", "nodequal0");
    assertNotNull(values);
    assertEquals(2, values.size());
    assertEquals("node0", values.get(100L).getLabel());
    assertEquals("node1", values.get(200L).getLabel());

    // Make sure they come in reverse chronological order.
    final Iterator<NavigableMap.Entry<Long, Node>> iter = values.entrySet().iterator();
    assertTrue(iter.hasNext());
    assertEquals(200L, iter.next().getKey().longValue());
    assertEquals(100L, iter.next().getKey().longValue());
    assertFalse(iter.hasNext());
  }

  @Test
  public void testReadWithTimestamp() throws IOException {
    final List<KeyValue> kvs = Lists.newArrayList();
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    final byte[] hbaseRowKey = eid.getHBaseRowKey();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal0, 100L, encodeNode(mNode0)));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal0, 200L, encodeNode(mNode1)));
    final Result result = new Result(kvs);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(
            ColumnsDef.create().withMaxVersions(Integer.MAX_VALUE).add("family", "nodequal0"))
        .build();
    final KijiRowData input = new HBaseKijiRowData(mTable, dataRequest, eid, result, null);
    assertTrue(input.containsColumn("family", "nodequal0"));
    assertEquals(
        "node0",
        ((Node) input.getValue("family", "nodequal0", 100L)).getLabel().toString());
    assertEquals(
        "node1",
        ((Node) input.getValue("family", "nodequal0", 200L)).getLabel().toString());
  }

  @Test
  public void testReadSpecificTypes() throws IOException {
    final List<KeyValue> kvs = Lists.newArrayList();
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    final byte[] hbaseRowKey = eid.getHBaseRowKey();
    final Node node = Node.newBuilder().setLabel("foo").build();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal0, encodeNode(node)));
    final Result result = new Result(kvs);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().add("family", "nodequal0"))
        .build();
    final KijiRowData input = new HBaseKijiRowData(mTable, dataRequest, eid, result, null);
    assertTrue(input.containsColumn("family", "nodequal0"));
    final Node actual = input.getMostRecentValue("family", "nodequal0");
    assertEquals("foo", actual.getLabel());
  }

  @Test
  public void testContainsColumn() throws Exception {
    final String family = "family";
    final String qualifier = "qual0";
    final long timestamp = 1L;
    final Kiji kiji = new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(TEST_LAYOUT_V1))
            .withRow("row1")
                .withFamily(family).withQualifier(qualifier).withValue(timestamp, "foo1")
        .build();
    final KijiTable table = kiji.openTable(TABLE_NAME);
    try {
      final KijiTableReader reader = table.openTableReader();
      try {
        final KijiRowData row1 = reader.get(table.getEntityId("row1"),
            KijiDataRequest.create(family, qualifier));
        assertTrue(row1.containsCell(family, qualifier, timestamp));
        assertFalse(row1.containsCell(family, qualifier, 2L));
        assertFalse(row1.containsCell("blope", qualifier, timestamp));
        assertFalse(row1.containsCell(family, "blope", timestamp));
      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }

  @Test
  public void testIterator() throws IOException {
    final List<KeyValue> kvs = Lists.newArrayList();
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    kvs.add(new KeyValue(eid.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, encodeStr("value0")));
    kvs.add(new KeyValue(eid.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, encodeStr("value1")));
    kvs.add(new KeyValue(eid.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, encodeStr("value2")));
    final Result result = new Result(kvs);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(3).add("family", "qual0"))
        .build();
    final HBaseKijiRowData input = new HBaseKijiRowData(mTable, dataRequest, eid, result, null);
    logDebugRow(input);

    assertFalse(input.containsColumn("not-a-family"));
    assertTrue(input.containsColumn("family"));
    assertTrue(input.containsColumn("family", "qual0"));
    final Iterator<KijiCell<CharSequence>> cells = input.<CharSequence>iterator("family", "qual0");
    assertTrue(cells.hasNext());
    assertEquals("value0", cells.next().getData().toString());
    assertTrue(cells.hasNext());
    assertEquals("value1", cells.next().getData().toString());
    assertTrue(cells.hasNext());
    assertEquals("value2", cells.next().getData().toString());
    assertFalse(cells.hasNext());

    try {
      input.iterator("unknown_family");
      fail("HBaseKijiRowData.iterator() should fail on columns with no data request.");
    } catch (IllegalArgumentException iae) {
      LOG.info("Expected error: {}", iae.getMessage());
      assertTrue(iae.getMessage().contains("Column unknown_family has no data request."));
    }

    try {
      input.iterator("family", "qual1");
      fail("HBaseKijiRowData.iterator() should fail on columns with no data request.");
    } catch (IllegalArgumentException iae) {
      LOG.info("Expected error: {}", iae.getMessage());
      assertTrue(iae.getMessage().contains("Column family:qual1 has no data request."));
    }
  }

  @Test
  public void testIteratorMapFamilyTypes() throws IOException {
    new InstanceBuilder(getKiji())
        .withTable(mTable)
            .withRow("row1")
              .withFamily("map")
                  .withQualifier("key0").withValue(1L, 0)
                  .withQualifier("key1").withValue(1L, 1)
                  .withQualifier("key2").withValue(1L, 2)
              .withFamily("family")
                  .withQualifier("qual0").withValue(1L, "string1")
                  .withQualifier("qual0").withValue(2L, "string2")
        .build();

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(1).addFamily("map"))
        .build();

    final KijiTableReader reader = mTable.openTableReader();
    try {
      final KijiRowData row1 = reader.get(mTable.getEntityId("row1"), dataRequest);
      final Iterator<KijiCell<Integer>> cells = row1.iterator("map");
      assertTrue(cells.hasNext());
      final KijiCell<?> cell0 = cells.next();
      assertEquals("Wrong first cell!", "key0", cell0.getQualifier());
      assertTrue(cells.hasNext());
      final KijiCell<?> cell1 = cells.next();
      assertEquals("Wrong second cell!", "key1", cell1.getQualifier());
      assertTrue(cells.hasNext());
      final KijiCell<?> cell2 = cells.next();
      assertEquals("Wrong third cell!", "key2", cell2.getQualifier());
      assertFalse(cells.hasNext());

      final Iterator<KijiCell<Integer>> cellsKey1 = row1.iterator("map", "key1");
      assertTrue(cellsKey1.hasNext());
      final KijiCell<Integer> key1Cell = cellsKey1.next();
      assertEquals("key1", key1Cell.getQualifier());
      assertEquals(1L, key1Cell.getTimestamp());
      assertEquals((Integer) 1, key1Cell.getData());
      assertFalse(cellsKey1.hasNext());

    } finally {
      reader.close();
    }
  }

  @Test
  public void testIteratorMaxVersion() throws IOException {
    final List<KeyValue> kvs = Lists.newArrayList();
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    kvs.add(new KeyValue(eid.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, encodeStr("value0")));
    kvs.add(new KeyValue(eid.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, encodeStr("value1")));
    kvs.add(new KeyValue(eid.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, encodeStr("value2")));
    final Result result = new Result(kvs);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(2).add("family", "qual0"))
        .build();
    final HBaseKijiRowData input = new HBaseKijiRowData(mTable, dataRequest, eid, result, null);

    assertFalse(input.containsColumn("not-a-family"));
    assertTrue(input.containsColumn("family"));
    assertTrue(input.containsColumn("family", "qual0"));
    final Iterator<KijiCell<CharSequence>> cells = input.<CharSequence>iterator("family", "qual0");
    assertTrue(cells.hasNext());
    assertEquals("value0", cells.next().getData().toString());
    assertTrue(cells.hasNext());
    assertEquals("value1", cells.next().getData().toString());
    assertFalse(cells.hasNext());
  }

  @Test
  public void testIteratorMapFamilyMaxVersionsTypes() throws IOException {
    new InstanceBuilder(getKiji())
        .withTable(mTable)
            .withRow("row1")
                .withFamily("map")
                  .withQualifier("key0")
                    .withValue(1L, 0)
                    .withValue(2L, 1)
                    .withValue(3L, 2)
        .build();

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(2).addFamily("map"))
        .build();

    final KijiTableReader reader = mTable.openTableReader();
    try {
      final KijiRowData row1 = reader.get(mTable.getEntityId("row1"), dataRequest);
      final Iterator<KijiCell<Integer>> cells = row1.iterator("map");
      assertTrue(cells.hasNext());
      final KijiCell<Integer> cell0 = cells.next();
      assertEquals("Wrong first cell!", 2, cell0.getData().intValue());
      assertTrue(cells.hasNext());
      final KijiCell<Integer> cell1 = cells.next();
      assertEquals("Wrong second cell!", 1, cell1.getData().intValue());
      assertFalse(cells.hasNext());
    } finally {
      reader.close();
    }
  }

  @Test
  public void testMapAsIterable() throws IOException {
    new InstanceBuilder(getKiji())
        .withTable(mTable)
            .withRow("row1")
              .withFamily("map")
                  .withQualifier("key0").withValue(1L, 0)
                  .withQualifier("key1").withValue(1L, 1)
                  .withQualifier("key2").withValue(1L, 2)
              .withFamily("family")
                  .withQualifier("qual0").withValue(1L, "string1")
                  .withQualifier("qual0").withValue(2L, "string2")
        .build();

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(3).addFamily("map"))
        .build();

    final KijiTableReader reader = mTable.openTableReader();
    try {
      final KijiRowData row1 = reader.get(mTable.getEntityId("row1"), dataRequest);
      final List<KijiCell<Integer>> cells = Lists.newArrayList(row1.<Integer>asIterable("map"));
      final int cellCount = cells.size();
      assertEquals("Wrong number of cells returned by asIterable.", 3, cellCount);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testGroupAsIterable() throws IOException {
    final List<KeyValue> kvs = Lists.newArrayList();
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    kvs.add(new KeyValue(eid.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, encodeStr("value0")));
    kvs.add(new KeyValue(eid.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, encodeStr("value1")));
    kvs.add(new KeyValue(eid.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, encodeStr("value2")));
    final Result result = new Result(kvs);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(3).add("family", "qual0"))
        .build();

    final HBaseKijiRowData input = new HBaseKijiRowData(mTable, dataRequest, eid, result, null);
    logDebugRow(input);

    assertFalse(input.containsColumn("not-a-family"));
    assertTrue(input.containsColumn("family"));
    assertTrue(input.containsColumn("family", "qual0"));
    final List<KijiCell<CharSequence>> cells =
        Lists.newArrayList(input.<CharSequence>asIterable("family", "qual0"));
    final int cellCount = cells.size();
    assertEquals("Wrong number of cells returned by asIterable.", 3, cellCount);
  }

  @Test
  public void testReadMiddleTimestamp() throws IOException {
    // Test that we can select a timestamped value that is not the most recent value.
    new InstanceBuilder(getKiji())
        .withTable(mTable)
            .withRow("row1")
                .withFamily("family")
                    .withQualifier("qual0")
                        .withValue(4L, "oldest")
                        .withValue(6L, "middle")
                        .withValue(8L, "newest")
                    .withQualifier("qual1")
                        .withValue(1L, "one")
                        .withValue(2L, "two")
                        .withValue(3L, "three")
                        .withValue(4L, "four")
                        .withValue(8L, "eight")
                    .withQualifier("qual2")
                        .withValue(3L, "q2-three")
                        .withValue(4L, "q2-four")
                        .withValue(6L, "q2-six")
        .build();

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .withTimeRange(2L, 7L)
        .addColumns(ColumnsDef.create().add("family", "qual0"))
        .addColumns(ColumnsDef.create().withMaxVersions(2).add("family", "qual1"))
        .addColumns(ColumnsDef.create().withMaxVersions(3).add("family", "qual2"))
        .build();

    final KijiTableReader reader = mTable.openTableReader();
    try {
      final KijiRowData row1 = reader.get(mTable.getEntityId("row1"), dataRequest);

      // This should be "middle" based on the time range of the data request.
      final String qual0val = row1.getMostRecentValue("family", "qual0").toString();
      assertEquals("Didn't get the middle value for family:qual0", "middle", qual0val);

      // We always optimize maxVersions=1 to actually return exactly 1 value, even of
      // we requested more versions of other columns.
      final NavigableMap<Long, CharSequence> q0vals = row1.getValues("family", "qual0");
      assertEquals("qual0 should only return one thing", 1, q0vals.size());
      assertEquals("Newest (only) value in q0 should be 'middle'.",
          "middle", q0vals.firstEntry().getValue().toString());

      // qual1 should see at least two versions, but no newer than 7L.
      final NavigableMap<Long, CharSequence> q1vals = row1.getValues("family", "qual1");
      assertEquals("qual1 getValues should have exactly two items", 2, q1vals.size());
      assertEquals("Newest value in q1 should be 'four'.",
          "four", q1vals.firstEntry().getValue().toString());

      // qual2 should see exactly three versions.
      final NavigableMap<Long, CharSequence> q2vals = row1.getValues("family", "qual2");
      assertEquals("qual2 getValues should have exactly three items", 3, q2vals.size());
      assertEquals("Newest value in q2 should be 'q2-six'.",
          "q2-six", q2vals.firstEntry().getValue().toString());
    } finally {
      reader.close();
    }
  }

  @Test
  public void testEmptyResult() throws IOException {
    new InstanceBuilder(getKiji())
        .withTable(mTable)
            .withRow("row1")
              .withFamily("family")
                  .withQualifier("qual0").withValue(1L, "string1")
                  .withQualifier("qual0").withValue(2L, "string2")
        .build();

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().add("family", "qual1"))
        .build();

    final KijiTableReader reader = mTable.openTableReader();
    try {
      final KijiRowData row1 = reader.get(mTable.getEntityId("row1"), dataRequest);

      final NavigableMap<Long, CharSequence> values = row1.getValues("family", "qual1");
      assertTrue("getValues should return an empty map for empty rowdata.", values.isEmpty());

      final NavigableMap<Long, KijiCell<CharSequence>> cells = row1.getCells("family", "qual1");
      assertTrue("getCells should return an empty map for empty rowdata.", cells.isEmpty());

      final Iterator<KijiCell<CharSequence>> iterator =  row1.iterator("family", "qual1");
      assertFalse("iterator obtained on a column the rowdata has no data for should return false"
          + "when hasNext is called.",
          iterator.hasNext());

      final CharSequence value = row1.getMostRecentValue("family", "qual1");
      assertEquals("getMostRecentValue should return a null value from an empty rowdata.",
          null,
          value);

      final KijiCell<CharSequence> cell = row1.getMostRecentCell("family", "qual1");
      assertEquals("getMostRecentCell should return a null cell from empty rowdata.",
          null,
          cell);

    } finally {
      reader.close();
    }
  }

  /** Tests that reading an entire family with a column that has been deleted works. */
  @Test
  public void testReadDeletedColumns() throws Exception {
    final Kiji kiji = getKiji();
    new InstanceBuilder(kiji)
        .withTable(mTable)
            .withRow("row1")
              .withFamily("family")
                  .withQualifier("qual0").withValue(1L, "string1")
                  .withQualifier("qual0").withValue(2L, "string2")
        .build();

    final TableLayoutDesc update = KijiTableLayouts.getLayout(TEST_LAYOUT_V2);
    update.setReferenceLayout(mTable.getLayout().getDesc().getLayoutId());
    kiji.modifyTableLayout(update);

    mTable.release();
    mTable = (HBaseKijiTable) kiji.openTable(TABLE_NAME);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().addFamily("family"))
        .build();

    final KijiTableReader reader = mTable.openTableReader();
    try {
      final KijiRowData row1 = reader.get(mTable.getEntityId("row1"), dataRequest);
      assertTrue(row1.getValues("family", "qual0").isEmpty());

    } finally {
      reader.close();
    }
  }

  /**
   * Tests that we can read a record using the writer schema.
   * This tests the case when a specific record class is not found on the classpath.
   * However, this behavior is bogus. The reader schema should not be tied to the classes
   * available on the classpath.
   *
   * TODO(SCHEMA-295) the user may force using the writer schemas by overriding the
   *     declared reader schemas. This test will be updated accordingly.
   */
  @Test
  public void testWriterSchemaWhenSpecificRecordClassNotFound() throws Exception {
    final Kiji kiji = getKiji();  // not owned
    kiji.createTable(KijiTableLayouts.getLayout(WRITER_SCHEMA_TEST));
    final KijiTable table = kiji.openTable("writer_schema");
    try {
      // Write a (generic) record:
      final Schema writerSchema = Schema.createRecord("Found", null, "class.not", false);
      writerSchema.setFields(Lists.newArrayList(
          new Field("field", Schema.create(Schema.Type.STRING), null, null)));

      final KijiTableWriter writer = table.openTableWriter();
      try {
        final GenericData.Record record = new GenericRecordBuilder(writerSchema)
            .set("field", "value")
            .build();
        writer.put(table.getEntityId("eid"), "family", "qualifier", 1L, record);

      } finally {
        writer.close();
      }

      // Read the record back (should be a generic record):
      final KijiTableReader reader = table.openTableReader();
      try {
        final KijiDataRequest dataRequest = KijiDataRequest.builder()
            .addColumns(ColumnsDef.create().add("family", "qualifier"))
            .build();
        final KijiRowData row = reader.get(table.getEntityId("eid"), dataRequest);
        final GenericData.Record record = row.getValue("family", "qualifier", 1L);
        assertEquals(writerSchema, record.getSchema());
        assertEquals("value", record.get("field").toString());
      } finally {
        reader.close();
      }

    } finally {
      table.release();
    }
  }

}
