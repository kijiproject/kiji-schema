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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.avro.Schema;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.Node;
import org.kiji.schema.avro.SchemaStorage;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.impl.AvroCellEncoder;
import org.kiji.schema.impl.HBaseKijiRowData;
import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.impl.ColumnNameTranslator;
import org.kiji.schema.util.InstanceBuilder;

public class TestHBaseKijiRowData extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseKijiRowData.class);

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

  /** A kiji cell decoder factory. */
  private KijiCellDecoderFactory mCellDecoderFactory = SpecificCellDecoderFactory.get();

  /** Cell encoders. */
  private KijiCellEncoder mStringCellEncoder;
  private KijiCellEncoder mIntCellEncoder;
  private KijiCellEncoder mNodeCellEncoder;

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
  public final void setupInstance() throws Exception {
    final KijiTableLayout tableLayout =
        KijiTableLayouts.getTableLayout(KijiTableLayouts.ROW_DATA_TEST);
    getKiji().createTable(tableLayout.getDesc());

    ColumnNameTranslator translator = new ColumnNameTranslator(tableLayout);
    HBaseColumnName hcolumn = translator.toHBaseColumnName(new KijiColumnName("family", "empty"));
    mHBaseFamily = hcolumn.getFamily();
    mHBaseEmpty = hcolumn.getQualifier();
    mHBaseQual0 = translator.toHBaseColumnName(new KijiColumnName("family:qual0")).getQualifier();
    mHBaseQual1 = translator.toHBaseColumnName(new KijiColumnName("family:qual1")).getQualifier();
    mHBaseQual2 = translator.toHBaseColumnName(new KijiColumnName("family:qual2")).getQualifier();
    mHBaseQual3 = translator.toHBaseColumnName(new KijiColumnName("family:qual3")).getQualifier();
    mHBaseNodequal0 = translator.toHBaseColumnName(new KijiColumnName("family:nodequal0"))
        .getQualifier();
    mHBaseNodequal1 = translator.toHBaseColumnName(new KijiColumnName("family:nodequal1"))
        .getQualifier();
    mHBaseMapFamily = translator.toHBaseColumnName(new KijiColumnName("map")).getFamily();

    mEntityIdFactory = EntityIdFactory.getFactory(tableLayout);
  }

  @Test
  public void testEntityId() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId foo = mEntityIdFactory.getEntityId("foo");
    kvs.add(new KeyValue(foo.getHBaseRowKey(), mHBaseFamily, mHBaseQual0,
            Bytes.toBytes("bot")));
    kvs.add(new KeyValue(foo.getHBaseRowKey(), mHBaseFamily, mHBaseQual1,
            Bytes.toBytes("car")));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("family", "qual0");
    KijiDataRequest dataRequest = builder.build();
    final KijiTable table = getKiji().openTable(tableLayout.getName());
    try {
      HBaseKijiTable hKijiTable = HBaseKijiTable.downcast(table);
      KijiRowData input = new HBaseKijiRowData(foo, dataRequest, hKijiTable, result);
      assertEquals(foo, input.getEntityId());
    } finally {
      table.release();
    }
  }

  @Test
  public void testReadInts() throws IOException {
    LOG.info("start testReadInts");
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.getEntityId("row0");
    byte[] hbaseRowKey = row0.getHBaseRowKey();

    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual3, 1L, encodeInt(42)));

    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().addFamily("family");
    KijiDataRequest dataRequest = builder.build();
    final KijiTable table = getKiji().openTable(tableLayout.getName());
    try {
      HBaseKijiTable hKijiTable = HBaseKijiTable.downcast(table);
      HBaseKijiRowData input = new HBaseKijiRowData(row0, dataRequest, hKijiTable, result);
      input.getMap();
      final int integer = (Integer) input.getMostRecentValue("family", "qual3");
      assertEquals(42, integer);
      LOG.info("stop testReadInts");
    } finally {
      table.release();
    }
  }

  @Test
  public void testGetReaderSchema() throws IOException {
    Result result = new Result();
    KijiDataRequest dataRequest = KijiDataRequest.builder().build();
    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");

    KijiRowData input = new HBaseKijiRowData(
        RawEntityId.getEntityId(Bytes.toBytes("row-key")), dataRequest,
        mCellDecoderFactory, tableLayout, result, getKiji().getSchemaTable());

    assertEquals(Schema.create(Schema.Type.STRING), input.getReaderSchema("family", "empty"));
    assertEquals(Schema.create(Schema.Type.INT), input.getReaderSchema("family", "qual3"));
  }

  @Test(expected=NoSuchColumnException.class)
  public void testGetReaderSchemaNoSuchColumn() throws IOException {
    Result result = new Result();
    KijiDataRequest dataRequest = KijiDataRequest.builder().build();
    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");

    KijiRowData input = new HBaseKijiRowData(
        RawEntityId.getEntityId(Bytes.toBytes("row-key")), dataRequest,
        mCellDecoderFactory, tableLayout, result, getKiji().getSchemaTable());

    input.getReaderSchema("this_family", "does_not_exist");
  }

  /**
   * This test was created in response to WIBI-41.  If your KijiDataRequest doesn't contain
   * one of the columns in the Result map, you used to a get a NullPointerException.
   */
  @Test
  public void testGetMap() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId foo = mEntityIdFactory.getEntityId("foo");
    kvs.add(new KeyValue(foo.getHBaseRowKey(), mHBaseFamily, mHBaseQual0,
            Bytes.toBytes("bot")));
    kvs.add(new KeyValue(foo.getHBaseRowKey(), mHBaseFamily, mHBaseEmpty,
            Bytes.toBytes("car")));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequest dataRequest = KijiDataRequest.builder().build();
    // We didn't request any data, so the map should be null.
    HBaseKijiRowData input = new HBaseKijiRowData(dataRequest, mCellDecoderFactory,
        tableLayout, result, getKiji().getSchemaTable());
    assertTrue(input.getMap().isEmpty());
  }

  @Test
  public void testReadWithMaxVersions() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.getEntityId("row0");
    byte[] hbaseRowKey = row0.getHBaseRowKey();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0,
            3L, encodeStr("apple")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0,
            2L, encodeStr("banana")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0,
            1L, encodeStr("carrot")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1,
            6L, encodeStr("antelope")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1,
            5L, encodeStr("bear")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1,
            4L, encodeStr("cat")));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(1).add("family", "qual0");
    builder.newColumnsDef().withMaxVersions(2).add("family", "qual1");
    KijiDataRequest dataRequest = builder.build();
    KijiRowData input = new HBaseKijiRowData(dataRequest, mCellDecoderFactory,
        tableLayout, result, getKiji().getSchemaTable());
    assertEquals(1, input.getValues("family", "qual0").size());
    assertEquals("apple", input.getMostRecentValue("family",  "qual0").toString());
    assertEquals(2, input.getValues("family", "qual1").size());
    assertEquals("antelope", input.getValues("family", "qual1").get(6L).toString());
    assertEquals("bear", input.getValues("family", "qual1").get(5L).toString());
  }

  @Test
  public void testTypedReadWithMaxVersions() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.getEntityId("row0");
    byte[] hbaseRowKey = row0.getHBaseRowKey();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0, 3L, encodeStr("apple")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0, 2L, encodeStr("banana")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0, 1L, encodeStr("carrot")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1, 6L, Bytes.toBytes("antelope")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1, 5L, Bytes.toBytes("bear")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1, 4L, Bytes.toBytes("cat")));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(1).add("family", "qual0");
    builder.newColumnsDef().withMaxVersions(2).add("family", "qual1");
    KijiDataRequest dataRequest = builder.build();
    KijiRowData input = new HBaseKijiRowData(dataRequest, mCellDecoderFactory,
        tableLayout, result, getKiji().getSchemaTable());
    assertEquals(1, input.getValues("family", "qual0").size());
    NavigableMap<Long, CharSequence> typedValues = input.getValues("family", "qual0");
    assertEquals("apple", typedValues.get(3L).toString());
    assertEquals(2, input.getTimestamps("family", "qual1").size());
  }

  @Test
  public void testReadWithTimeRange() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.getEntityId("row0");
    byte[] hbaseRowKey = row0.getHBaseRowKey();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0,
            3L, encodeStr("apple")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0,
            2L, encodeStr("banana")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0,
            1L, encodeStr("carrot")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1,
            5L, encodeStr("bear")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1,
            4L, encodeStr("cat")));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.withTimeRange(2L, 6L);
    builder.newColumnsDef().withMaxVersions(1).add("family", "qual0");
    builder.newColumnsDef().withMaxVersions(2).add("family", "qual1");
    KijiDataRequest dataRequest = builder.build();
    KijiRowData input = new HBaseKijiRowData(dataRequest, mCellDecoderFactory,
        tableLayout, result, getKiji().getSchemaTable());
    assertEquals(1, input.getTimestamps("family", "qual0").size());
    assertEquals("apple", input.getMostRecentValue("family", "qual0").toString());
    assertEquals(2, input.getTimestamps("family", "qual1").size());
    assertEquals("bear", input.getMostRecentValue("family", "qual1").toString());
    assertEquals("cat", input.getValue("family", "qual1", 4L).toString());
  }

  @Test
  public void testReadColumnTypes() throws IOException {
    LOG.info("start testReadColumnTypes");
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.getEntityId("row0");
    kvs.add(new KeyValue(row0.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, encodeStr("value")));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(1).add("family", "qual0");
    KijiDataRequest dataRequest = builder.build();
    HBaseKijiRowData input = new HBaseKijiRowData(dataRequest, mCellDecoderFactory,
        tableLayout, result, getKiji().getSchemaTable());
    for (String family : input.getMap().keySet()) {
      LOG.info("Family: " + family);
      for (String qual : input.getMap().get(family).keySet()) {
        LOG.info("Qualifier: " + qual);
      }
    }
    assertFalse(input.containsColumn("not-a-family"));
    assertTrue(input.containsColumn("family"));
    assertTrue(input.containsColumn("family", "qual0"));
    assertEquals("value", input.getMostRecentValue("family", "qual0").toString());
    assertEquals("value", input.getMostRecentValue("family", "qual0").toString());
    LOG.info("stop testReadColumnTypes");
  }

  @Test
  public void testReadFamilyTypes() throws IOException {
    final List<KeyValue> kvs = new ArrayList<KeyValue>();
    final EntityId row0 = mEntityIdFactory.getEntityId("row0");
    final byte[] hbaseRowKey = row0.getHBaseRowKey();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0, encodeStr("value0")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1, encodeStr("value1")));
    final Result result = new Result(kvs);

    final KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("family", "qual0");
    builder.newColumnsDef().add("family", "qual1");
    KijiDataRequest dataRequest = builder.build();
    KijiRowData input = new HBaseKijiRowData(dataRequest, mCellDecoderFactory,
        tableLayout, result, getKiji().getSchemaTable());
    assertTrue(input.containsColumn("family", "qual0"));
    assertEquals("value0", input.getMostRecentValue("family", "qual0").toString());
    assertEquals("value0", input.getMostRecentValue("family", "qual0").toString());
    assertTrue(input.containsColumn("family", "qual1"));
    assertEquals("value1", input.getMostRecentValue("family", "qual1").toString());
    assertEquals("value1", input.getMostRecentValue("family", "qual1").toString());
  }

  @Test
  public void testReadMapFamilyTypes() throws IOException {
    final List<KeyValue> kvs = new ArrayList<KeyValue>();
    final EntityId row0 = mEntityIdFactory.getEntityId("row0");
    final byte[] hbaseRowKey = row0.getHBaseRowKey();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseMapFamily, encodeStr("key0"), encodeStr("value0")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseMapFamily, encodeStr("key1"), encodeStr("value1")));
    final Result result = new Result(kvs);

    final KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().addFamily("map");
    KijiDataRequest dataRequest = builder.build();
    KijiRowData input = new HBaseKijiRowData(dataRequest, mCellDecoderFactory,
        tableLayout, result, getKiji().getSchemaTable());

    final NavigableMap<String, NavigableMap<Long, CharSequence>> stringsByTime =
       input.getValues("map");
    for (Map.Entry<String, NavigableMap<Long, CharSequence>> qualToOtherMap
      : stringsByTime.entrySet()) {
      LOG.debug("Qualifiers found: []", qualToOtherMap.getKey());
    }
  }

  @Test
  public void testReadSpecificFamilyTypes() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.getEntityId("row0");
    byte[] hbaseRowKey = row0.getHBaseRowKey();
    Node node0 = new Node();
    node0.setLabel("node0");
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal0, encodeNode(node0)));
    Node node1 = new Node();
    node1.setLabel("node1");
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal1, encodeNode(node1)));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(1).addFamily("family");
    KijiDataRequest dataRequest = builder.build();
    KijiRowData input = new HBaseKijiRowData(dataRequest, mCellDecoderFactory,
        tableLayout, result, getKiji().getSchemaTable());
    assertTrue(input.containsColumn("family", "nodequal0"));
    assertTrue(input.containsColumn("family", "nodequal1"));

    Node value0 = input.getMostRecentValue("family", "nodequal0");
    assertEquals("node0", value0.getLabel().toString());
    Node value1 = input.getMostRecentValue("family", "nodequal1");
    assertEquals("node1", value1.getLabel().toString());
  }

  @Test
  public void testReadSpecificTimestampTypes() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.getEntityId("row0");
    byte[] hbaseRowKey = row0.getHBaseRowKey();
    Node node0 = new Node();
    node0.setLabel("node0");
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal0, 100L, encodeNode(node0)));
    Node node1 = new Node();
    node1.setLabel("node1");
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal0, 200L, encodeNode(node1)));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(Integer.MAX_VALUE).add("family", "nodequal0");
    KijiDataRequest dataRequest = builder.build();
    KijiRowData input = new HBaseKijiRowData(dataRequest, mCellDecoderFactory,
        tableLayout, result, getKiji().getSchemaTable());
    assertTrue(input.containsColumn("family", "nodequal0"));
    NavigableMap<Long, Node> values = input.getValues("family", "nodequal0");
    assertNotNull(values);
    assertEquals(2, values.size());
    assertEquals("node0", values.get(100L).getLabel().toString());
    assertEquals("node1", values.get(200L).getLabel().toString());

    // Make sure they come in reverse chronological order.
    Iterator<NavigableMap.Entry<Long, Node>> iter = values.entrySet().iterator();
    assertTrue(iter.hasNext());
    assertEquals(200L, iter.next().getKey().longValue());
    assertEquals(100L, iter.next().getKey().longValue());
    assertFalse(iter.hasNext());
  }

  @Test
  public void testReadWithTimestamp() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.getEntityId("row0");
    byte[] hbaseRowKey = row0.getHBaseRowKey();
    Node node0 = new Node();
    node0.setLabel("node0");
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal0, 100L, encodeNode(node0)));
    Node node1 = new Node();
    node1.setLabel("node1");
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal0, 200L, encodeNode(node1)));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(Integer.MAX_VALUE).add("family", "nodequal0");
    KijiDataRequest dataRequest = builder.build();
    KijiRowData input = new HBaseKijiRowData(dataRequest, mCellDecoderFactory,
        tableLayout, result, getKiji().getSchemaTable());
    assertTrue(input.containsColumn("family", "nodequal0"));
    assertEquals("node0",
        ((Node) input.getValue("family", "nodequal0", 100L)).getLabel().toString());
    assertEquals("node1",
        ((Node) input.getValue("family", "nodequal0", 200L)).getLabel().toString());
  }

  @Test
  public void testReadSpecificTypes() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.getEntityId("row0");
    byte[] hbaseRowKey = row0.getHBaseRowKey();
    Node node = new Node();
    node.setLabel("foo");
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal0, encodeNode(node)));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("family", "nodequal0");
    KijiDataRequest dataRequest = builder.build();
    KijiRowData input = new HBaseKijiRowData(dataRequest, mCellDecoderFactory,
        tableLayout, result, getKiji().getSchemaTable());
    assertTrue(input.containsColumn("family", "nodequal0"));
    Node actual = input.getMostRecentValue("family", "nodequal0");
    assertEquals("foo", actual.getLabel().toString());
  }

  @Test
  public void testContainsColumn() throws Exception {
    final KijiTableLayout layout =
        KijiTableLayout.newLayout(KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE));

    // Create a different Kiji instance, with a table 'table' different than the one created
    // in setup:
    final Kiji kiji = new InstanceBuilder()
        .withTable("table", layout)
            .withRow("row1")
               .withFamily("family")
                  .withQualifier("column").withValue(1, "foo1")
        .build();
    try {
      final KijiTable table = kiji.openTable("table");
      try {
        final KijiTableReader reader = table.openTableReader();
        try {
          final KijiRowData row1 = reader.get(table.getEntityId("row1"),
              KijiDataRequest.create("family", "column"));
          assertTrue(row1.containsCell("family", "column", 1L));
          assertFalse(row1.containsCell("family", "column", 2L));
          assertFalse(row1.containsCell("blope", "column", 1L));
          assertFalse(row1.containsCell("family", "blope", 1L));
        } finally {
          reader.close();
        }
      } finally {
        table.release();
      }
    } finally {
      kiji.release();
    }
  }

  @Test
  public void testIterator() throws IOException {
    LOG.info("start testIterator");
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.getEntityId("row0");
    kvs.add(new KeyValue(row0.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, encodeStr("value0")));
    kvs.add(new KeyValue(row0.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, encodeStr("value1")));
    kvs.add(new KeyValue(row0.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, encodeStr("value2")));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(3).add("family", "qual0");
    KijiDataRequest dataRequest = builder.build();
    HBaseKijiRowData input = new HBaseKijiRowData(dataRequest, mCellDecoderFactory,
        tableLayout, result, getKiji().getSchemaTable());
    for (String family : input.getMap().keySet()) {
      LOG.info("Family: " + family);
      for (String qual : input.getMap().get(family).keySet()) {
        LOG.info("Qualifier: " + qual);
      }
    }
    assertFalse(input.containsColumn("not-a-family"));
    assertTrue(input.containsColumn("family"));
    assertTrue(input.containsColumn("family", "qual0"));
    Iterator<KijiCell<CharSequence>> cells = input.<CharSequence>iterator("family", "qual0");
    assertTrue(cells.hasNext());
    assertEquals("value0", cells.next().getData().toString());
    assertTrue(cells.hasNext());
    assertEquals("value1", cells.next().getData().toString());
    assertTrue(cells.hasNext());
    assertEquals("value2", cells.next().getData().toString());
    assertFalse(cells.hasNext());
  }

  @Test
  public void tesIteratorMapFamilyTypes() throws IOException {
    final KijiTableLayout layout =
    KijiTableLayout.newLayout(KijiTableLayouts.getLayout(KijiTableLayouts.ROW_DATA_TEST));
    Kiji kiji = null;
    KijiTable table = null;
    KijiTableReader reader = null;
    try {
      // Create a different Kiji instance, with a table 'table' different than the one created
      // in setup:
      kiji = new InstanceBuilder()
          .withTable("table", layout)
              .withRow("row1")
                .withFamily("map")
                    .withQualifier("key0").withValue(1L, 0)
                    .withQualifier("key1").withValue(1L, 1)
                    .withQualifier("key2").withValue(1L, 2)
                .withFamily("family")
                    .withQualifier("qual0").withValue(1L, "string1")
                    .withQualifier("qual0").withValue(2L, "string2")
          .build();
      KijiDataRequestBuilder builder = KijiDataRequest.builder();
      builder.newColumnsDef().withMaxVersions(1).addFamily("map");
      KijiDataRequest dataRequest = builder.build();
      try {
        table = kiji.openTable("table");
        try {
        reader = table.openTableReader();
        final KijiRowData row1 = reader.get(table.getEntityId("row1"),
            dataRequest);
        Iterator<KijiCell<Integer>> cells = row1.iterator("map");
        assertTrue(cells.hasNext());
        KijiCell cell0 = cells.next();
        assertEquals("Wrong first cell!", "key0", cell0.getQualifier());
        assertTrue(cells.hasNext());
        KijiCell cell1 = cells.next();
        assertEquals("Wrong second cell!", "key1", cell1.getQualifier());
        assertTrue(cells.hasNext());
        KijiCell cell2 = cells.next();
        assertEquals("Wrong third cell!", "key2", cell2.getQualifier());
        assertFalse(cells.hasNext());
        } finally {
          reader.close();
        }
      } finally {
        table.release();
      }
    } finally {
      kiji.release();
    }
  }

  @Test
  public void testIteratorMaxVersion() throws IOException {
    LOG.info("start testIteratorMaxVersion");
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.getEntityId("row0");
    kvs.add(new KeyValue(row0.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, encodeStr("value0")));
    kvs.add(new KeyValue(row0.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, encodeStr("value1")));
    kvs.add(new KeyValue(row0.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, encodeStr("value2")));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(2).add("family", "qual0");
    KijiDataRequest dataRequest = builder.build();
    HBaseKijiRowData input = new HBaseKijiRowData(dataRequest, mCellDecoderFactory,
        tableLayout, result, getKiji().getSchemaTable());

    assertFalse(input.containsColumn("not-a-family"));
    assertTrue(input.containsColumn("family"));
    assertTrue(input.containsColumn("family", "qual0"));
    Iterator<KijiCell<CharSequence>> cells = input.<CharSequence>iterator("family", "qual0");
    assertTrue(cells.hasNext());
    assertEquals("value0", cells.next().getData().toString());
    assertTrue(cells.hasNext());
    assertEquals("value1", cells.next().getData().toString());
    assertFalse(cells.hasNext());
  }

  @Test
  public void testIteratorMapFamilyMaxVersionsTypes() throws IOException {
    final KijiTableLayout layout =
      KijiTableLayout.newLayout(KijiTableLayouts.getLayout(KijiTableLayouts.ROW_DATA_TEST));
    Kiji kiji = null;
    KijiTable table = null;
    KijiTableReader reader = null;
    try {
      // Create a different Kiji instance, with a table 'table' different than the one created
      // in setup:
      kiji = new InstanceBuilder().withTable("table", layout)
        .withRow("row1")
            .withFamily("map")
              .withQualifier("key0")
                .withValue(1L, 0)
                .withValue(2L, 1)
                .withValue(3L, 2).build();
      KijiDataRequestBuilder builder = KijiDataRequest.builder();
      builder.newColumnsDef().withMaxVersions(2).addFamily("map");
      KijiDataRequest dataRequest = builder.build();
      try {
        table = kiji.openTable("table");
        try {
          reader = table.openTableReader();
          final KijiRowData row1 = reader.get(table.getEntityId("row1"),
            dataRequest);
          Iterator<KijiCell<Integer>> cells = row1.iterator("map");
          assertTrue(cells.hasNext());
          KijiCell<Integer> cell0 = cells.next();
          assertEquals("Wrong first cell!", 2, cell0.getData().intValue());
          assertTrue(cells.hasNext());
          KijiCell<Integer> cell1 = cells.next();
          assertEquals("Wrong second cell!", 1, cell1.getData().intValue());
          assertFalse(cells.hasNext());
        } finally {
          reader.close();
        }
      } finally {
        table.release();
      }
    } finally {
      kiji.release();
    }
  }

  @Test
  public void testMapAsIterable() throws IOException {
      final KijiTableLayout layout =
    KijiTableLayout.newLayout(KijiTableLayouts.getLayout(KijiTableLayouts.ROW_DATA_TEST));
    Kiji kiji = null;
    KijiTable table = null;
    KijiTableReader reader = null;
    try {
      // Create a different Kiji instance, with a table 'table' different than the one created
      // in setup:
      kiji = new InstanceBuilder()
          .withTable("table", layout)
              .withRow("row1")
                .withFamily("map")
                    .withQualifier("key0").withValue(1L, 0)
                    .withQualifier("key1").withValue(1L, 1)
                    .withQualifier("key2").withValue(1L, 2)
                .withFamily("family")
                    .withQualifier("qual0").withValue(1L, "string1")
                    .withQualifier("qual0").withValue(2L, "string2")
          .build();
      KijiDataRequestBuilder builder = KijiDataRequest.builder();
      builder.newColumnsDef().withMaxVersions(3).addFamily("map");
      KijiDataRequest dataRequest = builder.build();
      try {
        table = kiji.openTable("table");
        try {
        reader = table.openTableReader();
        final KijiRowData row1 = reader.get(table.getEntityId("row1"),
            dataRequest);
        Iterable<KijiCell<Integer>> cells = row1.<Integer>asIterable("map");
        int cellCount = 0;
        for (KijiCell<Integer> cell: cells) {
          cellCount += 1;
        }
        assertEquals("Wrong number of cells returned by asIterable.", 3, cellCount);
        } finally {
          reader.close();
        }
      } finally {
        table.release();
      }
    } finally {
      kiji.release();
    }
  }

  @Test
  public void testGroupAsIterable() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.getEntityId("row0");
    kvs.add(new KeyValue(row0.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, encodeStr("value0")));
    kvs.add(new KeyValue(row0.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, encodeStr("value1")));
    kvs.add(new KeyValue(row0.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, encodeStr("value2")));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(3).add("family", "qual0");
    KijiDataRequest dataRequest = builder.build();
    HBaseKijiRowData input = new HBaseKijiRowData(dataRequest, mCellDecoderFactory,
        tableLayout, result, getKiji().getSchemaTable());
    for (String family : input.getMap().keySet()) {
      LOG.info("Family: " + family);
      for (String qual : input.getMap().get(family).keySet()) {
        LOG.info("Qualifier: " + qual);
      }
    }
    assertFalse(input.containsColumn("not-a-family"));
    assertTrue(input.containsColumn("family"));
    assertTrue(input.containsColumn("family", "qual0"));
    Iterable<KijiCell<CharSequence>> cells = input.<CharSequence>asIterable("family", "qual0");
    assertTrue(input.iterator("family", "qual0").hasNext());
    int cellCount = 0;
    for (KijiCell<CharSequence> cell: cells) {
      cellCount += 1;
    }
    assertEquals("Wrong number of cells returned by asIterable.", 3, cellCount);
  }

  @Test
  public void testEmptyResult() throws IOException {
    LOG.info("start testEmptyResult");
    final KijiTableLayout layout =
    KijiTableLayout.newLayout(KijiTableLayouts.getLayout(KijiTableLayouts.ROW_DATA_TEST));
    Kiji kiji = null;
    KijiTable table = null;
    KijiTableReader reader = null;
    try {
      // Create a different Kiji instance, with a table 'table' different than the one created
      // in setup:
      kiji = new InstanceBuilder()
          .withTable("table", layout)
              .withRow("row1")
                .withFamily("family")
                    .withQualifier("qual0").withValue(1L, "string1")
                    .withQualifier("qual0").withValue(2L, "string2")
          .build();
      KijiDataRequestBuilder builder = KijiDataRequest.builder();
      builder.newColumnsDef().add("family", "qual1");
      KijiDataRequest dataRequest = builder.build();
        table = kiji.openTable("table");
        try {
        reader = table.openTableReader();
        final KijiRowData row1 = reader.get(table.getEntityId("row1"),
            dataRequest);

        NavigableMap<Long, CharSequence> values = row1.getValues("family", "qual1");
        assertTrue("getValues should return an empty map for empty rowdata.", values.isEmpty());
        NavigableMap<Long, KijiCell<CharSequence>> cells = row1.getCells("family", "qual1");
        assertTrue("getCells should return an empty map for empty rowdata.", cells.isEmpty());
        Iterator<KijiCell<CharSequence>> iterator =  row1.iterator("family", "qual1");
        assertFalse("iterator obtained on a column the rowdata has no data for should return false"
          + "when hasNext is called.",
            iterator.hasNext());
        CharSequence value = row1.getMostRecentValue("family", "qual1");
        assertEquals("getMostRecentValue should return a null value from an empty rowdata.", null,
            value);
        KijiCell<CharSequence> cell = row1.getMostRecentCell("family", "qual1");
        assertEquals("getMostRecentCell should return a null cell from empty rowdata.", null,
            cell);
        } finally {
          reader.close();
        }
      } finally {
        table.release();
      }
  }

}
