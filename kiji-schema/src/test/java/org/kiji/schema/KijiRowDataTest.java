/**
 * (c) Copyright 2014 WibiData, Inc.
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.avro.Node;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;

/**
 * Tests for the {@link KijiRowData} API. Implementations of {@code KijiRowData} should provide a
 * concrete implementation for this test suite.
 */
public abstract class KijiRowDataTest extends KijiClientTest {

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

  private static final String FAMILY = "family";
  private static final String QUALIFIER_0 = "qual0";
  private static final String QUALIFIER_1 = "qual1";
  private static final String QUALIFIER_2 = "qual2";
  private static final String QUALIFIER_3 = "qual3";
  private static final String QUALIFIER_NODE_0 = "nodequal0";
  private static final String QUALIFIER_NODE_1 = "nodequal1";

  private static final String MAP_FAMILY = "map";
  private static final String KEY_0 = "key0";
  private static final String KEY_1 = "key1";
  private static final String KEY_2 = "key2";

  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableWriter mWriter;
  private KijiTableReader mReader;

  private final Node mNode0 = Node.newBuilder().setLabel("node0").build();
  private final Node mNode1 = Node.newBuilder().setLabel("node1").build();

  @Before
  public final void setupKijiRowDataTest() throws Exception {
    mKiji = getKiji();
    mKiji.createTable(KijiTableLayouts.getLayout(TEST_LAYOUT_V1));
    mTable = getKiji().openTable(TABLE_NAME);
    mWriter = mTable.openTableWriter();
    mReader = mTable.openTableReader();
  }

  @After
  public final void teardownKijiRowDataTest() throws Exception {
    mWriter.close();
    mReader.close();
    mTable.release();
    mTable = null;
    mKiji = null;
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Create a {@link KijiRowData} for the given table, reader, entity id, and data request.
   * Concrete implementations of {@code KijiRowData} should implement this test class and this
   * {@code KijiRowData} generator.
   *
   * @param table The table from which to create the {@link KijiRowData}.
   * @param reader The table reader to use for reading from the table.
   * @param eid The entity id of the requested row data.
   * @param dataRequest The data request defining the row data.
   * @return The requested Kiji row data object.
   * @throws IOException On error creating the Kiji row data.
   */
  public abstract KijiRowData getRowData(
      final KijiTable table,
      final KijiTableReader reader,
      final EntityId eid,
      final KijiDataRequest dataRequest
  ) throws IOException;

  // -----------------------------------------------------------------------------------------------

  @Test
  public void testEntityId() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");
    final KijiRowData data = getRowData(mTable, mReader, eid, KijiDataRequest.empty());
    assertEquals(eid, data.getEntityId());
  }

  @Test
  public void testGetReaderSchema() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");

    final KijiRowData data = getRowData(mTable, mReader, eid, KijiDataRequest.empty());
    assertEquals(Schema.create(Schema.Type.STRING), data.getReaderSchema(FAMILY, "empty"));
    assertEquals(Schema.create(Schema.Type.INT), data.getReaderSchema(FAMILY, "qual3"));
  }

  @Test
  public void testReadInts() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");
    mWriter.put(eid, FAMILY, QUALIFIER_3, 1L, 42);

    final KijiRowData data = getRowData(
        mTable,
        mReader, eid, KijiDataRequest.create(FAMILY, QUALIFIER_3));
    final int value = data.<Integer>getMostRecentValue(FAMILY, "qual3");
    assertEquals(42, value);
  }

  @Test
  public void testGetReaderSchemaNoSuchColumn() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");
    final KijiRowData data = getRowData(mTable, mReader, eid, KijiDataRequest.empty());

    try {
      data.getReaderSchema("this_family", "does_not_exist");
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("Table 'row_data_test_table' has no family 'this_family'.", nsce.getMessage());
    }

    try {
      data.getReaderSchema(FAMILY, "no_qualifier");
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("Table 'row_data_test_table' has no column 'family:no_qualifier'.",
          nsce.getMessage());
    }
  }

  /** Tests for KijiRowData.getReaderSchema() with layout-1.3 tables. */
  @Test
  public void testGetReaderSchemaLayout13() throws Exception {
    mKiji.createTable(KijiTableLayouts.getLayout(TEST_LAYOUT_V1_3));
    final KijiTable table = mKiji.openTable("table");
    try {
      final KijiTableReader reader = table.getReaderFactory().openTableReader();
      try {
        final KijiRowData row = reader.get(table.getEntityId("eid"), KijiDataRequest.empty());
        assertEquals(Schema.Type.STRING, row.getReaderSchema(FAMILY, QUALIFIER_0).getType());
      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }

  @Test
  public void testReadWithMaxVersions() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");

    mWriter.put(eid, FAMILY, QUALIFIER_0, 3L, "apple");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 2L, "banana");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 1L, "carrot");
    mWriter.put(eid, FAMILY, QUALIFIER_1, 6L, "antelope");
    mWriter.put(eid, FAMILY, QUALIFIER_1, 5L, "bear");
    mWriter.put(eid, FAMILY, QUALIFIER_1, 4L, "cat");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(1).add(FAMILY, QUALIFIER_0))
        .addColumns(ColumnsDef.create().withMaxVersions(2).add(FAMILY, QUALIFIER_1))
        .build();

    final KijiRowData data = getRowData(mTable, mReader, eid, dataRequest);
    assertEquals(1, data.getValues(FAMILY, QUALIFIER_0).size());
    assertEquals("apple", data.getMostRecentValue(FAMILY, QUALIFIER_0).toString());
    assertEquals(2, data.getValues(FAMILY, QUALIFIER_1).size());
    assertEquals("antelope", data.getValues(FAMILY, QUALIFIER_1).get(6L).toString());
    assertEquals("bear", data.getValues(FAMILY, QUALIFIER_1).get(5L).toString());
  }

  @Test
  public void testTypedReadWithMaxVersions() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");

    mWriter.put(eid, FAMILY, QUALIFIER_0, 3L, "apple");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 2L, "banana");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 1L, "carrot");
    mWriter.put(eid, FAMILY, QUALIFIER_1, 6L, "antelope");
    mWriter.put(eid, FAMILY, QUALIFIER_1, 5L, "bear");
    mWriter.put(eid, FAMILY, QUALIFIER_1, 4L, "cat");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(1).add(FAMILY, QUALIFIER_0))
        .addColumns(ColumnsDef.create().withMaxVersions(2).add(FAMILY, QUALIFIER_1))
        .build();

    final KijiRowData data = getRowData(mTable, mReader, eid, dataRequest);
    assertEquals(1, data.getValues(FAMILY, QUALIFIER_0).size());
    final NavigableMap<Long, CharSequence> typedValues = data.getValues(FAMILY, QUALIFIER_0);
    assertEquals("apple", typedValues.get(3L).toString());
    assertEquals(2, data.getTimestamps(FAMILY, QUALIFIER_1).size());
  }

  @Test
  public void testReadWithTimeRange() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");

    mWriter.put(eid, FAMILY, QUALIFIER_0, 3L, "apple");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 2L, "banana");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 1L, "carrot");
    mWriter.put(eid, FAMILY, QUALIFIER_1, 5L, "bear");
    mWriter.put(eid, FAMILY, QUALIFIER_1, 4L, "cat");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .withTimeRange(2L, 6L)
        .addColumns(ColumnsDef.create().withMaxVersions(1).add(FAMILY, QUALIFIER_0))
        .addColumns(ColumnsDef.create().withMaxVersions(2).add(FAMILY, QUALIFIER_1))
        .build();

    final KijiRowData data = getRowData(mTable, mReader, eid, dataRequest);
    assertEquals(1, data.getTimestamps(FAMILY, QUALIFIER_0).size());
    assertEquals("apple", data.getMostRecentValue(FAMILY, QUALIFIER_0).toString());
    assertEquals(2, data.getTimestamps(FAMILY, QUALIFIER_1).size());
    assertEquals("bear", data.getMostRecentValue(FAMILY, QUALIFIER_1).toString());
    assertEquals("cat", data.getValue(FAMILY, QUALIFIER_1, 4L).toString());
  }

  @Test
  public void testReadColumnTypes() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");

    mWriter.put(eid, FAMILY, QUALIFIER_0, "value");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(1).add(FAMILY, QUALIFIER_0))
        .build();
    final KijiRowData data = getRowData(mTable, mReader, eid, dataRequest);

    assertFalse(data.containsColumn("not_a_family"));
    assertTrue(data.containsColumn(FAMILY));
    assertTrue(data.containsColumn(FAMILY, QUALIFIER_0));
    assertEquals("value", data.getMostRecentValue(FAMILY, QUALIFIER_0).toString());
    assertEquals("value", data.getMostRecentValue(FAMILY, QUALIFIER_0).toString());
  }

  @Test
  public void testReadFamilyTypes() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");

    mWriter.put(eid, FAMILY, QUALIFIER_0, "value0");
    mWriter.put(eid, FAMILY, QUALIFIER_1, "value1");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().add(FAMILY, QUALIFIER_0))
        .addColumns(ColumnsDef.create().add(FAMILY, QUALIFIER_1))
        .build();


    final KijiRowData data = getRowData(mTable, mReader, eid, dataRequest);
    assertTrue(data.containsColumn(FAMILY, QUALIFIER_0));
    assertEquals("value0", data.getMostRecentValue(FAMILY, QUALIFIER_0).toString());
    assertEquals("value0", data.getMostRecentValue(FAMILY, QUALIFIER_0).toString());
    assertTrue(data.containsColumn(FAMILY, QUALIFIER_1));
    assertEquals("value1", data.getMostRecentValue(FAMILY, QUALIFIER_1).toString());
    assertEquals("value1", data.getMostRecentValue(FAMILY, QUALIFIER_1).toString());
  }

  @Test
  public void testReadMapFamilyTypes() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");

    mWriter.put(eid, MAP_FAMILY, KEY_0, 0L, 0);
    mWriter.put(eid, MAP_FAMILY, KEY_1, 0L, 1);

    final KijiDataRequest dataRequest = KijiDataRequest.create(MAP_FAMILY);

    final KijiRowData data = getRowData(mTable, mReader, eid, dataRequest);
    final NavigableMap<String, NavigableMap<Long, Integer>> values =
        data.getValues(MAP_FAMILY);

    assertEquals(2, values.size());
    assertEquals(1, values.get(KEY_0).size());
    assertEquals(1, values.get(KEY_1).size());
    assertEquals(Integer.valueOf(0), values.get(KEY_0).get(0L));
    assertEquals(Integer.valueOf(1), values.get(KEY_1).get(0L));
  }

  @Test
  public void testReadSpecificFamilyTypes() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");
    mWriter.put(eid, FAMILY, QUALIFIER_NODE_0, mNode0);
    mWriter.put(eid, FAMILY, QUALIFIER_NODE_1, mNode1);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(1).addFamily(FAMILY))
        .build();

    final KijiRowData data = getRowData(mTable, mReader, eid, dataRequest);
    assertTrue(data.containsColumn(FAMILY, "nodequal0"));
    assertTrue(data.containsColumn(FAMILY, "nodequal1"));

    final Node value0 = data.getMostRecentValue(FAMILY, "nodequal0");
    assertEquals("node0", value0.getLabel());
    final Node value1 = data.getMostRecentValue(FAMILY, "nodequal1");
    assertEquals("node1", value1.getLabel());
  }

  @Test
  public void testReadSpecificTimestampTypes() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");
    mWriter.put(eid, FAMILY, QUALIFIER_NODE_0, 100L, mNode0);
    mWriter.put(eid, FAMILY, QUALIFIER_NODE_0, 200L, mNode1);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(
            ColumnsDef.create().withMaxVersions(Integer.MAX_VALUE).add(FAMILY, QUALIFIER_NODE_0))
        .build();

    final KijiRowData data = getRowData(mTable, mReader, eid, dataRequest);

    assertTrue(data.containsColumn(FAMILY, QUALIFIER_NODE_0));
    final NavigableMap<Long, Node> values = data.getValues(FAMILY, QUALIFIER_NODE_0);
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
    final EntityId eid = mTable.getEntityId("eid");
    mWriter.put(eid, FAMILY, QUALIFIER_NODE_0, 100L, mNode0);
    mWriter.put(eid, FAMILY, QUALIFIER_NODE_0, 200L, mNode1);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(
            ColumnsDef.create().withMaxVersions(Integer.MAX_VALUE).add(FAMILY, "nodequal0"))
        .build();

    final KijiRowData data = getRowData(mTable, mReader, eid, dataRequest);
    assertTrue(data.containsColumn(FAMILY, QUALIFIER_NODE_0));
    assertEquals("node0", ((Node) data.getValue(FAMILY, QUALIFIER_NODE_0, 100L)).getLabel());
    assertEquals("node1", ((Node) data.getValue(FAMILY, QUALIFIER_NODE_0, 200L)).getLabel());
  }

  @Test
  public void testReadSpecificTypes() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");
    final Node node = Node.newBuilder().setLabel("foo").build();
    mWriter.put(eid, FAMILY, QUALIFIER_NODE_0, node);

    final KijiDataRequest dataRequest = KijiDataRequest.create(FAMILY, QUALIFIER_NODE_0);
    final KijiRowData data = getRowData(mTable, mReader, eid, dataRequest);

    assertTrue(data.containsColumn(FAMILY, QUALIFIER_NODE_0));
    final Node actual = data.getMostRecentValue(FAMILY, QUALIFIER_NODE_0);
    assertEquals("foo", actual.getLabel());
  }

  @Test
  public void testContainsColumn() throws Exception {
    final long timestamp = 1L;
    final EntityId eid = mTable.getEntityId("eid");
    mWriter.put(eid, FAMILY, QUALIFIER_0, timestamp, "foo");

    final KijiRowData data = getRowData(
        mTable,
        mReader, eid, KijiDataRequest.create(FAMILY, QUALIFIER_0));

    assertTrue(data.containsCell(FAMILY, QUALIFIER_0, timestamp));
    assertFalse(data.containsCell(FAMILY, QUALIFIER_0, 2L));
    assertFalse(data.containsCell("blope", QUALIFIER_0, timestamp));
    assertFalse(data.containsCell(FAMILY, "blope", timestamp));
  }

  @Test
  public void testIterator() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");

    mWriter.put(eid, FAMILY, QUALIFIER_0, 2L, "value0");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 1L, "value1");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 0L, "value2");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(3).add(FAMILY, QUALIFIER_0))
        .build();
    final KijiRowData data = getRowData(mTable, mReader, eid, dataRequest);

    assertFalse(data.containsColumn("not_a_family"));
    assertTrue(data.containsColumn(FAMILY));
    assertTrue(data.containsColumn(FAMILY, QUALIFIER_0));
    final Iterator<KijiCell<CharSequence>> cells = data.iterator(FAMILY, QUALIFIER_0);
    assertTrue(cells.hasNext());
    assertEquals("value0", cells.next().getData().toString());
    assertTrue(cells.hasNext());
    assertEquals("value1", cells.next().getData().toString());
    assertTrue(cells.hasNext());
    assertEquals("value2", cells.next().getData().toString());
    assertFalse(cells.hasNext());

    try {
      data.iterator("unknown_family");
      fail("HBaseKijiRowData.iterator() should fail on columns with no data request.");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("Column unknown_family has no data request."));
    }

    try {
      data.iterator(FAMILY, QUALIFIER_1);
      fail("HBaseKijiRowData.iterator() should fail on columns with no data request.");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("Column family:qual1 has no data request."));
    }
  }

  @Test
  public void testCellIterator() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");

    mWriter.put(eid, FAMILY, QUALIFIER_0, 3L, "value3");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 2L, "value2");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 1L, "value1");

    final KijiDataRequest request = KijiDataRequest.create(FAMILY);
    final KijiRowData data = getRowData(mTable, mReader, eid, request);

    final Iterator<KijiCell<String>> iter = data.iterator(FAMILY, QUALIFIER_1);
    // FAMILY:QUALIFIER_1 should be empty. Prior to SCHEMA-838 the iterator would return all values
    // from FAMILY:QUALIFIER_0
    assertFalse(iter.hasNext());
  }

  @Test
  public void testCellIteratorReversed() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");

    mWriter.put(eid, FAMILY, QUALIFIER_1, 3L, "value3");
    mWriter.put(eid, FAMILY, QUALIFIER_1, 2L, "value2");
    mWriter.put(eid, FAMILY, QUALIFIER_1, 1L, "value1");

    final KijiDataRequest request = KijiDataRequest.create(FAMILY);
    final KijiRowData data = getRowData(mTable, mReader, eid, request);

    final Iterator<KijiCell<String>> iter = data.iterator(FAMILY, QUALIFIER_0);
    // FAMILY:QUALIFIER_0 should be empty. Prior to SCHEMA-838 the iterator would return all values
    // from FAMILY:QUALIFIER_1
    assertFalse(iter.hasNext());
  }

  @Test
  public void testIteratorMapFamilyTypes() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");

    mWriter.put(eid, MAP_FAMILY, KEY_0, 1L, 0);
    mWriter.put(eid, MAP_FAMILY, KEY_1, 1L, 1);
    mWriter.put(eid, MAP_FAMILY, KEY_2, 1L, 2);
    mWriter.put(eid, FAMILY, QUALIFIER_0, 1L, "string1");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 2L, "string2");

    final KijiDataRequest dataRequest = KijiDataRequest.create(MAP_FAMILY);

    final KijiRowData data = getRowData(mTable, mReader, eid, dataRequest);
    final Iterator<KijiCell<Integer>> cells = data.iterator("map");
    assertTrue(cells.hasNext());
    final KijiCell<?> cell0 = cells.next();
    assertEquals("Wrong first cell!", KEY_0, cell0.getColumn().getQualifier());
    assertTrue(cells.hasNext());
    final KijiCell<?> cell1 = cells.next();
    assertEquals("Wrong second cell!", KEY_1, cell1.getColumn().getQualifier());
    assertTrue(cells.hasNext());
    final KijiCell<?> cell2 = cells.next();
    assertEquals("Wrong third cell!", KEY_2, cell2.getColumn().getQualifier());
    assertFalse(cells.hasNext());

    final Iterator<KijiCell<Integer>> cellsKey1 = data.iterator(MAP_FAMILY, KEY_1);
    assertTrue(cellsKey1.hasNext());
    final KijiCell<Integer> key1Cell = cellsKey1.next();
    assertEquals(KEY_1, key1Cell.getColumn().getQualifier());
    assertEquals(1L, key1Cell.getTimestamp());
    assertEquals((Integer) 1, key1Cell.getData());
    assertFalse(cellsKey1.hasNext());
  }

  @Test
  public void testIteratorMaxVersion() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 2L, "value0");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 1L, "value1");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 0L, "value2");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(2).add(FAMILY, QUALIFIER_0))
        .build();
    final KijiRowData data = getRowData(mTable, mReader, eid, dataRequest);

    assertFalse(data.containsColumn("not_a_family"));
    assertTrue(data.containsColumn(FAMILY));
    assertTrue(data.containsColumn(FAMILY, QUALIFIER_0));
    final Iterator<KijiCell<CharSequence>> cells = data.iterator(FAMILY, QUALIFIER_0);
    assertTrue(cells.hasNext());
    assertEquals("value0", cells.next().getData().toString());
    assertTrue(cells.hasNext());
    assertEquals("value1", cells.next().getData().toString());
    assertFalse(cells.hasNext());
  }

  @Test
  public void testIteratorMapFamilyMaxVersionsTypes() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");
    mWriter.put(eid, MAP_FAMILY, KEY_0, 0L, 0);
    mWriter.put(eid, MAP_FAMILY, KEY_0, 1L, 1);
    mWriter.put(eid, MAP_FAMILY, KEY_0, 2L, 2);


    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(2).addFamily("map"))
        .build();


    final KijiRowData data = getRowData(mTable, mReader, eid, dataRequest);
    final Iterator<KijiCell<Integer>> cells = data.iterator(MAP_FAMILY);
    assertTrue(cells.hasNext());
    final KijiCell<Integer> cell0 = cells.next();
    assertEquals("Wrong first cell!", 2, cell0.getData().intValue());
    assertTrue(cells.hasNext());
    final KijiCell<Integer> cell1 = cells.next();
    assertEquals("Wrong second cell!", 1, cell1.getData().intValue());
    assertFalse(cells.hasNext());
  }

  @Test
  public void testMapAsIterable() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");
    mWriter.put(eid, MAP_FAMILY, KEY_0, 1L, 0);
    mWriter.put(eid, MAP_FAMILY, KEY_1, 1L, 1);
    mWriter.put(eid, MAP_FAMILY, KEY_2, 1L, 2);
    mWriter.put(eid, FAMILY, QUALIFIER_0, 1L, "string1");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 2L, "string2");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(3).addFamily("map"))
        .build();

    final KijiRowData row1 = getRowData(mTable, mReader, eid, dataRequest);
    final List<KijiCell<Integer>> cells = Lists.newArrayList(row1.<Integer>asIterable(MAP_FAMILY));
    final int cellCount = cells.size();
    assertEquals("Wrong number of cells returned by asIterable.", 3, cellCount);
  }

  @Test
  public void testGroupAsIterable() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");

    mWriter.put(eid, FAMILY, QUALIFIER_0, 2L, "value0");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 1L, "value1");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 0L, "value2");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(3).add(FAMILY, QUALIFIER_0))
        .build();

    final KijiRowData data = getRowData(mTable, mReader, eid, dataRequest);

    assertFalse(data.containsColumn("not_a_family"));
    assertTrue(data.containsColumn(FAMILY));
    assertTrue(data.containsColumn(FAMILY, QUALIFIER_0));
    final List<KijiCell<CharSequence>> cells =
        Lists.newArrayList(data.<CharSequence>asIterable(FAMILY, QUALIFIER_0));
    final int cellCount = cells.size();
    assertEquals("Wrong number of cells returned by asIterable.", 3, cellCount);
  }

  /** Test that we can select a timestamped value that is not the most recent value. */
  @Test
  public void testReadMiddleTimestamp() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");

    mWriter.put(eid, FAMILY, QUALIFIER_0, 4L, "oldest");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 6L, "middle");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 8L, "newest");

    mWriter.put(eid, FAMILY, QUALIFIER_1, 1L, "one");
    mWriter.put(eid, FAMILY, QUALIFIER_1, 2L, "two");
    mWriter.put(eid, FAMILY, QUALIFIER_1, 3L, "three");
    mWriter.put(eid, FAMILY, QUALIFIER_1, 4L, "four");
    mWriter.put(eid, FAMILY, QUALIFIER_1, 8L, "eight");

    mWriter.put(eid, FAMILY, QUALIFIER_2, 3L, "q2-three");
    mWriter.put(eid, FAMILY, QUALIFIER_2, 4L, "q2-four");
    mWriter.put(eid, FAMILY, QUALIFIER_2, 6L, "q2-six");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .withTimeRange(2L, 7L)
        .addColumns(ColumnsDef.create().add(FAMILY, QUALIFIER_0))
        .addColumns(ColumnsDef.create().withMaxVersions(2).add(FAMILY, QUALIFIER_1))
        .addColumns(ColumnsDef.create().withMaxVersions(3).add(FAMILY, QUALIFIER_2))
        .build();

    final KijiRowData data = getRowData(mTable, mReader, eid, dataRequest);

    // This should be "middle" based on the time range of the data request.
    final String qual0val = data.getMostRecentValue(FAMILY, QUALIFIER_0).toString();
    assertEquals("Didn't get the middle value for family:qual0", "middle", qual0val);

    // We always optimize maxVersions=1 to actually return exactly 1 value, even of
    // we requested more versions of other columns.
    final NavigableMap<Long, CharSequence> q0vals = data.getValues(FAMILY, QUALIFIER_0);
    assertEquals("qual0 should only return one thing", 1, q0vals.size());
    assertEquals("Newest (only) value in q0 should be 'middle'.",
        "middle", q0vals.firstEntry().getValue().toString());

    // qual1 should see at least two versions, but no newer than 7L.
    final NavigableMap<Long, CharSequence> q1vals = data.getValues(FAMILY, QUALIFIER_1);
    assertEquals("qual1 getValues should have exactly two items", 2, q1vals.size());
    assertEquals(
        "Newest value in q1 should be 'four'.",
        "four", q1vals.firstEntry().getValue().toString());

    // qual2 should see exactly three versions.
    final NavigableMap<Long, CharSequence> q2vals = data.getValues(FAMILY, QUALIFIER_2);
    assertEquals("qual2 getValues should have exactly three items", 3, q2vals.size());
    assertEquals("Newest value in q2 should be 'q2-six'.",
        "q2-six", q2vals.firstEntry().getValue().toString());
  }

  @Test
  public void testEmptyResult() throws IOException {
    final EntityId eid = mTable.getEntityId("eid");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 1L, "string1");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 2L, "string2");

    final KijiDataRequest dataRequest = KijiDataRequest.create(FAMILY, QUALIFIER_1);

    final KijiRowData row1 = getRowData(mTable, mReader, eid, dataRequest);

    final NavigableMap<Long, CharSequence> values = row1.getValues(FAMILY, QUALIFIER_1);
    assertTrue("getValues should return an empty map for empty rowdata.", values.isEmpty());

    final NavigableMap<Long, KijiCell<CharSequence>> cells = row1.getCells(FAMILY, QUALIFIER_1);
    assertTrue("getCells should return an empty map for empty rowdata.", cells.isEmpty());

    final Iterator<KijiCell<CharSequence>> iterator =  row1.iterator(FAMILY, QUALIFIER_1);
    assertFalse("iterator obtained on a column the rowdata has no data for should return false"
        + "when hasNext is called.",
        iterator.hasNext());

    final CharSequence value = row1.getMostRecentValue(FAMILY, QUALIFIER_1);
    assertNull("getMostRecentValue should return a null value from an empty rowdata.", value);

    final KijiCell<CharSequence> cell = row1.getMostRecentCell(FAMILY, QUALIFIER_1);
    assertNull("getMostRecentCell should return a null cell from empty rowdata.", cell);
  }

  /** Tests that reading an entire family with a column that has been deleted works. */
  @Test
  public void testReadDeletedColumns() throws Exception {
    final EntityId eid = mTable.getEntityId("eid");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 1L, "string1");
    mWriter.put(eid, FAMILY, QUALIFIER_0, 2L, "string2");

    final TableLayoutDesc update = KijiTableLayouts.getLayout(TEST_LAYOUT_V2);
    update.setReferenceLayout(mTable.getLayout().getDesc().getLayoutId());
    mKiji.modifyTableLayout(update);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().addFamily(FAMILY))
        .build();

    final KijiRowData row1 = getRowData(mTable, mReader, eid, dataRequest);
    assertTrue(row1.getValues(FAMILY, QUALIFIER_0).isEmpty());
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
    mKiji.createTable(KijiTableLayouts.getLayout(WRITER_SCHEMA_TEST));
    final KijiTable table = mKiji.openTable("writer_schema");
    try {
      final EntityId eid = table.getEntityId("eid");

      // Write a (generic) record:
      final Schema writerSchema = Schema.createRecord("Found", null, "class.not", false);
      writerSchema.setFields(
          Lists.newArrayList(new Field("field", Schema.create(Schema.Type.STRING), null, null)));

      final KijiTableWriter writer = table.openTableWriter();
      try {
        final GenericData.Record record =
            new GenericRecordBuilder(writerSchema).set("field", "value").build();
        writer.put(eid, FAMILY, "qualifier", 1L, record);
      } finally {
        writer.close();
      }

      // Read the record back (should be a generic record):
      final KijiTableReader reader = table.openTableReader();
      try {
        final KijiDataRequest dataRequest = KijiDataRequest.create(FAMILY, "qualifier");
        final KijiRowData row = reader.get(eid, dataRequest);
        final GenericData.Record record = row.getValue(FAMILY, "qualifier", 1L);
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
