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

package org.kiji.schema.cassandra;

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
import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.avro.Node;
import org.kiji.schema.impl.cassandra.CassandraDataRequestAdapter;
import org.kiji.schema.impl.cassandra.CassandraKijiRowData;
import org.kiji.schema.impl.cassandra.CassandraKijiTable;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestCassandraKijiRowData2 {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraKijiRowData2.class);

  /** Test layout. */
  public static final String TEST_LAYOUT_V1 =
      "org/kiji/schema/layout/TestHBaseKijiRowData.test-layout-v1.json";

  private static final String TABLE_NAME = "row_data_test_table";

  private static final String FAMILY = "family";
  private static final String EMPTY = "empty";
  private static final String QUAL0 = "qual0";
  private static final String QUAL1 = "qual1";
  private static final String QUAL3 = "qual3";
  private static final String NODEQUAL0 = "nodequal0";
  private static final String NODEQUAL1 = "nodequal1";
  private static final String MAP = "map";
  private static final String KEY0 = "key0";
  private static final String KEY1 = "key1";

  private static final int KEY0_VAL = 100;
  private static final int KEY1_VAL = 101;

  private static EntityIdFactory mEntityIdFactory;

  // Share this table among lots of different tests that don't use it for anything other than
  // creating an instance of CassandraKijiRowData.  Within CassandraKijiRowData, the table is not
  // use unless you use a pager (which we do not in these tests).
  private static CassandraKijiTable mSharedTable;

  private static final CassandraKijiClientTest CLIENT_TEST_DELEGATE = new CassandraKijiClientTest();

  private static final Node NODE0 = Node.newBuilder().setLabel("node0").build();
  private static final Node NODE1 = Node.newBuilder().setLabel("node1").build();

  private static Set<Row> mAllRows;

  @BeforeClass
  public static void createSharedRows() throws Exception {
    CLIENT_TEST_DELEGATE.setupKijiTest();
    Kiji kiji = CLIENT_TEST_DELEGATE.getKiji();
    kiji.createTable(KijiTableLayouts.getLayout(TEST_LAYOUT_V1));
    mSharedTable = (CassandraKijiTable) kiji.openTable(TABLE_NAME);

    mEntityIdFactory = EntityIdFactory.getFactory(mSharedTable.getLayout());

    EntityId eid = mEntityIdFactory.getEntityId("foo");
    // Put some data into the mSharedTable.
    KijiTableWriter writer = mSharedTable.openTableWriter();
    writer.put(eid, FAMILY, QUAL0, 3L, "apple");
    writer.put(eid, FAMILY, QUAL0, 2L, "banana");
    writer.put(eid, FAMILY, QUAL0, 1L, "carrot");
    writer.put(eid, FAMILY, QUAL1, 6L, "antelope");
    writer.put(eid, FAMILY, QUAL1, 5L, "bear");
    writer.put(eid, FAMILY, QUAL1, 4L, "cat");

    // Add something to the map-type family also!
    writer.put(eid, MAP, KEY0, 0L, KEY0_VAL);
    writer.put(eid, MAP, KEY1, 0L, KEY1_VAL);

    // And the node values.
    writer.put(eid, FAMILY, NODEQUAL0, 0L, NODE0);
    writer.put(eid, FAMILY, NODEQUAL1, 100L, NODE0);
    writer.put(eid, FAMILY, NODEQUAL1, 200L, NODE1);

    writer.close();

    // Create a big set of Rows with a data request without max versions.
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef()
        .withMaxVersions(Integer.MAX_VALUE)
        .addFamily(FAMILY)
        .addFamily(MAP);
    final KijiDataRequest dataRequestAllVersions = builder.build();

    CassandraDataRequestAdapter adapter = new CassandraDataRequestAdapter(
        dataRequestAllVersions,
        mSharedTable.getColumnNameTranslator());

    List<ResultSet> results = adapter.doGet(mSharedTable, eid);
    Set<Row> allRows = Sets.newHashSet();

    // Note that we do not order the results there, since the other classes in Kiji do not
    // preserve row ordering in results from Cassandra either.  We could modify that behavior to
    // retain row ordering (and in doing so, possibly improve performance for some client-side
    // filtering).
    for (ResultSet res : results) {
      for (Row row : res.all()) {
        allRows.add(row);
      }
    }
    mAllRows = allRows;
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    mSharedTable.release();
    CLIENT_TEST_DELEGATE.tearDownKijiTest();
  }

  // -----------------------------------------------------------------------------------------------
  // Tests after this all use a shared collection of Row objects, instead of interacting with a
  // table (this makes them faster).

  // A lot of test cases initialize a table with the same rows and then need to get all of the data
  // from those rows back into a set of Row objects to use in the creation of a KijiRowData.
  // Put all of the common code into this utility function!
  private Set<Row> getRowsAppleBananaCarrot(EntityId eid) throws IOException {
    return mAllRows;
  }

  @Test
  public void testReadWithMaxVersions() throws IOException {
    final EntityId eid = mEntityIdFactory.getEntityId("row0");

    Set<Row> allRows = getRowsAppleBananaCarrot(eid);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(1).add("family", "qual0"))
        .addColumns(ColumnsDef.create().withMaxVersions(2).add("family", "qual1"))
        .build();

    final KijiRowData input =
        new CassandraKijiRowData(mSharedTable, dataRequest, eid, allRows, null);

    assertEquals(1, input.getValues("family", "qual0").size());
    assertEquals("apple", input.getMostRecentValue("family",  "qual0").toString());
    assertEquals(2, input.getValues("family", "qual1").size());
    assertEquals("antelope", input.getValues("family", "qual1").get(6L).toString());
    assertEquals("bear", input.getValues(FAMILY, "qual1").get(5L).toString());
  }

  @Test
  public void testTypedReadWithMaxVersions() throws IOException {
    // Begin by inserting some data into the table.
    final EntityId eid = mEntityIdFactory.getEntityId("row0");

    Set<Row> allRows = getRowsAppleBananaCarrot(eid);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(1).add("family", "qual0"))
        .addColumns(ColumnsDef.create().withMaxVersions(2).add("family", "qual1"))
        .build();

    final KijiRowData input =
        new CassandraKijiRowData(mSharedTable, dataRequest, eid, allRows, null);
    assertEquals(1, input.getValues("family", "qual0").size());
    final NavigableMap<Long, CharSequence> typedValues = input.getValues("family", "qual0");
    assertEquals("apple", typedValues.get(3L).toString());
    assertEquals(2, input.getTimestamps("family", "qual1").size());
  }

  @Test
  public void testReadWithTimeRange() throws IOException {
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    Set<Row> allRows = getRowsAppleBananaCarrot(eid);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .withTimeRange(2L, 6L)
        .addColumns(ColumnsDef.create().withMaxVersions(1).add("family", "qual0"))
        .addColumns(ColumnsDef.create().withMaxVersions(2).add("family", "qual1"))
        .build();
    final KijiRowData input =
        new CassandraKijiRowData(mSharedTable, dataRequest, eid, allRows, null);
    assertEquals(1, input.getTimestamps("family", "qual0").size());
    assertEquals("apple", input.getMostRecentValue("family", "qual0").toString());
    assertEquals(2, input.getTimestamps("family", "qual1").size());
    assertEquals("bear", input.getMostRecentValue("family", "qual1").toString());
    assertEquals("cat", input.getValue("family", "qual1", 4L).toString());
  }

  // Logs the content of an CassandraKijiRowData (debug log-level).
  // @param row CassandraKijiRowData to dump.
  private static void logDebugRow(CassandraKijiRowData row) {
    if (!LOG.isDebugEnabled()) {
      return;
    }
    LOG.debug("Dumping content of CassandraKijiRowData {}", row);
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
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    Set<Row> allRows = getRowsAppleBananaCarrot(eid);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(1).add("family", "qual0"))
        .build();
    final CassandraKijiRowData input =
        new CassandraKijiRowData(mSharedTable, dataRequest, eid, allRows, null);
    logDebugRow(input);

    assertFalse(input.containsColumn("not-a-family"));
    assertTrue(input.containsColumn("family"));
    assertTrue(input.containsColumn("family", "qual0"));
    assertEquals("apple", input.getMostRecentValue("family", "qual0").toString());
  }

  @Test
  public void testReadFamilyTypes() throws IOException {
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    Set<Row> allRows = getRowsAppleBananaCarrot(eid);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().add(FAMILY, QUAL0))
        .addColumns(ColumnsDef.create().add(FAMILY, QUAL1))
        .build();

    final KijiRowData input =
        new CassandraKijiRowData(mSharedTable, dataRequest, eid, allRows, null);
    assertTrue(input.containsColumn(FAMILY, QUAL0));
    assertEquals("apple", input.getMostRecentValue(FAMILY, QUAL0).toString());
    assertTrue(input.containsColumn(FAMILY, QUAL1));
    assertEquals("antelope", input.getMostRecentValue(FAMILY, QUAL1).toString());
  }

  @Test
  public void testReadMapFamilyTypes() throws IOException {
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    Set<Row> allRows = getRowsAppleBananaCarrot(eid);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().addFamily(MAP))
        .build();

    final KijiRowData input =
        new CassandraKijiRowData(mSharedTable, dataRequest, eid, allRows, null);
    final NavigableMap<String, NavigableMap<Long, Integer>> stringsByTime =
        input.getValues(MAP);

    // Should have only two families.
    assertEquals(2, stringsByTime.size());
    assertTrue(stringsByTime.containsKey(KEY0));
    assertTrue(stringsByTime.containsKey(KEY1));

    final NavigableMap<Long, Integer> key0map = stringsByTime.get(KEY0);
    assertTrue(key0map.size() == 1);
    assertTrue(key0map.containsKey(0L));
    assertEquals(KEY0_VAL, (int)(key0map.get(0L)));

    final NavigableMap<Long, Integer> key1map = stringsByTime.get(KEY1);
    assertTrue(key0map.size() == 1);
    assertTrue(key0map.containsKey(0L));
    assertEquals(KEY1_VAL, (int)(key1map.get(0L)));

  }

  @Test
  public void testReadSpecificFamilyTypes() throws IOException {
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    Set<Row> allRows = getRowsAppleBananaCarrot(eid);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(1).addFamily("family"))
        .build();
    final KijiRowData input =
        new CassandraKijiRowData(mSharedTable, dataRequest, eid, allRows, null);
    assertTrue(input.containsColumn(FAMILY, NODEQUAL0));
    assertTrue(input.containsColumn(FAMILY, NODEQUAL1));

    final Node value0 = input.getMostRecentValue(FAMILY, NODEQUAL0);
    assertEquals("node0", value0.getLabel());
    final Node value1 = input.getMostRecentValue(FAMILY, NODEQUAL1);
    assertEquals("node1", value1.getLabel());
  }

  @Test
  public void testReadSpecificTimestampTypes() throws IOException {
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    Set<Row> allRows = getRowsAppleBananaCarrot(eid);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(
            ColumnsDef.create().withMaxVersions(Integer.MAX_VALUE).add(FAMILY, NODEQUAL1))
        .build();
    final KijiRowData input =
        new CassandraKijiRowData(mSharedTable, dataRequest, eid, allRows, null);

    assertTrue(input.containsColumn(FAMILY, NODEQUAL1));
    final NavigableMap<Long, Node> values = input.getValues(FAMILY, NODEQUAL1);
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
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    Set<Row> allRows = getRowsAppleBananaCarrot(eid);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(
            ColumnsDef.create().withMaxVersions(Integer.MAX_VALUE).add(FAMILY, NODEQUAL1))
        .build();

    final KijiRowData input =
        new CassandraKijiRowData(mSharedTable, dataRequest, eid, allRows, null);
    assertTrue(input.containsColumn(FAMILY, NODEQUAL1));
    assertEquals(
        "node0",
        ((Node) input.getValue(FAMILY, NODEQUAL1, 100L)).getLabel());
    assertEquals(
        "node1",
        ((Node) input.getValue(FAMILY, NODEQUAL1, 200L)).getLabel());
  }

  @Test
  public void testReadSpecificTypes() throws IOException {
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    Set<Row> allRows = getRowsAppleBananaCarrot(eid);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().add(FAMILY, NODEQUAL0))
        .build();

    final KijiRowData input =
        new CassandraKijiRowData(mSharedTable, dataRequest, eid, allRows, null);

    assertTrue(input.containsColumn(FAMILY, NODEQUAL0));
    final Node actual = input.getMostRecentValue(FAMILY, NODEQUAL0);
    assertEquals("node0", actual.getLabel());
  }

  @Test
  public void testIterator() throws IOException {
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    Set<Row> allRows = getRowsAppleBananaCarrot(eid);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(3).add(FAMILY, QUAL0))
        .build();

    final CassandraKijiRowData input =
        new CassandraKijiRowData(mSharedTable, dataRequest, eid, allRows, null);
    logDebugRow(input);

    assertFalse(input.containsColumn("not-a-family"));
    assertTrue(input.containsColumn(FAMILY));
    assertTrue(input.containsColumn(FAMILY, QUAL0));
    final Iterator<KijiCell<CharSequence>> cells = input.<CharSequence>iterator(FAMILY, QUAL0);
    assertTrue(cells.hasNext());
    assertEquals("apple", cells.next().getData().toString());
    assertTrue(cells.hasNext());
    assertEquals("banana", cells.next().getData().toString());
    assertTrue(cells.hasNext());
    assertEquals("carrot", cells.next().getData().toString());
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
  public void testIteratorMaxVersion() throws IOException {
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    Set<Row> allRows = getRowsAppleBananaCarrot(eid);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(2).add("family", "qual0"))
        .build();
    final KijiRowData input =
        new CassandraKijiRowData(mSharedTable, dataRequest, eid, allRows, null);

    assertFalse(input.containsColumn("not-a-family"));
    assertTrue(input.containsColumn("family"));
    assertTrue(input.containsColumn("family", "qual0"));
    final Iterator<KijiCell<CharSequence>> cells = input.<CharSequence>iterator("family", "qual0");
    assertTrue(cells.hasNext());
    assertEquals("apple", cells.next().getData().toString());
    assertTrue(cells.hasNext());
    assertEquals("banana", cells.next().getData().toString());
    assertFalse(cells.hasNext());
  }

  @Test
  public void testGroupAsIterable() throws IOException {
    final EntityId eid = mEntityIdFactory.getEntityId("row0");
    Set<Row> allRows = getRowsAppleBananaCarrot(eid);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(3).add("family", "qual0"))
        .build();

    final CassandraKijiRowData input =
        new CassandraKijiRowData(mSharedTable, dataRequest, eid, allRows, null);
    logDebugRow(input);

    assertFalse(input.containsColumn("not-a-family"));
    assertTrue(input.containsColumn("family"));
    assertTrue(input.containsColumn("family", "qual0"));
    final List<KijiCell<CharSequence>> cells =
        Lists.newArrayList(input.<CharSequence>asIterable("family", "qual0"));
    final int cellCount = cells.size();
    assertEquals("Wrong number of cells returned by asIterable.", 3, cellCount);
  }
}
