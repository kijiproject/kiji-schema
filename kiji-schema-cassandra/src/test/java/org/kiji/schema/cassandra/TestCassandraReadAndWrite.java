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

package org.kiji.schema.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.HConstants;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.impl.cassandra.CassandraKiji;
import org.kiji.schema.impl.cassandra.CassandraKijiScannerOptions;
import org.kiji.schema.impl.cassandra.CassandraKijiTable;
import org.kiji.schema.impl.cassandra.CassandraKijiTableReader;
import org.kiji.schema.layout.KijiTableLayouts;

/** Simple read/write tests. */
public class TestCassandraReadAndWrite {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraReadAndWrite.class);

  private static CassandraKiji mKiji;
  private static KijiTable mTable;
  private KijiTableWriter mWriter;
  private KijiTableReader mReader;
  private EntityId mEntityId;

  /** Use to create unique entity IDs for each test case. */
  private static final AtomicInteger TEST_ID_COUNTER = new AtomicInteger(0);
  private static final CassandraKijiClientTest CLIENT_TEST_DELEGATE = new CassandraKijiClientTest();

  @BeforeClass
  public static void initShared() throws Exception {
    CLIENT_TEST_DELEGATE.setupKijiTest();
    mKiji = CLIENT_TEST_DELEGATE.getKiji();
    mKiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE_FORMATTED_EID));
    mKiji.createTable(KijiTableLayouts.getLayout(
           KijiTableLayouts.SIMPLE_FORMATTED_EID_TWO_COMPONENTS));
    mKiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE_MAP_TYPE));
    mTable = mKiji.openTable("table");
  }

  @Before
  public final void setupEnvironment() throws Exception {
    // Fill local variables.
    mReader = mTable.openTableReader();
    mWriter = mTable.openTableWriter();
    mEntityId = mTable.getEntityId("eid-" + TEST_ID_COUNTER.getAndIncrement());
  }

  @After
  public final void cleanupEnvironment() throws IOException {
    mReader.close();
    mWriter.close();
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    mTable.release();
    CLIENT_TEST_DELEGATE.tearDownKijiTest();
  }

  @Test
  public void testBasicReadAndWrite() throws Exception {

    mWriter.put(mEntityId, "family", "column", 0L, "Value at timestamp 0.");
    mWriter.put(mEntityId, "family", "column", 1L, "Value at timestamp 1.");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(2).add("family", "column"))
        .build();

    // Try this as a get.
    KijiRowData rowData = mReader.get(mEntityId, dataRequest);
    String s = rowData.getValue("family", "column", 0L).toString();
    assertEquals(s, "Value at timestamp 0.");

    // Delete the cell and make sure that this value is missing.
    mWriter.deleteCell(mEntityId, "family", "column", 0L);

    rowData = mReader.get(mEntityId, dataRequest);
    assertFalse(rowData.containsCell("family", "column", 0L));
  }

  /**
   * Test that exposes bug in timestamp ordering of KijiRowData.
   */
  @Test
  public void testReadLatestValue() throws Exception {
    // Write just a value at timestamp 0.
    mWriter.put(mEntityId, "family", "column", 0L, "Value at timestamp 0.");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(100).add("family", "column"))
        .build();

    // Do a get and verify the value (only one value should be present now).
    KijiRowData rowData = mReader.get(mEntityId, dataRequest);
    String s = rowData.getValue("family", "column", 0L).toString();
    assertEquals(s, "Value at timestamp 0.");

    // Write a second value, with a more-recent timestamp.
    mWriter.put(mEntityId, "family", "column", 1L, "Value at timestamp 1.");

    // Do a get and verify that both values are present.
    rowData = mReader.get(mEntityId, dataRequest);
    assertTrue(rowData.containsCell("family", "column", 0L));
    assertTrue(rowData.containsCell("family", "column", 1L));

    // The most-recent value should be the one with the highest timestamp!
    assertEquals(
        "Value at timestamp 1.", rowData.getMostRecentValue("family", "column").toString());
  }

  /**
   * Test the multiple writes without a timestamp will step on one other.
   */
  @Test
  public void testDefaultTimestamp() throws Exception {
    mWriter.put(mEntityId, "family", "column", "First value");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(100).add("family", "column"))
        .build();

    // Try this as a get.
    KijiRowData rowData = mReader.get(mEntityId, dataRequest);
    assertNotNull(rowData.getMostRecentValue("family", "column"));
    assertEquals(
      rowData.getMostRecentValue("family", "column").toString(),
      "First value");

    mWriter.put(mEntityId, "family", "column", "Second value");
    rowData = mReader.get(mEntityId, dataRequest);

    assertNotNull(rowData.getMostRecentValue("family", "column"));
    assertEquals(
        rowData.getMostRecentValue("family", "column").toString(),
        "Second value");
  }

  /** Try deleting an entire column. */
   @Test
  public void testDeleteColumn() throws Exception {
    mWriter.put(mEntityId, "family", "column", 0L, "Value at timestamp 0.");
    mWriter.put(mEntityId, "family", "column", 1L, "Value at timestamp 1.");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(100).add("family", "column"))
        .build();

    // Try this as a get.
    KijiRowData rowData = mReader.get(mEntityId, dataRequest);
    String s = rowData.getValue("family", "column", 0L).toString();
    assertEquals(s, "Value at timestamp 0.");

    // Delete the entire column.
    mWriter.deleteColumn(mEntityId, "family", "column");

    rowData = mReader.get(mEntityId, dataRequest);

    // Should not get any data back!
    assertFalse(rowData.containsColumn("family", "column"));
  }

  /**
   * Test using a row scanner with a token range.
   */
  @Test
  public void testRowScannerWithTokenRanges() throws Exception {
    final KijiTable table = mKiji.openTable("users");
    try {
      // Reuse these entity IDs for puts and for gets.
      final EntityId alice = table.getEntityId("Alice2");
      final EntityId bob = table.getEntityId("Bob2");
      final EntityId cathy = table.getEntityId("Cathy2");
      final EntityId david = table.getEntityId("David2");

      final String pets = "pets";
      final String cat = "cat";
      final String dog = "dog";
      final String rabbit = "rabbit";
      final String fish = "fish";
      final String bird = "bird";

      final KijiTableWriter writer = table.openTableWriter();
      try {
        // Insert some data into the table.  Give various users different pets.
        writer.put(alice, pets, cat, 0L, "Alister");
        writer.put(bob, pets, cat, 0L, "Buffy");
        writer.put(cathy, pets, cat, 0L, "Mr Cat");
        writer.put(david, pets, cat, 0L, "Dash");

        writer.put(alice, pets, dog, 0L, "Amour");
        writer.put(bob, pets, rabbit, 0L, "Bounce");
        writer.put(cathy, pets, fish, 0L, "Catfish");
        writer.put(david, pets, bird, 0L, "Da Bird");
      } finally {
        writer.close();
      }

      final KijiDataRequest dataRequest = KijiDataRequest.builder()
          .addColumns(
              ColumnsDef
                  .create()
                  .withMaxVersions(100)
                  .add(pets, cat)
                  .add(pets, dog)
                  .add(pets, rabbit)
                  .add(pets, fish)
                  .add(pets, bird)
          ).build();

      final KijiTableReader myreader = table.openTableReader();
      assert myreader instanceof CassandraKijiTableReader;
      final CassandraKijiTableReader reader = (CassandraKijiTableReader) myreader;

      try {
        // Fire up a row scanner!
        final KijiRowScanner scanner = reader.getScanner(dataRequest);
        try {
          checkScannerResults(
              alice, bob, cathy, david, scanner);
        } finally {
          scanner.close();
        }

        final KijiRowScanner scannerWithStartToken = reader.getScannerWithOptions(
            dataRequest, CassandraKijiScannerOptions.withStartToken(Long.MIN_VALUE));
        try {
          checkScannerResults(
              alice, bob, cathy, david, scannerWithStartToken);
        } finally {
          scannerWithStartToken.close();
        }

        final KijiRowScanner scannerWithStopToken = reader.getScannerWithOptions(
            dataRequest, CassandraKijiScannerOptions.withStopToken(Long.MAX_VALUE));
        try {
          checkScannerResults(
              alice, bob, cathy, david, scannerWithStopToken);
        } finally {
          scannerWithStopToken.close();
        }

        final KijiRowScanner scannerWithTokens = reader.getScannerWithOptions(
            dataRequest, CassandraKijiScannerOptions.withTokens(Long.MIN_VALUE, Long.MAX_VALUE));
        try {
          checkScannerResults(
              alice, bob, cathy, david, scannerWithTokens);
        } finally {
          scannerWithTokens.close();
        }

      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }

  private void checkScannerResults(
      EntityId alice,
      EntityId bob,
      EntityId cathy,
      EntityId david,
      KijiRowScanner scanner) {
    final String pets = "pets";
    final String cat = "cat";
    final String dog = "dog";
    final String rabbit = "rabbit";
    final String fish = "fish";
    final String bird = "bird";

    // There is a small enough amount of data that we can just put all of the rows into a hash
    // from entity ID to row data.
    HashMap<EntityId, KijiRowData> allData = new HashMap<EntityId, KijiRowData>();

    for (KijiRowData row : scanner) {
      EntityId eid = row.getEntityId();
      assert (!allData.containsKey(eid));
      allData.put(eid, row);
    }

    assertTrue(allData.containsKey(alice));
    assertTrue(allData.containsKey(bob));
    assertTrue(allData.containsKey(cathy));
    assertTrue(allData.containsKey(david));

    assertTrue(allData.get(alice).containsColumn(pets, cat));
    assertTrue(allData.get(alice).containsColumn(pets, dog));
    assertFalse(allData.get(alice).containsColumn(pets, rabbit));
    assertFalse(allData.get(alice).containsColumn(pets, fish));
    assertFalse(allData.get(alice).containsColumn(pets, bird));

    assertTrue(allData.get(bob).containsColumn(pets, cat));
    assertTrue(allData.get(bob).containsColumn(pets, rabbit));
    assertFalse(allData.get(bob).containsColumn(pets, dog));
    assertFalse(allData.get(bob).containsColumn(pets, fish));
    assertFalse(allData.get(bob).containsColumn(pets, bird));

    assertTrue(allData.get(cathy).containsColumn(pets, cat));
    assertTrue(allData.get(cathy).containsColumn(pets, fish));
    assertFalse(allData.get(cathy).containsColumn(pets, dog));
    assertFalse(allData.get(cathy).containsColumn(pets, rabbit));
    assertFalse(allData.get(cathy).containsColumn(pets, bird));

    assertTrue(allData.get(david).containsColumn(pets, cat));
    assertTrue(allData.get(david).containsColumn(pets, bird));
    assertFalse(allData.get(david).containsColumn(pets, dog));
    assertFalse(allData.get(david).containsColumn(pets, rabbit));
    assertFalse(allData.get(david).containsColumn(pets, fish));
  }

  /**
   * Test corner cases for a row scanner.  The row scanner may have to issue multiple discrete
   * queries for a single `KijiDataRequest`.  Each query will return an iterator over Cassandra
   * `Row` objects, which the `KijiRowScanner` will then assemble back into `KijiRowData` objects.
   *
   * Below we test a corner case in which several Kiji rows have data for different column
   * qualifiers.  We structure the data request such that we specifically request data for the
   * different qualifiers, meaning that we will see a different Cassandra query for each qualifier.
   * Thus, the `KijiRowScanner` will get back a different `Row` iterator for each unique qualifier,
   * and then have to assemble them back together.
   */
  @Test
  public void testRowScannerSparseData() throws Exception {
    final KijiTable table = mKiji.openTable("users");
    try {

      // Reuse these entity IDs for puts and for gets.
      final EntityId alice = table.getEntityId("Alice");
      final EntityId bob = table.getEntityId("Bob");
      final EntityId cathy = table.getEntityId("Cathy");
      final EntityId david = table.getEntityId("David");

      final String pets = "pets";
      final String cat = "cat";
      final String dog = "dog";
      final String rabbit = "rabbit";
      final String fish = "fish";
      final String bird = "bird";

      final KijiTableWriter writer = table.openTableWriter();
      try {
        // Insert some data into the table.  Give various users different pets.
        writer.put(alice, pets, cat, 0L, "Alister");
        writer.put(bob, pets, cat, 0L, "Buffy");
        writer.put(cathy, pets, cat, 0L, "Mr Cat");
        writer.put(david, pets, cat, 0L, "Dash");

        writer.put(alice, pets, dog, 0L, "Amour");
        writer.put(bob, pets, rabbit, 0L, "Bounce");
        writer.put(cathy, pets, fish, 0L, "Catfish");
        writer.put(david, pets, bird, 0L, "Da Bird");
      } finally {
        writer.close();
      }

      final KijiDataRequest dataRequest = KijiDataRequest.builder()
          .addColumns(
              ColumnsDef
                  .create()
                  .withMaxVersions(100)
                  .add(pets, cat)
                  .add(pets, dog)
                  .add(pets, rabbit)
                  .add(pets, fish)
                  .add(pets, bird)
          ).build();

      final KijiTableReader reader = table.openTableReader();
      try {
        // Fire up a row scanner!
        final KijiRowScanner scanner = reader.getScanner(dataRequest);
        try {
          // There is a small enough amount of data that we can just put all of the rows into a hash
          // from entity ID to row data.
          HashMap<EntityId, KijiRowData> allData = new HashMap<EntityId, KijiRowData>();

          for (KijiRowData row : scanner) {
            EntityId eid = row.getEntityId();
            assert (!allData.containsKey(eid));
            allData.put(eid, row);
          }

          assertTrue(allData.containsKey(alice));
          assertTrue(allData.containsKey(bob));
          assertTrue(allData.containsKey(cathy));
          assertTrue(allData.containsKey(david));

          assertTrue(allData.get(alice).containsColumn(pets, cat));
          assertTrue(allData.get(alice).containsColumn(pets, dog));
          assertFalse(allData.get(alice).containsColumn(pets, rabbit));
          assertFalse(allData.get(alice).containsColumn(pets, fish));
          assertFalse(allData.get(alice).containsColumn(pets, bird));

          assertTrue(allData.get(bob).containsColumn(pets, cat));
          assertTrue(allData.get(bob).containsColumn(pets, rabbit));
          assertFalse(allData.get(bob).containsColumn(pets, dog));
          assertFalse(allData.get(bob).containsColumn(pets, fish));
          assertFalse(allData.get(bob).containsColumn(pets, bird));

          assertTrue(allData.get(cathy).containsColumn(pets, cat));
          assertTrue(allData.get(cathy).containsColumn(pets, fish));
          assertFalse(allData.get(cathy).containsColumn(pets, dog));
          assertFalse(allData.get(cathy).containsColumn(pets, rabbit));
          assertFalse(allData.get(cathy).containsColumn(pets, bird));

          assertTrue(allData.get(david).containsColumn(pets, cat));
          assertTrue(allData.get(david).containsColumn(pets, bird));
          assertFalse(allData.get(david).containsColumn(pets, dog));
          assertFalse(allData.get(david).containsColumn(pets, rabbit));
          assertFalse(allData.get(david).containsColumn(pets, fish));
        } finally {
          scanner.close();
        }
      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }

  // Attempt to read a value that is not there.  Should return null, not throw an exception!
  @Test
  public void testReadMissingValue() throws Exception {
    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(100).add("family", "column"))
        .build();

    KijiRowData rowData = mReader.get(mEntityId, dataRequest);
    assertNull(rowData.getValue("family", "column", 0L));
  }

  @Test
  public void testDeleteFamily() throws Exception {
    mWriter.put(mEntityId, "family", "column", 0L, "Value at timestamp 0.");
    mWriter.put(mEntityId, "family", "column", 1L, "Value at timestamp 1.");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(100).add("family", "column"))
        .build();

    // Try this as a get.
    KijiRowData rowData = mReader.get(mEntityId, dataRequest);
    String s = rowData.getValue("family", "column", 0L).toString();
    assertEquals(s, "Value at timestamp 0.");

    // Delete the entire family.
    mWriter.deleteFamily(mEntityId, "family");

    rowData = mReader.get(mEntityId, dataRequest);

    // Should not get any data back!
    assertFalse(rowData.containsColumn("family", "column"));
  }

  @Test
  public void testDeleteFamilyWithTimestamp() throws Exception {
    try {
      // Delete the entire family.
      mWriter.deleteFamily(mEntityId, "family", 0L);
      fail("Exception should have occurred.");
    } catch (UnsupportedOperationException e) {
      assertNotNull(e);
    }
  }

  @Test
  public void testDeleteColumnWithTimestamp() throws Exception {
    try {
      // Delete the entire family.
      mWriter.deleteColumn(mEntityId, "family", "column", 0L);
      fail("Exception should have occurred.");
    } catch (UnsupportedOperationException e) {
      assertNotNull(e);
    }
  }

  @Test
  public void testDeleteRow() throws Exception {
    mWriter.put(mEntityId, "family", "column", 0L, "Value at timestamp 0.");
    mWriter.put(mEntityId, "family", "column", 1L, "Value at timestamp 1.");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(100).add("family", "column"))
        .build();

    // Try this as a get.
    KijiRowData rowData = mReader.get(mEntityId, dataRequest);
    String s = rowData.getValue("family", "column", 0L).toString();
    assertEquals(s, "Value at timestamp 0.");

    // Delete the entire row.
    mWriter.deleteRow(mEntityId);

    rowData = mReader.get(mEntityId, dataRequest);

    // Should not get any data back!
    assertFalse(rowData.containsColumn("family", "column"));
  }

  @Test
  public void testDeleteRowWithTimestamp() throws Exception {
    try {
      mWriter.deleteRow(mEntityId, 0L);
      fail("Exception should have occurred.");
    } catch (UnsupportedOperationException e) {
      assertNotNull(e);
    }
  }

  @Test
  public void testBulkGet() throws Exception {
    // Write data for a bunch of rows.
    final int numRows = 10;
    ArrayList<EntityId> entityIds = new ArrayList<EntityId>();
    for (int rowNum = 0; rowNum < numRows; rowNum++) {
      EntityId eid = mTable.getEntityId("row#" + rowNum);
      mWriter.put(eid, "family", "column", "This is row " + rowNum + ".");
      entityIds.add(eid);
    }

    KijiDataRequest request = KijiDataRequest.create("family", "column");

    List<KijiRowData> dataList = mReader.bulkGet(entityIds, request);

    assertEquals(numRows, dataList.size());

    for (int rowNum = 0; rowNum < numRows; rowNum++) {
      KijiRowData data = dataList.get(rowNum);
      EntityId eid = mTable.getEntityId("row#" + rowNum);
      assertEquals(eid, data.getEntityId());
      assertEquals(
          "This is row " + rowNum + ".",
          data.getMostRecentValue("family", "column").toString()
      );
    }
  }

  @Test
  public void testHConstantsLatestTimestamp() throws Exception {
    mWriter.put(mEntityId, "family", "column", HConstants.LATEST_TIMESTAMP, "latest");
    //mWriter.put(mEntityId, "family", "column", "latest");

    final KijiDataRequest dataRequest = KijiDataRequest.create("family", "column");

    // Sanity check that the write succeeded.
    KijiRowData rowData = mReader.get(mEntityId, dataRequest);
    KijiCell cell = rowData.getMostRecentCell("family", "column");
    assertNotNull(cell);
    assertEquals(cell.getData().toString(), "latest");

    // Let's just check that the timestamp is within 5 minutes of the system time.
    Long storedTimestamp = cell.getTimestamp();

    Long currentTime = System.currentTimeMillis();

    assertTrue(currentTime > storedTimestamp);
    assertTrue(currentTime - storedTimestamp < TimeUnit.MINUTES.toMillis(5));
  }

  /**
   * Test using a row scanner in a table a multi-component entity ID.
   */
  @Test
  public void testRowScannerWithMultiEntityIDTable() throws Exception {
    final CassandraKijiTable table = mKiji.openTable("table_two_components");
    try {
      // Reuse these entity IDs for puts and for gets.
      final EntityId alice = table.getEntityId("A", "Alice");
      final EntityId bob = table.getEntityId("B", "Bob");
      final EntityId cathy = table.getEntityId("C", "Cathy");
      final EntityId david = table.getEntityId("D", "David");

      final String family = "family";
      final String column = "column";

      final String cat = "cat";
      final String dog = "dog";
      final String fish = "fish";
      final String bird = "bird";

      final KijiTableWriter writer = table.openTableWriter();
      try {
        // Insert some data into the table.  Give various users different pets.
        writer.put(alice, family, column, 0L, cat);
        writer.put(alice, family, column, 1L, cat);
        writer.put(bob, family, column, 0L, dog);
        writer.put(cathy, family, column, 0L, fish);
        writer.put(david, family, column, 0L, bird);
      } finally {
        writer.close();
      }

      final KijiDataRequest dataRequest = KijiDataRequest.create(family, column);
      final CassandraKijiTableReader reader = table.openTableReader();

      try {
        // Fire up a row scanner!
        final KijiRowScanner scanner = reader.getScanner(dataRequest);
        try {
          HashMap<EntityId, KijiRowData> allData = new HashMap<EntityId, KijiRowData>();

          for (KijiRowData row : scanner) {
            EntityId eid = row.getEntityId();
            assert (!allData.containsKey(eid));
            allData.put(eid, row);
          }
          assertEquals(4, allData.size());
        } finally {
          scanner.close();
        }
      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }
}

