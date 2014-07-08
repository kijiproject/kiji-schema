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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.AtomicKijiPutter;
import org.kiji.schema.EntityId;
import org.kiji.schema.KConstants;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiBufferedWriter;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiPager;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.layout.KijiTableLayouts;

/** Collection of different tests for C* counters. */
public class TestCassandraCounters {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraCounters.class);

  /** Use to create unique entity IDs for each test case. */
  private static final AtomicInteger TEST_ID_COUNTER = new AtomicInteger(0);
  private static final CassandraKijiClientTest CLIENT_TEST_DELEGATE = new CassandraKijiClientTest();

  private static final String MAP = "experiments";
  private static final String INFO = "info";
  private static final String VISITS = "visits";
  private static final String NAME = "name";
  private static final String Q0 = "q0";
  private static final String MR_BONKERS = "Mr Bonkers";

  private static KijiTable mTable;
  private KijiTableWriter mWriter;
  private KijiBufferedWriter mBuffered;
  private KijiTableReader mReader;
  private AtomicKijiPutter mPutter;

  /** Unique per test case -- keep tests on different rows. */
  private EntityId mEntityId;

  @BeforeClass
  public static void initShared() throws Exception {
    CLIENT_TEST_DELEGATE.setupKijiTest();
    Kiji kiji = CLIENT_TEST_DELEGATE.getKiji();
    kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
    mTable = kiji.openTable("user");
  }

  @Before
  public final void setupEnvironment() throws Exception {
    // Fill local variables.
    mBuffered = mTable.getWriterFactory().openBufferedWriter();
    mPutter = mTable.getWriterFactory().openAtomicPutter();
    mReader = mTable.openTableReader();
    mWriter = mTable.openTableWriter();
    mEntityId = mTable.getEntityId("eid-" + TEST_ID_COUNTER.getAndIncrement());
  }

  @After
  public final void cleanupEnvironment() throws IOException {
    mBuffered.close();
    mPutter.close();
    mReader.close();
    mWriter.close();
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    mTable.release();
    CLIENT_TEST_DELEGATE.tearDownKijiTest();
  }

  // Test incrementing a counter that has already been initialized.
  @Test
  public void testIncrementInitialized() throws Exception {
    mWriter.put(mEntityId, "info", "visits", 42L);

    KijiCell<Long> incrementResult = mWriter.increment(mEntityId, "info", "visits", 5L);
    final KijiDataRequest request = KijiDataRequest.create("info", "visits");
    final long postIncrementValue = incrementResult.getData();
    assertEquals(47L, postIncrementValue);
    assertEquals(KConstants.CASSANDRA_COUNTER_TIMESTAMP, incrementResult.getTimestamp());


    KijiCell<Long> counter = mReader.get(mEntityId, request).getMostRecentCell("info", "visits");
    final long actual = counter.getData();
    assertEquals(47L, actual);
    assertEquals(KConstants.CASSANDRA_COUNTER_TIMESTAMP, counter.getTimestamp());
  }

  // Test incrementing a counter that has not yet been initialized.
  @Test
  public void testIncrementUninitialized() throws Exception {
    mWriter.increment(mEntityId, MAP, Q0, 1L);
    final KijiDataRequest request = KijiDataRequest.create(MAP, Q0);
    KijiCell<Long> counter = mReader.get(mEntityId, request).getMostRecentCell(MAP, Q0);
    final long actual = counter.getData();
    assertEquals(1L, actual);
    assertEquals(KConstants.CASSANDRA_COUNTER_TIMESTAMP, counter.getTimestamp());
  }

  // Test setting a counter.
  @Test
  public void testSet() throws Exception {
    mWriter.put(mEntityId, "info", "visits", 5L);
    final KijiDataRequest request = KijiDataRequest.create("info", "visits");
    KijiCell<Long> counter = mReader.get(mEntityId, request).getMostRecentCell("info", "visits");
    assertNotNull(counter);
    final long actual = counter.getData();
    assertEquals(5L, actual);
    assertEquals(KConstants.CASSANDRA_COUNTER_TIMESTAMP, counter.getTimestamp());
  }

  // Test setting a counter with a timestamp (doesn't work).
  @Test
  public void testSetTimestamp() throws Exception {
    try {
      mWriter.put(mEntityId, "info", "visits", 5L, 5L);
      fail("An exception should have occurred.");
    } catch (UnsupportedOperationException e) {
      assertNotNull(e);
    }
  }

  // Test that we can delete a counter with a writer.
  @Test
  public void testDelete() throws Exception {
    mWriter.increment(mEntityId, MAP, Q0, 1L);
    final KijiDataRequest request = KijiDataRequest.create(MAP, Q0);
    KijiCell<Long> counter = mReader.get(mEntityId, request).getMostRecentCell(MAP, Q0);
    long actual = counter.getData();
    assertEquals(1L, actual);
    assertEquals(KConstants.CASSANDRA_COUNTER_TIMESTAMP, counter.getTimestamp());

    mWriter.deleteColumn(mEntityId, MAP, Q0);
    assertFalse(
        mReader
            .get(mEntityId, request)
            .containsCell(MAP, Q0, KConstants.CASSANDRA_COUNTER_TIMESTAMP));

    // Issue in Cassandra Kiji - After deleting a counter, that counter is *gone* forever.  No way
    // to use it again.
    mWriter.increment(mEntityId, MAP, Q0, 1L);
    counter = mReader.get(mEntityId, request).getMostRecentCell(MAP, Q0);
    assertNull(counter);
  }

  // A buffered writer can only delete counters.
  @Test
  public void testBuffered() throws Exception {
    // Buffered writer can only delete a counter.
    mWriter.put(mEntityId, MAP, Q0, 1L);
    final KijiDataRequest request = KijiDataRequest.create(MAP, Q0);
    KijiCell<Long> counter = mReader.get(mEntityId, request).getMostRecentCell(MAP, Q0);
    long actual = counter.getData();
    assertEquals(1L, actual);
    assertEquals(KConstants.CASSANDRA_COUNTER_TIMESTAMP, counter.getTimestamp());

    mBuffered.deleteColumn(mEntityId, MAP, Q0);

    // No flush yet, data should still be there:
    counter = mReader.get(mEntityId, request).getMostRecentCell(MAP, Q0);
    actual = counter.getData();
    assertEquals(1L, actual);
    assertEquals(KConstants.CASSANDRA_COUNTER_TIMESTAMP, counter.getTimestamp());

    // Flush, data is gone.
    mBuffered.flush();
    assertFalse(mReader
        .get(mEntityId, request)
        .containsCell(MAP, Q0, KConstants.CASSANDRA_COUNTER_TIMESTAMP));

    // After deleting, it is impossible to actually set a counter again.
    // It will always be zero.
    mWriter.put(mEntityId, MAP, Q0, 1L);
    counter = mReader.get(mEntityId, request).getMostRecentCell(MAP, Q0);
    assertNull(counter);


    // Writing to a counter from a buffered writing should fail (counter writes in C* Kiji require
    // doing a read followed by an increment, so we cannot do them in batch).
    try {
      mBuffered.put(mEntityId, MAP, Q0, 10L);
      fail("An Exception should have occured.");
    } catch (UnsupportedOperationException e) {
      assertNotNull(e);
    }
  }

  @Test
  public void testAtomicPutter() throws Exception {
    // An atomic putter cannot write to a counter.
    try {
      mPutter.begin(mEntityId);
      mPutter.put(MAP, Q0, 1L);
      fail("An exception should have occurred.");
    } catch (UnsupportedOperationException e) {
      assertNotNull(e);
    }
  }

  @Test
  public void testIncrementNonCounter() throws Exception {
    try {
      mWriter.increment(mEntityId, "info", "name", 1L);
      fail("An exception should have occurred.");
    } catch (UnsupportedOperationException e) {
      assertNotNull(e);
    }
  }

  // Test reading a family with counters and non-counters.
  @Test
  public void testReadMixedFamily() throws Exception {
    mWriter.put(mEntityId, "info", "visits", 42L);
    mWriter.put(mEntityId, "info", "name", MR_BONKERS);

    final KijiDataRequest dataRequest = KijiDataRequest.create("info");
    KijiRowData rowData = mReader.get(mEntityId, dataRequest);

    KijiCell<Long> counter = rowData.getMostRecentCell("info", "visits");
    KijiCell<CharSequence> name = rowData.getMostRecentCell("info", "name");

    assertNotNull(counter);
    long counterValue = counter.getData();
    assertEquals(42L, counterValue);
    assertNotNull(name);
    assertEquals(MR_BONKERS, name.getData().toString());
  }

  // Test reading multiple families, some with counters, some without.
  // Test reading multiple families with a scanner, some with counters, some without.
  @Test
  public void testReadMultipleFamiliesScan() throws Exception {
    // Initialize a counter in the map-type family.
    mWriter.increment(mEntityId, MAP, Q0, 1L);

    // Add a non-counter value to another table.
    mWriter.put(mEntityId, "info", "name", MR_BONKERS);

    // Initialize a counter in the group-type family.
    mWriter.put(mEntityId, "info", "visits", 42L);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().addFamily(MAP))
        .addColumns(ColumnsDef.create().add("info", "name"))
        .addColumns(ColumnsDef.create().add("info", "visits"))
        .build();

    KijiRowScanner scanner = mReader.getScanner(dataRequest);

    for (KijiRowData rowData : scanner) {
      if (rowData.getEntityId() == mEntityId) {
        KijiCell<Long> counter = rowData.getMostRecentCell("info", "visits");
        KijiCell<CharSequence> name = rowData.getMostRecentCell("info", "name");
        KijiCell<Long> counterMap = rowData.getMostRecentCell(MAP, Q0);

        assertNotNull(counter);
        long counterValue = counter.getData();
        assertEquals(42L, counterValue);

        assertNotNull(name);
        assertEquals(MR_BONKERS, name.getData().toString());

        assertNotNull(counterMap);
        counterValue = counterMap.getData();
        assertEquals(1L, counterValue);
      }
    }
    scanner.close();
  }

  // Test with paging!
  @Test
  public void testReadMultipleFamiliesPaging() throws Exception {
    // Initialize a counter in the map-type family.
    mWriter.increment(mEntityId, MAP, Q0, 1L);
    mWriter.put(mEntityId, "info", "name", MR_BONKERS);

    // Initialize a counter in the group-type family.
    mWriter.put(mEntityId, "info", "visits", 42L);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().addFamily(MAP))
        .addColumns(ColumnsDef.create().add("info", "name"))
        .addColumns(ColumnsDef.create().withPageSize(1).add("info", "visits"))
        .build();

    KijiRowData rowData = mReader.get(mEntityId, dataRequest);

    KijiCell<CharSequence> name = rowData.getMostRecentCell("info", "name");
    KijiCell<Long> counterMap = rowData.getMostRecentCell(MAP, Q0);


    assertNotNull(name);
    assertEquals(MR_BONKERS, name.getData().toString());

    assertNotNull(counterMap);
    final long counterValue = counterMap.getData();
    assertEquals(1L, counterValue);

    KijiPager pager = rowData.getPager("info", "visits");
    try {
      KijiCell<Long> counter = pager.next().getMostRecentCell("info", "visits");
      assertNotNull(counter);
      final long pagedCounterValue = counter.getData();
      assertEquals(42L, pagedCounterValue);
    } finally {
      pager.close();
    }
  }

  // Test that we can delete a counter with a writer.
  @Test
  public void testDeleteGroupTypeFamily() throws Exception {
    // Set a counter and some other silliness.
    mWriter.increment(mEntityId, INFO, VISITS, 1L);
    mWriter.put(mEntityId, INFO, NAME, MR_BONKERS);

    // Sanity check that the counter is there.
    final KijiDataRequest request = KijiDataRequest.create(INFO);

    KijiRowData rowData = mReader.get(mEntityId, request);
    KijiCell<Long> counter = rowData.getMostRecentCell(INFO, VISITS);
    long actual = counter.getData();
    assertEquals(1L, actual);
    assertEquals(KConstants.CASSANDRA_COUNTER_TIMESTAMP, counter.getTimestamp());
    assertEquals(MR_BONKERS, rowData.getMostRecentValue(INFO, NAME).toString());

    // Delete the entire group-type family!
    mWriter.deleteFamily(mEntityId, INFO);

    rowData = mReader.get(mEntityId, request);
    assertNotNull(rowData);
    assertNull(rowData.getMostRecentValue(INFO, VISITS));
    assertNull(rowData.getMostRecentValue(INFO, NAME));
  }

  // Test that we can delete a row with a counter in it.
  @Test
  public void testDeleteRow() throws Exception {
    // Set a counter and some other silliness.
    mWriter.increment(mEntityId, INFO, VISITS, 1L);
    mWriter.put(mEntityId, INFO, NAME, MR_BONKERS);
    mWriter.put(mEntityId, MAP, Q0, 10L);

    // Sanity check that the counter is there.
    final KijiDataRequest request = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().add(INFO, VISITS))
        .addColumns(ColumnsDef.create().add(INFO, NAME))
        .addColumns(ColumnsDef.create().addFamily(MAP))
        .build();

    KijiRowData rowData = mReader.get(mEntityId, request);
    assertEquals(1L, rowData.getMostRecentValue(INFO, VISITS));
    assertEquals(MR_BONKERS, rowData.getMostRecentValue(INFO, NAME).toString());
    assertEquals(10L, rowData.getMostRecentValue(MAP, Q0));

    // Delete the entire group-type family!
    mWriter.deleteRow(mEntityId);

    rowData = mReader.get(mEntityId, request);
    assertFalse(rowData.iterator(INFO, VISITS).hasNext());
    assertFalse(rowData.iterator(INFO, NAME).hasNext());
    assertFalse(rowData.iterator(MAP).hasNext());
  }
  // TODO: Read a counter with scanner + paging.
}
