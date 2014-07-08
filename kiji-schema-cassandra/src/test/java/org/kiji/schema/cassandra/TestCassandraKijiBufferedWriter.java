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
import static org.junit.Assert.assertTrue;
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

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiBufferedWriter;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.layout.KijiTableLayouts;

/** Simple read/write tests. */
public class TestCassandraKijiBufferedWriter {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraKijiBufferedWriter.class);

  private static KijiTable mTable;

  private Kiji mKiji;
  private KijiBufferedWriter mBufferedWriter;
  private KijiTableReader mReader;
  private KijiTableWriter mWriter;

  /** Use to create unique entity IDs for each test case. */
  private static final AtomicInteger TEST_ID_COUNTER = new AtomicInteger(0);
  private static final CassandraKijiClientTest CLIENT_TEST_DELEGATE = new CassandraKijiClientTest();

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
    mBufferedWriter = mTable.getWriterFactory().openBufferedWriter();
    mReader = mTable.openTableReader();
    mWriter = mTable.openTableWriter();
    mEntityId = mTable.getEntityId("eid-" + TEST_ID_COUNTER.getAndIncrement());
  }

  @After
  public final void cleanupEnvironment() throws IOException {
    mBufferedWriter.close();
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
    mBufferedWriter.put(mEntityId, "info", "name", 0L, "Value at timestamp 0.");
    mBufferedWriter.put(mEntityId, "info", "name", 1L, "Value at timestamp 1.");

    // These have not been flushed yet, so should not be present.
    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(10).add("info", "name"))
        .build();

    KijiRowData rowData;
    rowData = mReader.get(mEntityId, dataRequest);
    assertFalse(rowData.containsCell("info", "name", 0L));

    // Now do a flush.
    mBufferedWriter.flush();

    // If you read again, the data should be present.

    // Try this as a get.
    rowData = mReader.get(mEntityId, dataRequest);
    String s = rowData.getValue("info", "name", 0L).toString();
    assertEquals(s, "Value at timestamp 0.");
  }


  @Test
  public void testPutWithTimestamp() throws Exception {
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Write a value now.
    mWriter.put(mEntityId, "info", "name", 123L, "old");

    // Buffer the new value and confirm it has not been written.
    mBufferedWriter.put(mEntityId, "info", "name", 123L, "baz");
    final String actual = mReader.get(mEntityId, request).getValue("info", "name", 123L).toString();
    assertEquals("old", actual);

    // Flush the buffer and confirm the new value has been written.
    mBufferedWriter.flush();
    final String actual2 =
        mReader.get(mEntityId, request).getValue("info", "name", 123L).toString();
    assertEquals("baz", actual2);
  }

  @Test
  public void testSetCounter() throws Exception {
    final KijiDataRequest request = KijiDataRequest.create("info", "visits");

    // Cannot set a counter from a Cassandra buffered writer.
    try {
      mBufferedWriter.put(mEntityId, "info", "visits", 5L);
      fail("Exception should have occurred.");
    } catch (UnsupportedOperationException e) {
      assertNotNull(e);
    }
  }

  @Test
  public void testDeleteColumn() throws Exception {
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Write initial value.
    mWriter.put(mEntityId, "info", "name", 123L, "not empty");

    // Buffer the delete and confirm it has not happened yet.
    assertTrue(mReader.get(mEntityId, request).containsCell("info", "name", 123L));
    mBufferedWriter.deleteColumn(mEntityId, "info", "name");
    assertTrue(mReader.get(mEntityId, request).containsCell("info", "name", 123L));

    // Flush the buffer and confirm the delete has happened.
    mBufferedWriter.flush();
    assertFalse(mReader.get(mEntityId, request).containsCell("info", "name", 123L));
  }

  @Test
  public void testDeleteCell() throws Exception {
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Write initial value.
    mWriter.put(mEntityId, "info", "name", 123L, "not empty");

    // Buffer the delete and confirm it has not happened yet.
    assertTrue(mReader.get(mEntityId, request).containsCell("info", "name", 123L));
    mBufferedWriter.deleteCell(mEntityId, "info", "name", 123L);
    final String actual = mReader.get(mEntityId, request).getValue("info", "name", 123L).toString();
    assertEquals("not empty", actual);

    // Flush the buffer and confirm the delete has happened.
    mBufferedWriter.flush();
    assertFalse(mReader.get(mEntityId, request).containsCell("info", "name", 123L));
  }

  @Test
  public void testDeleteCellNoTimestamp() throws Exception {
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Write initial value.
    mWriter.put(mEntityId, "info", "name", 123L, "not empty");

    // Buffer the delete and confirm it has not happened yet.
    assertTrue(mReader.get(mEntityId, request).containsCell("info", "name", 123L));

    // Cannot delete most-recent version of a cell in Cassandra Kiji.
    try {
      mBufferedWriter.deleteCell(mEntityId, "info", "name");
      fail("Exception should occur.");
    } catch (UnsupportedOperationException e) {
      assertNotNull(e);
    }
  }

  @Test
  public void testSetBufferSize() throws Exception {
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // TODO: Remove duplicate puts from test when buffered writer calculates buffer size correctly.
    // (Right now the buffer size is measured in C* Statements, not bytes.)

    // Add a put to the buffer.
    mBufferedWriter.put(mEntityId, "info", "name", 123L, "old");
    mBufferedWriter.put(mEntityId, "info", "name", 123L, "old");
    assertFalse(mReader.get(mEntityId, request).containsCell("info", "name", 123L));

    // Shrink the buffer, pushing the buffered put.
    mBufferedWriter.setBufferSize(1L);
    final String actual = mReader.get(mEntityId, request).getValue("info", "name", 123L).toString();
    assertEquals("old", actual);

    // Add a put which should commit immediately.
    mBufferedWriter.put(mEntityId, "info", "name", 234L, "new");
    mBufferedWriter.put(mEntityId, "info", "name", 234L, "new");
    final String actual2 =
        mReader.get(mEntityId, request).getValue("info", "name", 234L).toString();
    assertEquals("new", actual2);
  }

  @Test
  public void testBufferPutWithDelete() throws Exception {
    final EntityId oldEntityId = mTable.getEntityId("foo");
    final EntityId newEntityId = mTable.getEntityId("bar");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Initialize data for the old entity ID.
    mWriter.put(oldEntityId, "info", "name", "foo-name");

    // Buffer a delete for "foo" and a put to "bar" and confirm they have not been written.
    mBufferedWriter.deleteRow(oldEntityId);
    mBufferedWriter.put(newEntityId, "info", "name", "bar-name");
    assertTrue(mReader.get(oldEntityId, request).containsColumn("info", "name"));
    assertFalse(mReader.get(newEntityId, request).containsColumn("info", "name"));

    // Flush the buffer and ensure delete and put have been written
    mBufferedWriter.flush();
    assertFalse(mReader.get(oldEntityId, request).containsColumn("info", "name"));
    assertTrue(mReader.get(newEntityId, request).containsColumn("info", "name"));
  }

}
