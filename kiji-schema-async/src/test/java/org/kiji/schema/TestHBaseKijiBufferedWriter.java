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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestHBaseKijiBufferedWriter extends KijiClientTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestHBaseKijiBufferedWriter.class);

  private Kiji mKiji;
  private KijiTable mTable;
  private KijiBufferedWriter mBufferedWriter;
  private KijiTableReader mReader;

  @Before
  public final void setupEnvironment() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout = KijiTableLayout.newLayout(
        KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));

    // Populate the environment.
    mKiji = new InstanceBuilder(getKiji())
        .withTable("user", layout)
            .withRow("foo")
                .withFamily("info")
                    .withQualifier("name").withValue(1L, "foo-val")
                    .withQualifier("visits").withValue(1L, 42L)
            .withRow("bar")
                .withFamily("info")
                    .withQualifier("visits").withValue(1L, 100L)
        .build();

    // Fill local variables.
    mTable = mKiji.openTable("user");
    mBufferedWriter = mTable.getWriterFactory().openBufferedWriter();
    mReader = mTable.openTableReader();
  }

  @After
  public final void cleanupEnvironment() throws IOException {
    mBufferedWriter.close();
    mReader.close();
    mTable.release();
  }

  @Test
  public void testPutWithTimestamp() throws Exception {
    final EntityId entityId = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Prepare the old value.
    KijiTableWriter writer = mTable.openTableWriter();
    writer.put(entityId, "info", "name", 123L, "old");
    writer.close();
    // Buffer the new value and confirm it has not been written.
    mBufferedWriter.put(entityId, "info", "name", 123L, "baz");
    final String actual = mReader.get(entityId, request).getValue("info", "name", 123L).toString();
    assertEquals("old", actual);

    // Flush the buffer and confirm the new value has been written.
    mBufferedWriter.flush();
    final String actual2 = mReader.get(entityId, request).getValue("info", "name", 123L).toString();
    assertEquals("baz", actual2);
  }

  @Test
  public void testSetCounter() throws Exception {
    final EntityId entityId = mTable.getEntityId("bar");
    final KijiDataRequest request = KijiDataRequest.create("info", "visits");

    // Prepare the old value.
    KijiTableWriter writer = mTable.openTableWriter();
    writer.put(entityId, "info", "visits", 1L);
    writer.close();

    // Buffer the new value and confirm it has not been written.
    mBufferedWriter.put(entityId, "info", "visits", 5L);
    KijiCell<Long> counter = mReader.get(entityId, request).getMostRecentCell("info", "visits");
    final long actual = counter.getData();
    assertEquals(1L, actual);

    // Flush the buffer and confirm the new value has been written.
    mBufferedWriter.flush();
    KijiCell<Long> counter2 = mReader.get(entityId, request).getMostRecentCell("info", "visits");
    final long actual2 = counter2.getData();
    assertEquals(5L, actual2);
  }

  @Test
  public void testDeleteColumn() throws Exception {
    final EntityId entityId = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Prepare the old value.
    KijiTableWriter writer = mTable.openTableWriter();
    writer.put(entityId, "info", "name", 123L, "not empty");
    writer.close();

    // Buffer the delete and confirm it has not been written.
    assertTrue(mReader.get(entityId, request).containsCell("info", "name", 123L));
    mBufferedWriter.deleteColumn(entityId, "info", "name");
    assertTrue(mReader.get(entityId, request).containsCell("info", "name", 123L));

    // Flush the buffer and confirm the delete has been written.
    mBufferedWriter.flush();
    assertFalse(mReader.get(entityId, request).containsCell("info", "name", 123L));
  }

  @Test
  public void testDeleteCell() throws Exception {
    final EntityId entityId = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Prepare the old value.
    KijiTableWriter writer = mTable.openTableWriter();
    writer.put(entityId, "info", "name", 123L, "not empty");
    writer.close();

    // Buffer the delete and confirm it has not been written.
    assertTrue(mReader.get(entityId, request).containsCell("info", "name", 123L));
    mBufferedWriter.deleteCell(entityId, "info", "name");
    final String actual = mReader.get(entityId, request).getValue("info", "name", 123L).toString();
    assertEquals("not empty", actual);

    // Flush the buffer and confirm the delete has been written.
    mBufferedWriter.flush();
    assertFalse(mReader.get(entityId, request).containsCell("info", "name", 123L));
  }

  @Test
  public void testSetBufferSize() throws Exception {
    final EntityId entityId = mTable.getEntityId("bar");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Add a put to the buffer.
    mBufferedWriter.put(entityId, "info", "name", 123L, "old");
    assertFalse(mReader.get(entityId, request).containsCell("info", "name", 123L));

    // Shrink the buffer, pushing the buffered put.
    mBufferedWriter.setBufferSize(1L);
    final String actual = mReader.get(entityId, request).getValue("info", "name", 123L).toString();
    assertEquals("old", actual);

    // Add a put which should commit immediately.
    mBufferedWriter.put(entityId, "info", "name", 234L, "new");
    final String actual2 = mReader.get(entityId, request).getValue("info", "name", 234L).toString();
    assertEquals("new", actual2);
  }

  @Test
  public void testBufferPutWithDelete() throws Exception {
    final EntityId oldEntityId = mTable.getEntityId("foo");
    final EntityId newEntityId = mTable.getEntityId("blope");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Buffer a delete for "foo" and a put to "blope" and confirm they have not been written.
    mBufferedWriter.deleteRow(oldEntityId);
    mBufferedWriter.put(newEntityId, "info", "name", "blopeName");
    assertTrue(mReader.get(oldEntityId, request).containsColumn("info", "name"));
    assertFalse(mReader.get(newEntityId, request).containsColumn("info", "name"));

    // Flush the buffer and ensure delete and put have been written
    mBufferedWriter.flush();
    assertFalse(mReader.get(oldEntityId, request).containsColumn("info", "name"));
    assertTrue(mReader.get(newEntityId, request).containsColumn("info", "name"));
  }
}
