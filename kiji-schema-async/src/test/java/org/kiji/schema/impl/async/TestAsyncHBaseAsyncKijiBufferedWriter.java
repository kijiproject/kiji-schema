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

package org.kiji.schema.impl.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.AsyncKijiBufferedWriter;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiFuture;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestAsyncHBaseAsyncKijiBufferedWriter extends KijiClientTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestAsyncHBaseAsyncKijiBufferedWriter.class);
  private AsyncHBaseKiji mAsyncHBaseKiji;
  private KijiTable mAsyncTable;
  private Kiji mKiji;
  private KijiTable mTable;
  private AsyncKijiBufferedWriter mBufferedWriter;
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

    mAsyncHBaseKiji = new AsyncHBaseKiji(mKiji.getURI());

    // For the tests, set the flushInterval to a very
    // long interval to make sure that operations are
    // buffered and are only added once flush() is called.
    mAsyncHBaseKiji.setFlushInterval(Short.MAX_VALUE);

    // Fill local variables.
    mTable = mKiji.openTable("user");
    mAsyncTable = mAsyncHBaseKiji.openTable("user");
    mBufferedWriter = mAsyncTable.getWriterFactory().openAsyncBufferedWriter();
    mReader = mAsyncTable.openTableReader();
  }

  @After
  public final void cleanupEnvironment() throws IOException {
    mBufferedWriter.close();
    mReader.close();
    mTable.release();
    mAsyncTable.release();
    mAsyncHBaseKiji.release();
  }

  //@Test
  public void testPutWithTimestamp() throws Exception {
    final EntityId entityId = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Prepare the old value.
    KijiTableWriter writer = mTable.openTableWriter();
    writer.put(entityId, "info", "name", 123L, "old");
    writer.close();
    // Buffer the new value and confirm it has not been written.
    mBufferedWriter.put(entityId, KijiColumnName.create("info", "name"), 123L, "baz");
    final String actual = mReader.get(entityId, request).getValue("info", "name", 123L).toString();
    assertEquals("old", actual);

    // Flush the buffer and confirm the new value has been written.
    mBufferedWriter.flush().get();
    final String actual2 = mReader.get(entityId, request).getValue("info", "name", 123L).toString();
    assertEquals("baz", actual2);
  }

  //@Test
  public void testSetCounter() throws Exception {
    final EntityId entityId = mTable.getEntityId("bar");
    final KijiDataRequest request = KijiDataRequest.create("info", "visits");

    // Prepare the old value.
    KijiTableWriter writer = mTable.openTableWriter();
    writer.put(entityId, "info", "visits", 1L);
    writer.close();

    // Buffer the new value and confirm it has not been written.
    mBufferedWriter.put(entityId, KijiColumnName.create("info", "visits"), 5L);
    KijiCell<Long> counter = mReader.get(entityId, request).getMostRecentCell("info", "visits");
    final long actual = counter.getData();
    assertEquals(1L, actual);

    // Flush the buffer and confirm the new value has been written.
    mBufferedWriter.flush().get();
    KijiCell<Long> counter2 = mReader.get(entityId, request).getMostRecentCell("info", "visits");
    final long actual2 = counter2.getData();
    assertEquals(5L, actual2);
  }

  //@Test
  public void testDeleteColumn() throws Exception {
    final EntityId entityId = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Prepare the old value.
    KijiTableWriter writer = mTable.openTableWriter();
    writer.put(entityId, "info", "name", 123L, "not empty");
    writer.close();

    // Buffer the delete and confirm it has not been written.
    assertTrue(mReader.get(entityId, request).containsCell("info", "name", 123L));
    mBufferedWriter.deleteColumn(entityId, KijiColumnName.create("info", "name"));
    assertTrue(mReader.get(entityId, request).containsCell("info", "name", 123L));

    // Flush the buffer and confirm the delete has been written.
    mBufferedWriter.flush().get();
    assertFalse(mReader.get(entityId, request).containsCell("info", "name", 123L));
  }

  //@Test
  public void testDeleteCell() throws Exception {
    final EntityId entityId = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Prepare the old value.
    KijiTableWriter writer = mTable.openTableWriter();
    writer.put(entityId, "info", "name", 123L, "not empty");
    writer.close();

    // Buffer the delete and confirm it has not been written.
    assertTrue(mReader.get(entityId, request).containsCell("info", "name", 123L));
    mBufferedWriter.deleteCell(entityId, KijiColumnName.create("info", "name"));
    final String actual = mReader.get(entityId, request).getValue("info", "name", 123L).toString();
    assertEquals("not empty", actual);

    // Flush the buffer and confirm the delete has been written.
    mBufferedWriter.flush().get();
    assertFalse(mReader.get(entityId, request).containsCell("info", "name", 123L));
  }

  //@Test
  public void testBufferPutWithDelete() throws Exception {
    final EntityId oldEntityId = mTable.getEntityId("foo");
    final EntityId newEntityId = mTable.getEntityId("blope");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Buffer a delete for "foo" and a put to "blope" and confirm they have not been written.
    mBufferedWriter.deleteRow(oldEntityId);
    mBufferedWriter.put(newEntityId, KijiColumnName.create("info", "name"), "blopeName");
    assertTrue(mReader.get(oldEntityId, request).containsColumn("info", "name"));
    assertFalse(mReader.get(newEntityId, request).containsColumn("info", "name"));

    // Flush the buffer and ensure delete and put have been written
    mBufferedWriter.flush().get();
    assertFalse(mReader.get(oldEntityId, request).containsColumn("info", "name"));
    assertTrue(mReader.get(newEntityId, request).containsColumn("info", "name"));
  }

  //@Test
  public void testFamilyDelete() throws Exception {

    // Populate the environment.
    final String tableName = "mapInputTest";
    final String familyName = "mapFamily";
    final KijiTableLayout layout =
        KijiTableLayout.newLayout(KijiTableLayouts.getLayout(KijiTableLayouts.SQOOP_EXPORT_MAP_TEST));
    Kiji kiji = new InstanceBuilder(mKiji)
        .withTable(tableName, layout)
          .withRow("foo")
            .withFamily(familyName)
              .withQualifier("barney").withValue(1L, "doggy1")
              .withQualifier("clance").withValue(1L, "doggy2")
        .build();

    AsyncHBaseKiji asyncHBaseKiji = new AsyncHBaseKiji(kiji.getURI());

    // For the tests, set the flushInterval to a very
    // long interval to make sure that operations are
    // buffered and are only added once flush() is called.
    asyncHBaseKiji.setFlushInterval(Short.MAX_VALUE);

    // Fill local variables.
    KijiTable table = kiji.openTable(tableName);
    KijiTable asyncTable = asyncHBaseKiji.openTable(tableName);
    AsyncKijiBufferedWriter bufferedWriter =
        asyncTable.getWriterFactory().openAsyncBufferedWriter();
    KijiTableReader reader = table.openTableReader();

    final EntityId eid = table.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create(familyName);
    final KijiColumnName deleteFamily = KijiColumnName.create(familyName);
    bufferedWriter.deleteColumn(eid, deleteFamily);
    // Should not yet be deleted.
    assertTrue(reader.get(eid, request).containsColumn(familyName));
    // After the call to flush, the family should be deleted.
    bufferedWriter.flush().get();
    assertFalse(reader.get(eid, request).containsColumn(familyName));

    bufferedWriter.close();
    reader.close();
    asyncTable.release();
    table.release();
    asyncHBaseKiji.release();
  }

  @Test
  public void testAsynchronousPutting() throws Exception {
    final EntityId entityId = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");
    final CountDownLatch latch = new CountDownLatch(1);
    final boolean success;

    // Prepare the old value.
    KijiTableWriter writer = mTable.openTableWriter();
    writer.put(entityId, "info", "name", 123L, "old");
    writer.close();
    // Buffer the new value and confirm it has not been written.
    KijiFuture<Object> future = mBufferedWriter.put(entityId, KijiColumnName.create("info", "name"), 123L, "baz");

    Futures.addCallback(future, new FutureCallback<Object>() {
          @Override
          public void onSuccess(@Nullable final Object o) {
            latch.countDown();
          }

          @Override
          public void onFailure(final Throwable throwable) {
            Assert.fail("Put should not fail");
            latch.countDown();
          }
        });

    //The put should still be in the buffer.
    final String actual = mReader.get(entityId, request).getValue("info", "name", 123L).toString();
    assertEquals("old", actual);

    // Flush the buffer and confirm the new value has been written.
    mBufferedWriter.flush();
    latch.await(5, TimeUnit.SECONDS);
    assertEquals("baz", mReader.get(entityId, request).getValue("info", "name", 123L).toString());
  }
}
