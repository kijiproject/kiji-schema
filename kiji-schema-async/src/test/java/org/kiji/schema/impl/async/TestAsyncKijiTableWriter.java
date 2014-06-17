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
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestAsyncKijiTableWriter extends KijiClientTest {
  private AsyncKiji mAsyncKiji;
  private KijiTable mAsyncTable;
  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableWriter mWriter;
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

    mAsyncKiji = new AsyncKiji(mKiji.getURI());

    // For the tests, set the flushInterval to a very
    // long interval to make sure that operations are
    // not being buffered.
    mAsyncKiji.setFlushInterval(Short.MAX_VALUE);

    // Fill local variables.
    mTable = mKiji.openTable("user");
    mAsyncTable = mAsyncKiji.openTable("user");
    mWriter = mAsyncTable.openTableWriter(); //mTable.openTableWriter();
    mReader = mTable.openTableReader();
  }

  @After
  public final void cleanupEnvironment() throws IOException {
    mWriter.close();
    mReader.close();
    mTable.release();
    mAsyncTable.release();
    mAsyncKiji.release();
  }

  @Test
  public void testPutWithTimestamp() throws Exception {
    final EntityId entityId = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");
    mWriter.put(entityId, "info", "name", 123L, "baz");

    final String actual = mReader.get(entityId, request).getValue("info", "name", 123L).toString();
    assertEquals("baz", actual);
  }

  @Test
  public void testIncrement() throws Exception {
    final EntityId entityId = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "visits");
    mWriter.increment(entityId, "info", "visits", 5L);

    KijiCell<Long> counter = mReader.get(entityId, request).getMostRecentCell("info", "visits");
    final long actual = counter.getData();
    assertEquals(47L, actual);
  }

  @Test
  public void testIncrementAColumnThatIsNotACounter() throws Exception {
    // This should throw an exception because we are attempting to increment a column that
    // isn't a counter.
    try {
      mWriter.increment(mTable.getEntityId("foo"), "info", "name", 5L);
      fail("An exception should have been thrown.");
    } catch (IOException ioe) {
      assertEquals("Column 'info:name' is not a counter", ioe.getMessage());
    }
  }

  @Test
  public void testSetCounter() throws Exception {
    final EntityId entityId = mTable.getEntityId("bar");
    final KijiDataRequest request = KijiDataRequest.create("info", "visits");
    mWriter.put(entityId, "info", "visits", 5L);

    KijiCell<Long> counter = mReader.get(entityId, request).getMostRecentCell("info", "visits");
    final long actual = counter.getData();
    assertEquals(5L, actual);
  }
}