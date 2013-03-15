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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.schema.util.ResourceUtils;

public class TestHBaseKijiTableWriter {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseKijiTableWriter.class);

  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableWriter mWriter;
  private KijiTableReader mReader;

  @Before
  public void setupEnvironment() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout = KijiTableLayout.newLayout(
        KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));

    // Populate the environment.
    mKiji = new InstanceBuilder()
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
    mWriter = mTable.openTableWriter();
    mReader = mTable.openTableReader();
  }

  @After
  public void cleanupEnvironment() throws IOException {
    ResourceUtils.closeOrLog(mWriter);
    ResourceUtils.closeOrLog(mReader);
    ResourceUtils.releaseOrLog(mTable);
    mKiji.release();
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

  @Test(expected=IOException.class)
  public void testIncrementAColumnThatIsNotACounter() throws Exception {
    // This should throw an exception because we are attempting to increment a column that
    // isn't a counter.
    mWriter.increment(mTable.getEntityId("foo"), "info", "name", 5L);
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

  @Test
  public void testCheckAndPut() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest nameRequest = KijiDataRequest.create("info", "name");
    final KijiDataRequest visitsRequest = KijiDataRequest.create("info", "visits");
    mWriter.put(eid, "info", "name", 123L, "old");

    // Create the list of conditional puts.
    ArrayList<KijiCell<?>> checkedPuts = Lists.newArrayList();
    checkedPuts.add(new KijiCell<String>(
        "info", "name", 234L, new DecodedCell<String>(mTable.getLayout().getCellSchema(
            new KijiColumnName("info", "name")).getSchema(), "new")));
    checkedPuts.add(new KijiCell<Long>(
        "info", "visits", 234L, new DecodedCell<Long>(mTable.getLayout().getCellSchema(
            new KijiColumnName("info", "visits")).getSchema(), 5L)));

    // Fail a check and ensure puts are not written.
    assertFalse(mWriter.checkAndPut(eid, "info", "name", "never", checkedPuts));
    assertEquals(
        mReader.get(eid, nameRequest).getMostRecentValue("info", "name").toString(),
        "old");

    // Pass a check and ensure puts are written.
    assertTrue(mWriter.checkAndPut(eid, "info", "name", "old", checkedPuts));
    final String actualName =
        mReader.get(eid, nameRequest).getMostRecentValue("info", "name").toString();
    final KijiCell<Long> counter =
        mReader.get(eid, visitsRequest).getMostRecentCell("info", "visits");
    final long actualVisits =
        counter.getData();
    assertEquals("new", actualName);
    assertEquals(5L, actualVisits);
  }
}
