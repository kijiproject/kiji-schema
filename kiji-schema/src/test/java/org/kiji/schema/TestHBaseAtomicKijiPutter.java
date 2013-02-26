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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestHBaseAtomicKijiPutter extends KijiClientTest {
  private Kiji mKiji;
  private KijiTable mTable;
  private AtomicKijiPutter mPutter;
  private KijiTableReader mReader;

  @Before
  public void setupEnvironment() throws Exception {
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
    mPutter = mTable.getWriterFactory().openAtomicPutter();
    mReader = mTable.openTableReader();
  }

  @After
  public void cleanupEnvironment() throws IOException {
    mPutter.close();
    mReader.close();
    mTable.release();
  }

  @Test
  public void testBasicPut() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "visits");

    mPutter.begin(eid);
    assertEquals(
        mReader.get(eid, request).getMostRecentCell("info", "visits").getData(), 42L);
    mPutter.put("info", "visits", 45L);
    assertEquals(
        mReader.get(eid, request).getMostRecentCell("info", "visits").getData(), 42L);
    mPutter.commit("default");
    assertEquals(
        mReader.get(eid, request).getMostRecentCell("info", "visits").getData(), 45L);
  }

  @Test
  public void testPutWithTimestamp() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "visits");

    assertEquals(
        mReader.get(eid, request).getMostRecentCell("info", "visits").getData(), 42L);

    mPutter.begin(eid);
    mPutter.put("info", "visits", 10L, 45L);
    mPutter.commit("default");

    assertEquals(
        mReader.get(eid, request).getMostRecentCell("info", "visits").getData(), 45L);

    mPutter.begin(eid);
    mPutter.put("info", "visits", 5L, 50L);
    mPutter.commit("default");

    assertEquals(
        mReader.get(eid, request).getMostRecentCell("info", "visits").getData(), 45L);
  }

  @Test
  public void testCompoundPut() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest nameRequest = KijiDataRequest.create("info", "name");
    final KijiDataRequest visitsRequest = KijiDataRequest.create("info", "visits");

    assertEquals(
        mReader.get(eid, nameRequest).getMostRecentCell("info", "name").getData().toString(),
        "foo-val");
    assertEquals(
        mReader.get(eid, visitsRequest).getMostRecentCell("info", "visits").getData(), 42L);

    mPutter.begin(eid);
    mPutter.put("info", "name", "foo-name");
    mPutter.put("info", "visits", 45L);
    mPutter.commit("default");

    assertEquals(
        mReader.get(eid, nameRequest).getMostRecentCell("info", "name").getData().toString(),
        "foo-name");
    assertEquals(
        mReader.get(eid, visitsRequest).getMostRecentCell("info", "visits").getData(), 45L);
  }

  @Test
  public void testBasicCheckandCommit() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest visitsRequest = KijiDataRequest.create("info", "visits");

    assertEquals(
        mReader.get(eid, visitsRequest).getMostRecentCell("info", "visits").getData(), 42L);

    mPutter.begin(eid);
    mPutter.put("info", "visits", 45L);
    assertFalse(mPutter.checkAndCommit("info", "visits", 43L));

    assertEquals(
        mReader.get(eid, visitsRequest).getMostRecentCell("info", "visits").getData(), 42L);

    assertTrue(mPutter.checkAndCommit("info", "visits", 42L));

    assertEquals(
        mReader.get(eid, visitsRequest).getMostRecentCell("info", "visits").getData(), 45L);
  }

  @Test(expected=IllegalStateException.class)
  public void testSkipBegin() throws Exception {
    mPutter.put("info", "visits", 45L);
  }

  @Test(expected=IllegalStateException.class)
  public void testBeginTwice() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");

    mPutter.begin(eid);
    mPutter.begin(eid);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testCrossLocalityGroup() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");

    mPutter.begin(eid);
    mPutter.put("info", "visits", 45L);
    mPutter.commit("notDefault");
  }

  @Test(expected=IllegalStateException.class)
  public void testRollback() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");

    mPutter.begin(eid);
    mPutter.put("info", "visits", 45L);
    mPutter.put("info", "name", "foo-name");
    mPutter.rollback();

    assertEquals(null, mPutter.getEntityId());
    mPutter.commit("default");
  }
}
