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
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestCassandraAtomicKijiPutter {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraAtomicKijiPutter.class);

  private static KijiTable mTable;
  private KijiTableWriter mWriter;
  private AtomicKijiPutter mPutter;
  private KijiTableReader mReader;
  private EntityId mEntityId;

  /** Use to create unique entity IDs for each test case. */
  private static final AtomicInteger TEST_ID_COUNTER = new AtomicInteger(0);
  private static final CassandraKijiClientTest CLIENT_TEST_DELEGATE = new CassandraKijiClientTest();

  // Useful shortcuts for families, qualifiers, and values.
  private static final String MAP = "experiments";
  private static final String INFO = "info";
  private static final String VISITS = "visits";
  private static final String NAME = "name";
  private static final String Q0 = "q0";
  private static final String MR_BONKERS = "Mr Bonkers";
  private static final String BIRDHEAD = "Giant Robot-Birdhead";
  private static final String AMINO_MAN = "Amino Man";

  @BeforeClass
  public static void initShared() throws Exception {
    CLIENT_TEST_DELEGATE.setupKijiTest();
    Kiji kiji = CLIENT_TEST_DELEGATE.getKiji();
    // Use the counter test layout so that we can verify that trying to modify counters in an
    // atomic putter causes an exception.
    kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
    mTable = kiji.openTable("user");
  }

  @Before
  public final void setupEnvironment() throws Exception {
    // Fill local variables.
    mReader = mTable.openTableReader();
    mWriter = mTable.openTableWriter();
    mPutter = mTable.getWriterFactory().openAtomicPutter();
    mEntityId = mTable.getEntityId("eid-" + TEST_ID_COUNTER.getAndIncrement());
  }

  @After
  public final void cleanupEnvironment() throws IOException {
    mReader.close();
    mWriter.close();
    mPutter.close();
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    mTable.release();
    CLIENT_TEST_DELEGATE.tearDownKijiTest();
  }

  @Test
  public void testBasicPut() throws Exception {
    final KijiDataRequest request = KijiDataRequest.create(INFO, NAME);

    mWriter.put(mEntityId, INFO, NAME, MR_BONKERS);
    assertEquals(
        MR_BONKERS,
        mReader.get(mEntityId, request).getMostRecentValue(INFO, NAME).toString()
    );

    mPutter.begin(mEntityId);
    mPutter.put(INFO, NAME, BIRDHEAD);

    assertEquals(
        MR_BONKERS,
        mReader.get(mEntityId, request).getMostRecentValue(INFO, NAME).toString()
    );

    mPutter.commit();
    assertEquals(
        BIRDHEAD,
        mReader.get(mEntityId, request).getMostRecentValue(INFO, NAME).toString()
    );

  }

  @Test
  public void testBasicPutWithTimestamp() throws Exception {
    final KijiDataRequest request = KijiDataRequest.builder().addColumns(
        KijiDataRequestBuilder.ColumnsDef.create().withMaxVersions(100).add(INFO, NAME)).build();

    mWriter.put(mEntityId, INFO, NAME, 5L, MR_BONKERS);
    assertEquals(MR_BONKERS, mReader.get(mEntityId, request).getValue(INFO, NAME, 5L).toString());

    mPutter.begin(mEntityId);
    mPutter.put(INFO, NAME, 0L, BIRDHEAD);
    mPutter.put(INFO, NAME, 10L, AMINO_MAN);

    assertEquals(MR_BONKERS, mReader.get(mEntityId, request).getValue(INFO, NAME, 5L).toString());

    mPutter.commit();

    KijiRowData rowData = mReader.get(mEntityId, request);
    assertEquals(BIRDHEAD, rowData.getValue(INFO, NAME, 0L).toString());
    assertEquals(MR_BONKERS, rowData.getValue(INFO, NAME, 5L).toString());
    assertEquals(AMINO_MAN, rowData.getValue(INFO, NAME, 10L).toString());
  }

  @Test
  public void testSkipBegin() throws Exception {
    try {
      mPutter.put(INFO, NAME, MR_BONKERS);
      fail("An exception should have been thrown.");
    } catch (IllegalStateException e) {
      assertNotNull(e);
    }
  }

  @Test
  public void testBeginTwice() throws Exception {
    mPutter.begin(mEntityId);
    try {
      mPutter.begin(mEntityId);
      fail("An exception should have been thrown.");
    } catch (IllegalStateException e) {
      assertNotNull(e);
    }
  }

  @Test
  public void testRollback() throws Exception {
    mPutter.begin(mEntityId);
    mPutter.put(INFO, NAME, 0L, BIRDHEAD);
    mPutter.put(INFO, NAME, 10L, AMINO_MAN);
    mPutter.rollback();

    assertNull(mPutter.getEntityId());

    try {
      mPutter.commit();
      fail("An exception should have been thrown.");
    } catch (IllegalStateException e) {
      assertNotNull(e);
    }
  }

  // TODO: Any test to make sure that everything is really atomic?

  // TODO: Uncomment these tests when compare-and-set is ready.
  /*
  @Test
  public void testBasicCheckandCommit() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest visitsRequest = KijiDataRequest.create("info", "visits");

    assertEquals(42L,
        mReader.get(eid, visitsRequest).getMostRecentCell("info", "visits").getData());

    mPutter.begin(eid);
    mPutter.put("info", "visits", 45L);
    assertFalse(mPutter.checkAndCommit("info", "visits", 43L));

    assertEquals(42L,
        mReader.get(eid, visitsRequest).getMostRecentCell("info", "visits").getData());

    assertTrue(mPutter.checkAndCommit("info", "visits", 42L));

    assertEquals(45L,
        mReader.get(eid, visitsRequest).getMostRecentCell("info", "visits").getData());
  }

  @Test
  public void testCheckNullAndCommit() throws Exception {
    final EntityId eid = mTable.getEntityId("dynasty");
    final KijiDataRequest nameRequest = KijiDataRequest.create("info", "name");

    assertEquals(null, mReader.get(eid, nameRequest).getMostRecentCell("info", "name"));

    mPutter.begin(eid);
    mPutter.put("info", "name", "valois");
    assertFalse(mPutter.checkAndCommit("info", "name", "henri"));

    assertEquals(null, mReader.get(eid, nameRequest).getMostRecentCell("info", "name"));

    assertTrue(mPutter.checkAndCommit("info", "name", null));

    assertEquals("valois",
        mReader.get(eid, nameRequest).getMostRecentValue("info", "name").toString());
  }

  */
}
