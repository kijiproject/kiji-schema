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
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.layout.KijiTableLayouts;

/** Simple read/write tests. */
public class TestCassandraTimeToLive {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraTimeToLive.class);

  private static KijiTable mTable;
  private KijiTableWriter mWriter;
  private KijiTableReader mReader;
  private EntityId mEntityId;

  // This is the family in this table layout with
  private static final String FAMILY = "info";
  private static final String QUALIFIER = "name";
  private static final String VALUE = "Mr Bonkers";
  private static final Long TIMESTAMP = 0L;

  /** Use to create unique entity IDs for each test case. */
  private static final AtomicInteger TEST_ID_COUNTER = new AtomicInteger(0);
  private static final CassandraKijiClientTest CLIENT_TEST_DELEGATE = new CassandraKijiClientTest();

  @BeforeClass
  public static void initShared() throws Exception {
    CLIENT_TEST_DELEGATE.setupKijiTest();
    Kiji kiji = CLIENT_TEST_DELEGATE.getKiji();
    kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.TTL_TEST));
    mTable = kiji.openTable("ttl_test");
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
  public void testTimeToLive() throws Exception {

    // TTL is 10 seconds for this cell.
    mWriter.put(mEntityId, FAMILY, QUALIFIER, TIMESTAMP, VALUE);

    final KijiDataRequest dataRequest = KijiDataRequest.create(FAMILY, QUALIFIER);

    // The data should be there now!
    KijiRowData rowData = mReader.get(mEntityId, dataRequest);
    String s = rowData.getValue(FAMILY, QUALIFIER, TIMESTAMP).toString();
    assertEquals(s, VALUE);

    // Wait for ten seconds.
    Thread.sleep(10 * 1000);

    rowData = mReader.get(mEntityId, dataRequest);
    assertFalse(rowData.containsCell(FAMILY, QUALIFIER, TIMESTAMP));
  }

}

