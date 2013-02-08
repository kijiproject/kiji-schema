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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;

/**
 * Integration test for formatted row keys.
 */
public class IntegrationTestFormattedRowKeys
    extends AbstractKijiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(
      IntegrationTestFormattedRowKeys.class);

  public static final String TABLE_NAME = "table";

  private Kiji mKiji = null;
  private KijiTable mTable = null;
  private KijiTableWriter mWriter = null;
  private KijiTableReader mReader = null;

  private final KijiDataRequestBuilder mBuilder = KijiDataRequest.builder();
  private KijiDataRequest mDataRequest = null;

  @Before
  public void setup() throws Exception {
    mKiji = Kiji.Factory.open(getKijiURI());
    LOG.info("Creating test table.");
    mKiji.createTable(TABLE_NAME, KijiTableLayouts.getTableLayout(KijiTableLayouts.FORMATTED_RKF));
    mTable = mKiji.openTable(TABLE_NAME);
    mWriter = mTable.openTableWriter();
    mReader = mTable.openTableReader();
    mBuilder.addColumns().withMaxVersions(1).add("family", "column");
    mDataRequest = mBuilder.build();
  }

  @Test
  public void testFormattedRKF() throws IOException {
    LOG.info("Populating test table.");

    mWriter.put(mTable.getEntityId(new String("x"), new String("a"), new String("bc")),
        "family", "column", "1");

    mWriter.flush();

    KijiRowData result = mReader.get(mTable.getEntityId(new String("x"), new String("a"),
        new String("bc")), mDataRequest);
    assertTrue(result.containsColumn("family", "column"));
    assertEquals(3, result.getEntityId().getComponents().size());
    assertEquals("x", result.getEntityId().getComponentByIndex(0));
    assertEquals("a", result.getEntityId().getComponentByIndex(1));
    assertEquals("bc", result.getEntityId().getComponentByIndex(2));

    String res = result.getMostRecentValue("family", "column").toString();
    LOG.info(res);
    assertEquals("1", res);

    mWriter.deleteRow(result.getEntityId());
    result = mReader.get(mTable.getEntityId(new String("x"), new String("a"),
        new String("bc")), mDataRequest);
    assertFalse(result.containsColumn("family", "column"));
  }

  @Test
  public void testFormattedKeyOrdering() throws IOException {
    List<EntityId> expected = new ArrayList<EntityId>();

    // We expect this to be the ordering for the keys below.
    // x is the dummy key to make all hashes equal.
    expected.add(mTable.getEntityId(new String("x"), new String("a"), new String("bc")));
    expected.add(mTable.getEntityId(new String("x"), new String("a0"), new String("aa")));
    expected.add(mTable.getEntityId(new String("x"), new String("a0"), new String("aa"),
        Integer.valueOf(-1)));
    expected.add(mTable.getEntityId(new String("x"), new String("a0"), new String("aa"),
        Integer.valueOf(0)));
    expected.add(mTable.getEntityId(new String("x"), new String("a0"), new String("aa"),
        Integer.valueOf(Integer.MAX_VALUE)));
    expected.add(mTable.getEntityId(new String("x"), new String("a0"), new String("aa0"),
        Integer.valueOf(-1)));
    expected.add(mTable.getEntityId(new String("x"), new String("a0"), new String("aa0"),
        Integer.valueOf(0)));
    expected.add(mTable.getEntityId(new String("x"), new String("a0"), new String("aa0"),
        Integer.valueOf(0), Long.valueOf(Long.MIN_VALUE)));
    expected.add(mTable.getEntityId(new String("x"), new String("a0"), new String("aa0"),
        Integer.valueOf(0), Long.valueOf(0)));
    expected.add(mTable.getEntityId(new String("x"), new String("a0"), new String("aa0"),
        Integer.valueOf(0), Long.valueOf(Long.MAX_VALUE)));
    expected.add(mTable.getEntityId(new String("x"), new String("a0"), new String("aa0"),
        Integer.valueOf(Integer.MAX_VALUE)));

    for (EntityId eid: expected) {
      mWriter.put(eid, "family", "column", "1");
    }

    mWriter.flush();

    final KijiRowScanner scanner = mReader.getScanner(mDataRequest);
    final Iterator<KijiRowData> resultIterator = scanner.iterator();

    final Iterator<EntityId> expectedIterator = expected.iterator();
    while (resultIterator.hasNext() && expectedIterator.hasNext()) {
      byte[] expectedhb = expectedIterator.next().getHBaseRowKey();
      byte[] resultkey = resultIterator.next().getEntityId().getHBaseRowKey();
      System.out.println("expected");
      for (byte b: expectedhb) {
        System.out.format("%x ", b);
      }
      System.out.format("\n");
      System.out.println("result");
      for (byte b: resultkey) {
        System.out.format("%x ", b);
      }
      System.out.format("\n");
      assertArrayEquals(expectedhb, resultkey);
    }
    assertFalse(expectedIterator.hasNext());
    assertFalse(resultIterator.hasNext());

    scanner.close();
  }

  @After
  public void teardown() throws IOException {
    IOUtils.closeQuietly(mReader);
    IOUtils.closeQuietly(mWriter);
    IOUtils.closeQuietly(mTable);
    mKiji.release();
  }
}
