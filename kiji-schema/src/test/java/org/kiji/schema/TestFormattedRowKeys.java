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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.ByteArrayFormatter;

/** Tests for formatted row keys. */
public class TestFormattedRowKeys extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestFormattedRowKeys.class);

  public static final String TABLE_NAME = "table";

  private Kiji mKiji = null;
  private KijiTable mTable = null;
  private KijiTableWriter mWriter = null;
  private KijiTableReader mReader = null;
  private KijiDataRequest mDataRequest = null;

  @Before
  public final void setup() throws Exception {
    mKiji = getKiji();
    mKiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.FORMATTED_RKF));
    mTable = mKiji.openTable(TABLE_NAME);
    mWriter = mTable.openTableWriter();
    mReader = mTable.openTableReader();
    mDataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(1).add("family", "column"))
        .build();
  }

  @After
  public final void teardown() throws Exception {
    mReader.close();
    mWriter.close();
    mTable.release();
  }

  @Test
  public void testFormattedRKF() throws IOException {
    LOG.info("Populating test table.");

    mWriter.put(mTable.getEntityId("x", "a", "bc"), "family", "column", "1");

    KijiRowData result = mReader.get(mTable.getEntityId("x", "a", "bc"), mDataRequest);
    assertTrue(result.containsColumn("family", "column"));
    assertEquals(3, result.getEntityId().getComponents().size());
    assertEquals("x", result.getEntityId().getComponentByIndex(0));
    assertEquals("a", result.getEntityId().getComponentByIndex(1));
    assertEquals("bc", result.getEntityId().getComponentByIndex(2));

    String res = result.getMostRecentValue("family", "column").toString();
    assertEquals("1", res);

    mWriter.deleteRow(result.getEntityId());
    result = mReader.get(mTable.getEntityId("x", "a", "bc"), mDataRequest);
    assertFalse(result.containsColumn("family", "column"));
  }

  @Test
  public void testFormattedKeyOrdering() throws IOException {
    final List<EntityId> expected = new ArrayList<EntityId>();

    // We expect this to be the ordering for the keys below.
    // x is the dummy key to make all hashes equal.
    expected.add(mTable.getEntityId("x", "a", "bc"));
    expected.add(mTable.getEntityId("x", "a0", "aa"));
    expected.add(mTable.getEntityId("x", "a0", "aa", -1));
    expected.add(mTable.getEntityId("x", "a0", "aa", 0));
    expected.add(mTable.getEntityId("x", "a0", "aa", Integer.MAX_VALUE));
    expected.add(mTable.getEntityId("x", "a0", "aa0", -1));
    expected.add(mTable.getEntityId("x", "a0", "aa0", 0));
    expected.add(mTable.getEntityId("x", "a0", "aa0", 0, Long.MIN_VALUE));
    expected.add(mTable.getEntityId("x", "a0", "aa0", 0, 0L));
    expected.add(mTable.getEntityId("x", "a0", "aa0", 0, Long.MAX_VALUE));
    expected.add(mTable.getEntityId("x", "a0", "aa0", Integer.MAX_VALUE));

    for (EntityId eid: expected) {
      mWriter.put(eid, "family", "column", "1");
    }

    final KijiRowScanner scanner = mReader.getScanner(mDataRequest);
    try {
      final Iterator<KijiRowData> resultIterator = scanner.iterator();

      final Iterator<EntityId> expectedIterator = expected.iterator();
      while (resultIterator.hasNext() && expectedIterator.hasNext()) {
        byte[] expectedhb = expectedIterator.next().getHBaseRowKey();
        byte[] resultkey = resultIterator.next().getEntityId().getHBaseRowKey();
        LOG.debug("Expected: {}", ByteArrayFormatter.toHex(expectedhb, ':'));
        LOG.debug("Result:   {}", ByteArrayFormatter.toHex(resultkey, ':'));
        assertArrayEquals(expectedhb, resultkey);
      }
      assertFalse(expectedIterator.hasNext());
      assertFalse(resultIterator.hasNext());
    } finally {
      scanner.close();
    }
  }
}
