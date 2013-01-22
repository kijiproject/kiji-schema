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

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestHBaseKijiTableReader {
  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableReader mReader;

  @Before
  public void setupEnvironment() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout = new KijiTableLayout(
        KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST), null);

    // Populate the environment.
    mKiji = new InstanceBuilder()
        .withTable("user", layout)
            .withRow("foo")
                .withFamily("info")
                    .withQualifier("name").withValue(1L, "foo-val")
                    .withQualifier("visits").withValue(1L, 42L)
            .withRow("bar")
                .withFamily("info")
                    .withQualifier("name").withValue(1L, "bar-val")
                    .withQualifier("visits").withValue(1L, 100L)
        .build();

    // Fill local variables.
    mTable = mKiji.openTable("user");
    mReader = mTable.openTableReader();
  }

  @After
  public void cleanupEnvironment() throws Exception {
    IOUtils.closeQuietly(mReader);
    IOUtils.closeQuietly(mTable);
    mKiji.release();
  }

  @Test
  public void testGet() throws Exception {
    final EntityId entityId = mTable.getEntityId("foo");
    final KijiDataRequest request = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("info", "name"));
    final String actual = mReader.get(entityId, request).getValue("info", "name", 1L).toString();
    assertEquals("foo-val", actual);
  }

  @Test
  public void testGetCounter() throws Exception {
    final EntityId entityId = mTable.getEntityId("foo");
    final KijiDataRequest request = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("info", "visits"));
    final long actual = mReader.get(entityId, request).getCounter("info", "visits").getValue();
    assertEquals(42L, actual);
  }

  @Test
  public void testBulkGet() throws Exception {
    final EntityId entityId1 = mTable.getEntityId("foo");
    final EntityId entityId2 = mTable.getEntityId("bar");
    final KijiDataRequest request = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("info", "name"));
    final String actual1 = mReader.get(entityId1, request).getValue("info", "name", 1L).toString();
    final String actual2 = mReader.get(entityId2, request).getValue("info", "name", 1L).toString();
    assertEquals("foo-val", actual1);
    assertEquals("bar-val", actual2);
  }
}
