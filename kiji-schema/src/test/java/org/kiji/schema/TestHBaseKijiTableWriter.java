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

import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.EnvironmentBuilder;

public class TestHBaseKijiTableWriter {
  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableWriter mWriter;
  private KijiTableReader mReader;

  @Before
  public void setupEnvironment() throws Exception {
    // TODO: Put this in a withInstance() method.
    final String instance = java.util.UUID.randomUUID().toString().replace('-', 'x');

    // Get the test table layouts.
    final KijiTableLayout layout = new KijiTableLayout(
        KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST), null);

    // Populate the environment.
    Map<String, Kiji> environment = new EnvironmentBuilder()
        .withInstance(instance)
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
    mKiji = environment.get(instance);
    mTable = mKiji.openTable("user");
    mWriter = mTable.openTableWriter();
    mReader = mTable.openTableReader();
  }

  @After
  public void cleanupEnvironment() {
    IOUtils.closeQuietly(mWriter);
    IOUtils.closeQuietly(mReader);
    IOUtils.closeQuietly(mTable);
    IOUtils.closeQuietly(mKiji);
  }

  @Test
  public void testPutWithTimestamp() throws Exception {
    final EntityId entityId = mTable.getEntityId("foo");
    final KijiDataRequest request = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("info", "name"));
    mWriter.put(entityId, "info", "name", 123L, "baz");

    final String actual = mReader.get(entityId, request).getValue("info", "name", 123L).toString();
    assertEquals("baz", actual);
  }

  @Test
  public void testIncrement() throws Exception {
    final EntityId entityId = mTable.getEntityId("foo");
    final KijiDataRequest request = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("info", "visits"));
    mWriter.increment(entityId, "info", "visits", 5L);

    final long actual = mReader.get(entityId, request).getCounter("info", "visits").getValue();
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
    final KijiDataRequest request = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("info", "visits"));
    mWriter.put(entityId, "info", "visits", 5L);

    final long actual = mReader.get(entityId, request).getCounter("info", "visits").getValue();
    assertEquals(5L, actual);
  }
}
