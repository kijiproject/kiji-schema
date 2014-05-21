/**
 * (c) Copyright 2013 WibiData, Inc.
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
package org.kiji.schema.impl.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.AtomicKijiPutter;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiBufferedWriter;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.impl.LayoutConsumer;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.impl.TableLayoutMonitor;
import org.kiji.schema.util.InstanceBuilder;

public class TestLayoutConsumer extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestLayoutConsumer.class);
  private Kiji mKiji;

  @Before
  public final void setupTestLayoutConsumer() throws Exception {
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
                    .withQualifier("name").withValue(1L, "bar-val")
                    .withQualifier("visits").withValue(1L, 100L)
        .build();
  }

  @Test
  public void testOpenReaders() throws IOException {
    final KijiTable table = mKiji.openTable("user");
    try {
      final HBaseKijiTable htable = HBaseKijiTable.downcast(table);
      final TableLayoutMonitor monitor = htable.getTableLayoutMonitor();
      assertTrue(monitor.getLayoutConsumers().isEmpty());
      // Readers should register with the table as they open.
      final KijiTableReader reader = table.openTableReader();
      // Check that all readers are accounted for.
      assertTrue(monitor.getLayoutConsumers().size() == 1);
      // Readers should unregister as they close.
      reader.close();
      // Check that no readers remain.
      assertTrue(monitor.getLayoutConsumers().isEmpty());
    } finally {
      table.release();
    }
  }

  @Test
  public void testOpenWriters() throws IOException {
    final KijiTable table = mKiji.openTable("user");
    try {
      final HBaseKijiTable htable = HBaseKijiTable.downcast(table);
      final TableLayoutMonitor monitor = htable.getTableLayoutMonitor();
      assertTrue(monitor.getLayoutConsumers().isEmpty());
      // Writers should register with the table as they open.
      final KijiTableWriter writer = table.openTableWriter();
      final KijiBufferedWriter bufferedWriter = table.getWriterFactory().openBufferedWriter();
      final AtomicKijiPutter atomicPutter = table.getWriterFactory().openAtomicPutter();

      // Check that all writers are accounted for.
      final Set<LayoutConsumer> consumers = monitor.getLayoutConsumers();
      assertTrue(consumers.size() == 3);

      // Writers should unregister as they close.
      writer.close();
      bufferedWriter.close();
      atomicPutter.close();

      // Check that no writers remain.
      assertTrue(monitor.getLayoutConsumers().isEmpty());
    } finally {
      table.release();
    }
  }

  @Test
  public void testUpdateLayout() throws IOException {
    final KijiTable table = mKiji.openTable("user");
    try {
      final HBaseKijiTable htable = HBaseKijiTable.downcast(table);
      final TableLayoutMonitor monitor = htable.getTableLayoutMonitor();
      assertTrue(monitor.getLayoutConsumers().isEmpty());
      final KijiTableWriter writer = table.openTableWriter();
      try {
        assertTrue(monitor.getLayoutConsumers().size() == 1);

        // We can write to info:name, but not family:column.
        writer.put(table.getEntityId("foo"), "info", "name", "new-val");
        try {
          writer.put(table.getEntityId("foo"), "family", "column", "foo-val");
          fail("writer.put() should have thrown an IOException.");
        } catch (NoSuchColumnException nsce) {
          assertEquals("family:column", nsce.getMessage());
        }

        // Update the table layout.
        final KijiTableLayout newLayout =
            KijiTableLayout.newLayout(KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE));
        monitor.updateLayoutConsumers(newLayout);

        // Now we can write to family:column, but not info:name.
        writer.put(table.getEntityId("foo"), "family", "column", "foo-val");
        try {
          writer.put(table.getEntityId("foo"), "info", "name", "two-val");
          fail("writer.put() should have thrown an IOException.");
        } catch (NoSuchColumnException nsce) {
          assertEquals("info:name", nsce.getMessage());
        }
      } finally {
        writer.close();
      }
    } finally {
      table.release();
    }
  }
}
