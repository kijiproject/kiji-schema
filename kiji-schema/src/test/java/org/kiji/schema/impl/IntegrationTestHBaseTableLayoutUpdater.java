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
package org.kiji.schema.impl;

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.impl.TableLayoutMonitor;
import org.kiji.schema.layout.impl.TableLayoutMonitor.LayoutTracker;
import org.kiji.schema.layout.impl.TableLayoutMonitor.LayoutUpdateHandler;
import org.kiji.schema.layout.impl.ZooKeeperClient;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;
import org.kiji.schema.util.ProtocolVersion;

public class IntegrationTestHBaseTableLayoutUpdater extends AbstractKijiIntegrationTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(IntegrationTestHBaseTableLayoutUpdater.class);

  private static final String LAYOUT_V1 = "org/kiji/schema/layout/layout-updater-v1.json";
  private static final String LAYOUT_V2 = "org/kiji/schema/layout/layout-updater-v2.json";

  /**
   * Basic test for the flow of a table layout update:
   * Create a new table, then update its layout,
   * while simulating a single live client using the table.
   */
  @Test
  public void testCreateTable() throws Exception {
    final TableLayoutDesc layout1 = KijiTableLayouts.getLayout(LAYOUT_V1);
    final TableLayoutDesc layout2 = KijiTableLayouts.getLayout(LAYOUT_V2);

    final KijiURI uri = getKijiURI();

    // Update the data version of the Kiji instance:
    {
      final Kiji kiji = Kiji.Factory.open(uri);
      try {
        kiji.getSystemTable().setDataVersion(ProtocolVersion.parse("system-2.0"));
      } finally {
        kiji.release();
      }
    }

    final Kiji kiji = Kiji.Factory.open(uri);
    try {
      final ZooKeeperClient zkClient = ((HBaseKiji) kiji).getZKClient();  // owned by kiji
      final TableLayoutMonitor monitor = new TableLayoutMonitor(zkClient);
      try {
        kiji.createTable(layout1);

        final KijiTable table = kiji.openTable("table_name");  // currently not registered as a user
        try {
          monitor.registerTableUser(table.getURI(), "user-id-1", "1");
          final List<String> layoutIDs = Lists.newArrayList();

          final LayoutTracker tracker = monitor.newTableLayoutTracker(table.getURI(),
              new LayoutUpdateHandler() {
                /** {@inheritDoc} */
                @Override
                public void update(byte[] layout) {
                  synchronized (layoutIDs) {
                    layoutIDs.add(Bytes.toString(layout));
                    layoutIDs.notifyAll();
                  }
                }
              });
          tracker.open();

          synchronized (layoutIDs) {
            while ((layoutIDs.size() < 1))  {
              layoutIDs.wait();
            }
            Assert.assertEquals("1", layoutIDs.get(0));
          }

          final HBaseTableLayoutUpdater updater =
              new HBaseTableLayoutUpdater((HBaseKiji) kiji, table.getURI(), layout2);
          try {
            final Thread thread = new Thread() {
              /** {@inheritDoc} */
              @Override
              public void run() {
                try {
                  updater.update();
                } catch (Exception exn) {
                  throw new RuntimeException(exn);
                }
              }
            };
            thread.start();

            synchronized (layoutIDs) {
              while ((layoutIDs.size() < 2))  {
                layoutIDs.wait();
              }
              Assert.assertEquals("2", layoutIDs.get(1));
            }
            tracker.close();

            monitor.registerTableUser(table.getURI(), "user-id-1", "2");
            monitor.unregisterTableUser(table.getURI(), "user-id-1", "1");

            thread.join();

          } finally {
            updater.close();
          }
        } finally {
          table.release();
        }
      } finally {
        monitor.close();
      }
      kiji.deleteTable("table_name");
    } finally {
      kiji.release();
    }
  }
}
