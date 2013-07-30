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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.RuntimeInterruptedException;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.impl.TableLayoutMonitor;
import org.kiji.schema.layout.impl.TableLayoutMonitor.UsersTracker;
import org.kiji.schema.layout.impl.TableLayoutMonitor.UsersUpdateHandler;
import org.kiji.schema.layout.impl.ZooKeeperClient;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;
import org.kiji.schema.util.ProtocolVersion;

public class IntegrationTestTableLayoutUpdate extends AbstractKijiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestTableLayoutUpdate.class);

  private static void setDataVersion(final KijiURI uri, final ProtocolVersion version)
      throws IOException {
    final Kiji kiji = Kiji.Factory.open(uri);
    try {
      kiji.getSystemTable().setDataVersion(version);
    } finally {
      kiji.release();
    }
  }

  @Test
  public void testUpdateLayout() throws Exception {
    final KijiURI uri = getKijiURI();
    final String tableName = "foo";
    final KijiURI tableURI = KijiURI.newBuilder(uri).withTableName(tableName).build();
    final String layoutId1 = "1";
    final String layoutId2 = "2";
    {
      final Kiji kiji = Kiji.Factory.open(uri);
      try {
        final TableLayoutDesc layoutDesc = KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST);
        layoutDesc.setLayoutId(layoutId1);
        kiji.createTable(layoutDesc);
      } finally {
        kiji.release();
      }
    }

    // Switch the Kiji instance to system-2.0:
    setDataVersion(uri, ProtocolVersion.parse("system-2.0"));

    final HBaseKiji kiji = (HBaseKiji) Kiji.Factory.open(uri, getConf());
    try {
      final ZooKeeperClient zkClient = kiji.getZKClient();
      final TableLayoutMonitor layoutMonitor = new TableLayoutMonitor(zkClient);
      try {
        layoutMonitor.notifyNewTableLayout(tableURI, Bytes.toBytes(layoutId1), -1);
        final BlockingQueue<Multimap<String, String>> queue = Queues.newSynchronousQueue();

        final UsersTracker tracker = layoutMonitor.newTableUsersTracker(tableURI,
            new UsersUpdateHandler() {
              /** {@inheritDoc} */
              @Override
              public void update(Multimap<String, String> users) {
                LOG.info("User map update: {}", users);
                try {
                  queue.put(users);
                } catch (InterruptedException ie) {
                  throw new RuntimeInterruptedException(ie);
                }
              }
            });
        tracker.open();
        try {

          // Initial user map should be empty:
          assertTrue(queue.take().isEmpty());

          final KijiTable table = kiji.openTable(tableName);
          try {
            {
              // We opened one table, user map must contain exactly one entry:
              final Multimap<String, String> umap = queue.take();
              assertEquals(1, umap.size());
              assertEquals(layoutId1, umap.values().iterator().next());
            }

            // Open a reader to exercise the logic:
            final KijiTableReader reader = table.openTableReader();
            try {

              // Push a layout update (a no-op, but with a new layout ID):
              final TableLayoutDesc newLayoutDesc =
                  KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST);
              newLayoutDesc.setReferenceLayout(layoutId1);
              newLayoutDesc.setLayoutId(layoutId2);
              kiji.getMetaTable().updateTableLayout(tableName, newLayoutDesc);
              layoutMonitor.notifyNewTableLayout(tableURI, Bytes.toBytes(layoutId2), -1);

              // The new user map should eventually reflect the new layout ID.
              // There may be, but not always, a transition set where both layout IDs are visible.
              while (true) {
                final Multimap<String, String> umap = queue.take();
                if (umap.size() == 2) {
                  continue;
                } else {
                  assertEquals(1, umap.size());
                  assertEquals(layoutId2, umap.values().iterator().next());
                  break;
                }
              }

            } finally {
              reader.close();
            }

          } finally {
            table.release();
          }

          // Table is now closed, the user map should become empty:
          assertTrue(queue.take().isEmpty());

        } finally {
          tracker.close();
        }

      } finally {
        layoutMonitor.close();
      }
    } finally {
      kiji.release();
    }
  }
}
