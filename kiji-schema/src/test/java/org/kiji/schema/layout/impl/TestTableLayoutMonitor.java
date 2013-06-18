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

package org.kiji.schema.layout.impl;

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.impl.TableLayoutMonitor.LayoutTracker;
import org.kiji.schema.layout.impl.TableLayoutMonitor.LayoutUpdateHandler;
import org.kiji.schema.util.ZooKeeperTest;

/** Tests for TableLayoutMonitor. */
public class TestTableLayoutMonitor extends ZooKeeperTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestTableLayoutMonitor.class);

  /** Terribly minimal unit-test for TableLayoutMonitor trackers. */
  @Test
  public void testTableLayoutMonitor() throws Exception {
    final ZooKeeperClient zkClient = new ZooKeeperClient(getZKAddress(), 60 * 1000);
    try {
      zkClient.open();

      final TableLayoutMonitor monitor = new TableLayoutMonitor(zkClient);
      final KijiURI tableURI =
          KijiURI.newBuilder(String.format("kiji://%s/kiji_instance/table_name", getZKAddress()))
          .build();

      monitor.notifyNewTableLayout(tableURI, Bytes.toBytes("layout.v1"), -1);

      final List<String> layouts = Lists.newArrayList();
      final Object lock = new Object();

      final LayoutTracker layoutTracker = monitor.newTableLayoutTracker(
          tableURI,
          new LayoutUpdateHandler() {
            /** {@inheritDoc} */
            @Override
            public void update(byte[] layout) {
              synchronized (lock) {
                layouts.add(Bytes.toString(layout));
                lock.notifyAll();
              }
            }
          });

      synchronized (lock) {
        layoutTracker.open();
        lock.wait(10 * 1000);
      }

      Assert.assertEquals(1, layouts.size());
      Assert.assertEquals("layout.v1", layouts.get(0));

      synchronized (lock) {
        monitor.notifyNewTableLayout(tableURI, Bytes.toBytes("layout.v2"), -1);
        lock.wait(10 * 1000);
      }
      Assert.assertEquals(2, layouts.size());
      Assert.assertEquals("layout.v2", layouts.get(1));

      monitor.close();
    } finally {
      zkClient.release();
    }
  }

}
