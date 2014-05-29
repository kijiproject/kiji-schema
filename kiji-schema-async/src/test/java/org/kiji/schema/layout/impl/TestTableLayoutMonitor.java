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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiURI;
import org.kiji.schema.RuntimeInterruptedException;
import org.kiji.schema.layout.impl.ZooKeeperMonitor.LayoutTracker;
import org.kiji.schema.layout.impl.ZooKeeperMonitor.LayoutUpdateHandler;
import org.kiji.schema.layout.impl.ZooKeeperMonitor.UsersTracker;
import org.kiji.schema.layout.impl.ZooKeeperMonitor.UsersUpdateHandler;
import org.kiji.schema.util.ZooKeeperTest;

/** Tests for TableLayoutMonitor. */
public class TestTableLayoutMonitor extends ZooKeeperTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestTableLayoutMonitor.class);

  /** Tests the layout update notification mechanism. */
  @Test
  public void testLayoutUpdateNotification() throws Exception {
    final ZooKeeperClient zkClient = ZooKeeperClient.getZooKeeperClient(getZKAddress());
    try {
      final ZooKeeperMonitor monitor = new ZooKeeperMonitor(zkClient);
      try {

        final KijiURI tableURI =
            KijiURI
                .newBuilder(String.format("kiji://%s/kiji_instance/table_name", getZKAddress()))
                .build();

        monitor.notifyNewTableLayout(tableURI, Bytes.toBytes("layout.v1"), -1);

        final BlockingQueue<String> queue = Queues.newSynchronousQueue();

        final LayoutTracker layoutTracker = monitor.newTableLayoutTracker(
            tableURI,
            new LayoutUpdateHandler() {
              /** {@inheritDoc} */
              @Override
              public void update(byte[] layout) {
                try {
                  queue.put(Bytes.toString(layout));
                } catch (InterruptedException ie) {
                  throw new RuntimeInterruptedException(ie);
                }
              }
            });

        layoutTracker.open();
        final String layout1 = queue.poll(1, TimeUnit.SECONDS);
        Assert.assertEquals("layout.v1", layout1);

        monitor.notifyNewTableLayout(tableURI, Bytes.toBytes("layout.v2"), -1);
        final String layout2 = queue.poll(1, TimeUnit.SECONDS);
        Assert.assertEquals("layout.v2", layout2);

      } finally {
        monitor.close();
      }
    } finally {
      zkClient.release();
    }
  }

  /** Tests the tracking of table users. */
  @Test
  public void testUsersTracker() throws Exception {
    final ZooKeeperClient zkClient = ZooKeeperClient.getZooKeeperClient(getZKAddress());
    try {
      final ZooKeeperMonitor monitor = new ZooKeeperMonitor(zkClient);
      try {
        final KijiURI tableURI =
            KijiURI
                .newBuilder(String.format("kiji://%s/kiji_instance/table_name", getZKAddress()))
                .build();

        final BlockingQueue<Multimap<String, String>> queue = Queues.newSynchronousQueue();

        final UsersTracker tracker = monitor.newTableUsersTracker(
            tableURI,
            new UsersUpdateHandler() {
              @Override
              public void update(Multimap<String, String> users) {
                LOG.info("Users map updated to: {}", users);
                try {
                  queue.put(users);
                } catch (InterruptedException ie) {
                  throw new RuntimeInterruptedException(ie);
                }
              }
            });
        tracker.open();
        final ZooKeeperMonitor.TableUserRegistration userRegistration1 =
            monitor.newTableUserRegistration("user-id-1", tableURI);
        final ZooKeeperMonitor.TableUserRegistration userRegistration2 =
            monitor.newTableUserRegistration("user-id-2", tableURI);
        try {
          Assert.assertTrue(queue.poll(1, TimeUnit.SECONDS).isEmpty());

          userRegistration1.updateRegisteredLayout("layout-id-1");
          Assert.assertEquals(
              ImmutableSetMultimap.of("user-id-1", "layout-id-1"),
              queue.poll(1, TimeUnit.SECONDS));

          userRegistration1.updateRegisteredLayout("layout-id-2");
          Assert.assertEquals(
              ImmutableSetMultimap.<String, String>of(), // unregistration event
              queue.poll(1, TimeUnit.SECONDS));
          Assert.assertEquals(
              ImmutableSetMultimap.of("user-id-1", "layout-id-2"), // re-registration event
              queue.poll(1, TimeUnit.SECONDS));

          userRegistration2.updateRegisteredLayout("layout-id-1");
          Assert.assertEquals(
              ImmutableSetMultimap.of(
                  "user-id-1", "layout-id-2",
                  "user-id-2", "layout-id-1"),
              queue.poll(1, TimeUnit.SECONDS));

          userRegistration1.close();
          Assert.assertEquals(
              ImmutableSetMultimap.of("user-id-2", "layout-id-1"),
              queue.poll(1, TimeUnit.SECONDS));

        } finally {
          tracker.close();
          userRegistration2.close();
        }
      } finally {
        monitor.close();
      }
    } finally {
      zkClient.release();
    }
  }
}
