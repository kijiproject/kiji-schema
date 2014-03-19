/**
 * (c) Copyright 2014 WibiData, Inc.
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

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.impl.ZooKeeperMonitor.LayoutTracker;
import org.kiji.schema.layout.impl.ZooKeeperMonitor.LayoutUpdateHandler;
import org.kiji.schema.layout.impl.ZooKeeperMonitor.TableUserRegistration;
import org.kiji.schema.layout.impl.ZooKeeperMonitor.UsersTracker;
import org.kiji.schema.layout.impl.ZooKeeperMonitor.UsersUpdateHandler;
import org.kiji.schema.util.ZooKeeperTest;

public class TestZooKeeperMonitor extends ZooKeeperTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestZooKeeperMonitor.class);
  private volatile ZooKeeperMonitor mMonitor;
  private volatile ZooKeeperClient mZKClient;
  private final AtomicInteger mTableCounter = new AtomicInteger(0);

  @Before
  public void setup() throws Exception {
    mZKClient = ZooKeeperClient.getZooKeeperClient(getZKAddress());
    mMonitor = new ZooKeeperMonitor(mZKClient);
  }

  @After
  public void cleanup() throws Exception {
    mMonitor.close();
    mZKClient.release();
    mMonitor = null;
    mZKClient = null;
  }

  private KijiURI getTableURI() {
    String instanceName = "TestZooKeeperMonitor";
    String tablePrefix = "table_";
    return KijiURI
        .newBuilder()
        .withInstanceName(instanceName)
        .withTableName(tablePrefix + mTableCounter.getAndIncrement())
        .build();
  }

  @Test
  public void testTableLayoutTracking() throws Exception {
    final byte[] layout0 = "0".getBytes();
    final byte[] layout1 = "1".getBytes();

    KijiURI tableURI = getTableURI();
    final BlockingQueue<byte[]> layoutQueue = Queues.newSynchronousQueue();

    LayoutTracker tracker =
        mMonitor.newTableLayoutTracker(tableURI, new QueueingLayoutUpdateHandler(layoutQueue));

    try {
      mMonitor.notifyNewTableLayout(tableURI, layout0, -1);
      tracker.open();
      Assert.assertTrue("No notification for initial layout.",
          Arrays.equals(layout0, layoutQueue.poll(1, TimeUnit.SECONDS)));

      mMonitor.notifyNewTableLayout(tableURI, layout1, -1);
      Assert.assertTrue("No notification for layout update.",
          Arrays.equals(layout1, layoutQueue.poll(1, TimeUnit.SECONDS)));
    } finally {
      tracker.close();
    }
  }

  @Test
  public void testTableUsersTracking() throws Exception {
    String user = "user1";
    String layout1 = "layout1";
    String layout2 = "layout2";

    KijiURI tableURI = getTableURI();
    final BlockingQueue<Multimap<String, String>> usersQueue = Queues.newSynchronousQueue();

    UsersTracker tracker =
        mMonitor.newTableUsersTracker(tableURI, new QueueingUsersUpdateHandler(usersQueue));

    try {
      tracker.open();

      Assert.assertEquals(
          ImmutableMultimap.<String, String>of(), usersQueue.poll(1, TimeUnit.SECONDS));

      TableUserRegistration registration = mMonitor.newTableUserRegistration(user, tableURI);
      try {

        Assert.assertNull("Unexpected users update.", usersQueue.poll());

        registration.updateRegisteredLayout(layout1);

        Assert.assertEquals(
            ImmutableSetMultimap.of(user, layout1), usersQueue.take());

        registration.updateRegisteredLayout(layout2);

        // First action is to unregister
        Assert.assertEquals(
            ImmutableSetMultimap.<String, String>of(), usersQueue.take());
        // and then re-register with updated layout
        Assert.assertEquals(
            ImmutableSetMultimap.of(user, layout2), usersQueue.take());

        registration.close();

        Assert.assertEquals(
            ImmutableSetMultimap.<String, String>of(), usersQueue.take());
      } finally {
        registration.close();
      }
    } finally {
      tracker.close();
    }
  }

  public static final class QueueingLayoutUpdateHandler implements LayoutUpdateHandler {
    private final BlockingQueue<byte[]> mLayoutQueue;
    public QueueingLayoutUpdateHandler(BlockingQueue<byte[]> layoutQueue) {
      this.mLayoutQueue = layoutQueue;
    }
    @Override
    public void update(byte[] layout) {
      try {
        mLayoutQueue.put(layout);
      } catch (InterruptedException e) {
        LOG.warn(e.getMessage());
      }
    }
  }

  public static final class QueueingUsersUpdateHandler implements UsersUpdateHandler {
    private final BlockingQueue<Multimap<String, String>> mUsersQueue;
    public QueueingUsersUpdateHandler(BlockingQueue<Multimap<String, String>> usersQueue) {
      this.mUsersQueue = usersQueue;
    }

    @Override
    public void update(Multimap<String, String> users) {
      try {
        mUsersQueue.put(users);
      } catch (InterruptedException e) {
        LOG.warn(e.getMessage());
      }
    }
  }
}
