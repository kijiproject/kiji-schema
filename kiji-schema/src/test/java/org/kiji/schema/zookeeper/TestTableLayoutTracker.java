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

package org.kiji.schema.zookeeper;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Queues;
import junit.framework.Assert;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.KillSession;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.impl.TestZooKeeperMonitor.QueueingLayoutUpdateHandler;
import org.kiji.schema.layout.impl.TestZooKeeperMonitor.QueueingUsersUpdateHandler;
import org.kiji.schema.layout.impl.ZooKeeperClient;
import org.kiji.schema.layout.impl.ZooKeeperMonitor;
import org.kiji.schema.layout.impl.ZooKeeperMonitor.LayoutTracker;
import org.kiji.schema.util.ZooKeeperTest;

public class TestTableLayoutTracker extends ZooKeeperTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestTableLayoutTracker.class);
  private volatile CuratorFramework mZKClient;

  @Before
  public void setUpTestTableLayoutTracker() throws Exception {
    mZKClient = ZooKeeperUtils.getZooKeeperClient(getZKAddress());
  }

  @After
  public void tearDownTestTableLayoutTracker() throws Exception {
    mZKClient.close();
  }

  @Test
  public void testLayoutTrackerUpdatesCache() throws Exception {
    final KijiURI tableURI = KijiURI.newBuilder().withInstanceName("i").withTableName("t").build();
    final String layout1 = "layout-id-1";
    final String layout2 = "layout-id-2";

    ZooKeeperUtils.setTableLayout(mZKClient, tableURI, layout1);

    final CyclicBarrier barrier = new CyclicBarrier(2);
    TableLayoutTracker tracker = new TableLayoutTracker(mZKClient, tableURI,
            new BarrierTableLayoutUpdateHandler(barrier));
    try {
      tracker.start();

      barrier.await(5, TimeUnit.SECONDS);
      Assert.assertEquals(layout1, tracker.getLayoutID());

      ZooKeeperUtils.setTableLayout(mZKClient, tableURI, layout2);
      barrier.await(5, TimeUnit.SECONDS);
      Assert.assertEquals(layout2, tracker.getLayoutID());
    } finally {
      tracker.close();
    }
  }

  @Test
  public void testLayoutTrackerUpdatesHandler() throws Exception {
    final KijiURI tableURI = KijiURI.newBuilder().withInstanceName("i").withTableName("t").build();
    final String layout1 = "layout-id-1";
    final String layout2 = "layout-id-2";

    ZooKeeperUtils.setTableLayout(mZKClient, tableURI, layout1);

    BlockingQueue<String> layoutQueue = Queues.newSynchronousQueue();
    TableLayoutTracker tracker = new TableLayoutTracker(mZKClient, tableURI,
        new QueuingTableLayoutUpdateHandler(layoutQueue));
    try {
      tracker.start();
      Assert.assertEquals(layout1, layoutQueue.poll(5, TimeUnit.SECONDS));

      ZooKeeperUtils.setTableLayout(mZKClient, tableURI, layout2);
      Assert.assertEquals(layout2, layoutQueue.poll(5, TimeUnit.SECONDS));
    } finally {
      tracker.close();
    }
  }

  @Test
  public void testLayoutTrackerUpdatesHandlerAfterSessionExpiration() throws Exception {
    final KijiURI tableURI = KijiURI.newBuilder().withInstanceName("i").withTableName("t").build();
    final String layout1 = "layout-id-1";
    final String layout2 = "layout-id-2";

    ZooKeeperUtils.setTableLayout(mZKClient, tableURI, layout1);

    BlockingQueue<String> layoutQueue = Queues.newSynchronousQueue();
    TableLayoutTracker tracker = new TableLayoutTracker(mZKClient, tableURI,
        new QueuingTableLayoutUpdateHandler(layoutQueue));
    try {
      tracker.start();
      Assert.assertEquals(layout1, layoutQueue.poll(5, TimeUnit.SECONDS));

      KillSession.kill(mZKClient.getZookeeperClient().getZooKeeper(), getZKAddress());

      ZooKeeperUtils.setTableLayout(mZKClient, tableURI, layout2);
      Assert.assertEquals(layout2, layoutQueue.poll(5, TimeUnit.SECONDS));
    } finally {
      tracker.close();
    }
  }

  @Test
  public void testLayoutTrackerUpdatesHandlerWithNullAfterLayoutNodeDeleted() throws Exception {
    final KijiURI tableURI = KijiURI.newBuilder().withInstanceName("i").withTableName("t").build();
    final String layout1 = "layout-id-1";
    final String layout2 = "layout-id-2";

    ZooKeeperUtils.setTableLayout(mZKClient, tableURI, layout1);

    final CyclicBarrier barrier = new CyclicBarrier(2);
    TableLayoutTracker tracker = new TableLayoutTracker(mZKClient, tableURI,
        new BarrierTableLayoutUpdateHandler(barrier));
    try {
      tracker.start();

      barrier.await(5, TimeUnit.SECONDS);
      Assert.assertEquals(layout1, tracker.getLayoutID());

      mZKClient.delete().forPath(ZooKeeperUtils.getTableLayoutFile(tableURI).getPath());

      barrier.await(5, TimeUnit.SECONDS);
      Assert.assertNull(tracker.getLayoutID());
    } finally {
      tracker.close();
    }
  }

  /**
   * Test that TableLayoutTracker correctly handles layout changes initiated by a ZooKeeperMonitor
   * layout change.  This is testing backwards compatibility with the original ZooKeeper
   * implementation.
   */
  @Test
  public void testLayoutTrackerUpdatesHandlerInResponsetoZKMLayoutChange() throws Exception {
    final KijiURI tableURI = KijiURI.newBuilder().withInstanceName("i").withTableName("t").build();
    final String layout1 = "layout-id-1";
    final String layout2 = "layout-id-2";
    ZooKeeperMonitor zkMonitor =
        new ZooKeeperMonitor(ZooKeeperClient.getZooKeeperClient(getZKAddress()));

    zkMonitor.notifyNewTableLayout(tableURI, Bytes.toBytes(layout1), -1);

    BlockingQueue<String> layoutQueue = Queues.newSynchronousQueue();
    TableLayoutTracker tracker = new TableLayoutTracker(mZKClient, tableURI,
        new QueuingTableLayoutUpdateHandler(layoutQueue));
    try {
      tracker.start();
      Assert.assertEquals(layout1, layoutQueue.poll(5, TimeUnit.SECONDS));

      zkMonitor.notifyNewTableLayout(tableURI, Bytes.toBytes(layout2), -1);
      Assert.assertEquals(layout2, layoutQueue.poll(5, TimeUnit.SECONDS));
    } finally {
      tracker.close();
    }
  }

  /**
   * Test the ZooKeeperMonitor$LayoutTracker correctly recognizes layout changes by the new
   * curator-based notifier method in ZooKeeperUtils.
   */
  @Test
  public void testZKMLayoutTrackerRespondsToZooKeeperUtilsSetTableLayout() throws Exception {
    final KijiURI tableURI = KijiURI.newBuilder().withInstanceName("i").withTableName("t").build();
    final String layout1 = "layout-id-1";
    final String layout2 = "layout-id-2";

    ZooKeeperMonitor zkMonitor =
        new ZooKeeperMonitor(ZooKeeperClient.getZooKeeperClient(getZKAddress()));

    ZooKeeperUtils.setTableLayout(mZKClient, tableURI, layout1);

    BlockingQueue<byte[]> layoutQueue = Queues.newSynchronousQueue();
    LayoutTracker tracker =
        zkMonitor.newTableLayoutTracker(tableURI, new QueueingLayoutUpdateHandler(layoutQueue));
    try {
      tracker.open();
      Assert.assertEquals(layout1, Bytes.toString(layoutQueue.poll(5, TimeUnit.SECONDS)));

      ZooKeeperUtils.setTableLayout(mZKClient, tableURI, layout2);
      Assert.assertEquals(layout2, Bytes.toString(layoutQueue.poll(5, TimeUnit.SECONDS)));
    } finally {
      tracker.close();
    }
  }

  public static final class BarrierTableLayoutUpdateHandler implements TableLayoutUpdateHandler {
    private static final Logger LOG = LoggerFactory.getLogger(QueueingUsersUpdateHandler.class);
    private final CyclicBarrier mBarrier;

    public BarrierTableLayoutUpdateHandler(CyclicBarrier barrier) {
      mBarrier = barrier;
    }

    /** {@inheritDoc}. */
    @Override
    public void update(String layout) {
      try {
        LOG.debug("Received table layout update: {}.", layout);
        mBarrier.await();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while attempting to await barrier.");
        Thread.currentThread().interrupt();
      } catch (BrokenBarrierException e) {
        // This should be handled on the main thread.
        LOG.debug("Broken barrier detected.");
      }
    }
  }

  public static final class QueuingTableLayoutUpdateHandler implements TableLayoutUpdateHandler {
    private static final Logger LOG = LoggerFactory.getLogger(QueueingUsersUpdateHandler.class);
    private final BlockingQueue<String> mLayoutQueue;

    public QueuingTableLayoutUpdateHandler(BlockingQueue<String> layoutQueue) {
      this.mLayoutQueue = layoutQueue;
    }

    /** {@inheritDoc}. */
    @Override
    public void update(String layout) {
      try {
        LOG.debug("Received table layout update: {}.", layout);
        mLayoutQueue.put(layout);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while attempting to enqueue table layout update.");
        Thread.currentThread().interrupt();
      }
    }
  }
}
