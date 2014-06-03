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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.zookeeper.TestUsersTracker.QueueingUsersUpdateHandler;
import org.kiji.schema.zookeeper.UsersTracker;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

public class TestHBaseKijiTable extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseKijiTable.class);

  @Test
  public void testReleasingKijiTableWillCloseItsResources() throws Exception {
    final KijiURI uri = getKiji().getURI();
    final String tableName = "IntegrationTestTableLayoutUpdate";
    final KijiURI tableURI = KijiURI.newBuilder(uri).withTableName(tableName).build();
    final String layoutId1 = "1";

    final HBaseKiji kiji = (HBaseKiji) getKiji();
    final TableLayoutDesc layoutDesc = KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST);
    layoutDesc.setLayoutId(layoutId1);
    layoutDesc.setName(tableName);
    kiji.createTable(layoutDesc);

    final BlockingQueue<Multimap<String, String>> queue = Queues.newSynchronousQueue();

    final UsersTracker tracker =
        ZooKeeperUtils
            .newTableUsersTracker(kiji.getZKClient(), tableURI)
            .registerUpdateHandler(new QueueingUsersUpdateHandler(queue));
    try {
      tracker.start();
      // Initial user map should be empty:
      assertEquals(ImmutableSetMultimap.<String, String>of(), queue.poll(1, TimeUnit.SECONDS));

      final KijiTable table = kiji.openTable(tableName);
      try {
        // We opened a table, user map must contain exactly one entry:
        final Multimap<String, String> umap = queue.poll(1, TimeUnit.SECONDS);
        assertEquals(ImmutableSet.of(layoutId1), ImmutableSet.copyOf(umap.values()));
      } finally {
        table.release();
      }
      // Table is now closed, the user map should become empty:
      assertEquals(ImmutableSetMultimap.<String, String>of(), queue.poll(1, TimeUnit.SECONDS));
    } finally {
      tracker.close();
    }
  }
}
