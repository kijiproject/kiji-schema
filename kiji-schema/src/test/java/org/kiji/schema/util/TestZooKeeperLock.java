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

package org.kiji.schema.util;

import static org.junit.Assert.assertFalse;

import java.io.File;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.layout.impl.ZooKeeperClient;

public class TestZooKeeperLock extends ZooKeeperTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestZooKeeperLock.class);

  /** Overly basic test for ZooKeeper locks. */
  @Test
  public void testZooKeeperLock() throws Exception {
    final File lockDir = new File("/lock");
    final ZooKeeperClient zkClient = ZooKeeperClient.getZooKeeperClient(getZKAddress());
    try {
      final CyclicBarrier barrier = new CyclicBarrier(2);
      final ZooKeeperLock lock1 = new ZooKeeperLock(zkClient, lockDir);
      final ZooKeeperLock lock2 = new ZooKeeperLock(zkClient, lockDir);
      lock1.lock();

      final Thread thread = new Thread() {
        /** {@inheritDoc} */
        @Override
        public void run() {
          try {
            assertFalse(lock2.lock(0.1));
            barrier.await();
            lock2.lock();
            lock2.unlock();

            lock2.lock();
            lock2.unlock();
            lock2.close();
            barrier.await();
          } catch (Exception e) {
            LOG.warn("Exception caught in locking thread: {}", e.getMessage());
          }
        }
      };
      thread.start();

      barrier.await(5, TimeUnit.SECONDS); // Eventually fail

      lock1.unlock();
      lock1.close();

      barrier.await(5, TimeUnit.SECONDS); // Eventually fail
    } finally {
      zkClient.release();
    }
  }
}
