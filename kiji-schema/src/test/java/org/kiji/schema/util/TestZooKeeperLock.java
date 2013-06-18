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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.kiji.schema.KijiIOException;
import org.kiji.schema.layout.impl.ZooKeeperClient;

public class TestZooKeeperLock extends ZooKeeperTest {

  /** Overly basic test for ZooKeeper locks. */
  @Test
  public void testZooKeeperLock() throws Exception {
    final File lockDir = new File("/lock");
    final ZooKeeperClient zkClient = new ZooKeeperClient(getZKAddress(), 60000);
    try {
      zkClient.open();
      final AtomicInteger counter = new AtomicInteger(0);

      final ZooKeeperLock lock1 = new ZooKeeperLock(zkClient, lockDir);
      final ZooKeeperLock lock2 = new ZooKeeperLock(zkClient, lockDir);
      lock1.lock();

      final Thread thread = new Thread() {
        /** {@inheritDoc} */
        @Override
        public void run() {
          try {
            assertFalse(lock2.lock(0.1));
            counter.incrementAndGet();
            lock2.lock();
            counter.incrementAndGet();
            lock2.unlock();
            counter.incrementAndGet();

            lock2.lock();
            counter.incrementAndGet();
            lock2.unlock();
            lock2.close();
            counter.incrementAndGet();
          } catch (Exception exn) {
            throw new KijiIOException(exn);
          }
        }
      };
      thread.start();

      while (counter.get() != 1) {
        Time.sleep(0.1);  // I hate it, but I can't think anything simpler for now.
      }
      lock1.unlock();
      lock1.close();

      thread.join();
      assertEquals(5, counter.get());

    } finally {
      zkClient.release();
    }
  }
}
