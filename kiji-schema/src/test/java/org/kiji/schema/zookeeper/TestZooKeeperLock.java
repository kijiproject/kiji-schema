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

import java.io.File;

import org.apache.curator.framework.CuratorFramework;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.layout.impl.ZooKeeperClient;
import org.kiji.schema.util.ZooKeeperTest;

public class TestZooKeeperLock extends ZooKeeperTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestZooKeeperLock.class);

  @Test
  public void testZooKeeperLockIsExclusiveAmongZKSessions() throws Exception {
    final File path = new File("/lock/is/exclusive");
    CuratorFramework zkClient1 = ZooKeeperUtils.getZooKeeperClient(getZKAddress());
    try {
      CuratorFramework zkClient2 = ZooKeeperUtils.createZooKeeperClient(getZKAddress());
      try {
        ZooKeeperLock lock1 = new ZooKeeperLock(zkClient1, path);
        try {
          ZooKeeperLock lock2 = new ZooKeeperLock(zkClient2, path);
          try {
            Assert.assertTrue(lock1.lock(1.0));
            Assert.assertFalse(lock2.lock(1.0));
            lock1.unlock();

            Assert.assertTrue(lock2.lock(1.0));
            Assert.assertFalse(lock1.lock(1.0));
            lock2.unlock();
          } finally {
            lock2.close();
          }
        } finally {
          lock1.close();
        }
      } finally {
        zkClient2.close();
      }
    } finally {
      zkClient1.close();
    }
  }

  @Test
  public void testZooKeeperLockIsExclusiveInAZKSession() throws Exception {
    final File path = new File("/lock/is/exclusive/in/session");
    CuratorFramework zkClient = ZooKeeperUtils.getZooKeeperClient(getZKAddress());
    try {
      ZooKeeperLock lock1 = new ZooKeeperLock(zkClient, path);
      try {
        ZooKeeperLock lock2 = new ZooKeeperLock(zkClient, path);
        try {
          Assert.assertTrue(lock1.lock(1.0));
          Assert.assertFalse(lock2.lock(1.0));
          lock1.unlock();

          Assert.assertTrue(lock2.lock(1.0));
          Assert.assertFalse(lock1.lock(1.0));
          lock2.unlock();
        } finally {
          lock2.close();
        }
      } finally {
        lock1.close();
      }
    } finally {
      zkClient.close();
    }
  }

  /**
   * Tests that this ZooKeeperLock can work interchangebly with the ZooKeeperClient based
   * ZooKeeperLock.
   */
  @Test
  public void testZooKeeperLockIsExclusiveWithZKMZooKeeperLock() throws Exception {
    final File path = new File("/lock/is/exclusive/zkm");
    CuratorFramework zkClient1 = ZooKeeperUtils.getZooKeeperClient(getZKAddress());
    try {
      ZooKeeperClient zkClient2 = ZooKeeperClient.getZooKeeperClient(getZKAddress());
      try {
        ZooKeeperLock lock1 = new ZooKeeperLock(zkClient1, path);
        try {
          org.kiji.schema.util.ZooKeeperLock lock2 =
              new org.kiji.schema.util.ZooKeeperLock(zkClient2, path);
          try {
            Assert.assertTrue(lock1.lock(1.0));
            Assert.assertFalse(lock2.lock(1.0));
            lock1.unlock();

            Assert.assertTrue(lock2.lock(1.0));
            Assert.assertFalse(lock1.lock(1.0));
            lock2.unlock();
          } finally {
            lock2.close();
          }
        } finally {
          lock1.close();
        }
      } finally {
        zkClient2.release();
      }
    } finally {
      zkClient1.close();
    }
  }
}
