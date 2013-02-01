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

package org.kiji.schema.hbase;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;

import org.kiji.delegation.Priority;
import org.kiji.schema.KijiURI;
import org.kiji.schema.impl.DefaultHBaseAdminFactory;
import org.kiji.schema.impl.DefaultHTableInterfaceFactory;
import org.kiji.schema.impl.HBaseAdminFactory;
import org.kiji.schema.impl.HTableInterfaceFactory;
import org.kiji.schema.util.LocalLockFactory;
import org.kiji.schema.util.LockFactory;
import org.kiji.schema.util.ZooKeeperLockFactory;
import org.kiji.testing.fakehtable.FakeHBase;

/** Factory for HBase instances based on URIs. */
public final class TestingHBaseFactory implements HBaseFactory {

  /** Map from fake HBase ID to fake HBase instances. */
  private final Map<String, FakeHBase> mFakeHBase = Maps.newHashMap();

  /** Map from fake HBase ID to fake (local) lock factories. */
  private final Map<String, LockFactory> mLock = Maps.newHashMap();

  /**
   * Public constructor. This should not be directly invoked by users; you should
   * use HBaseFactory.get(), which retains a singleton instance.
   */
  public TestingHBaseFactory() {
  }

  /** URIs for fake HBase instances are "kiji://.fake.[fake-id]/instance/table". */
  private static final String FAKE_HBASE_ID_PREFIX = ".fake.";

  /**
   * Extracts the ID of the fake HBase from a Kiji URI.
   *
   * @param uri URI to extract a fake HBase ID from.
   * @return the fake HBase ID, if any, or null.
   */
  private static String getFakeHBaseID(KijiURI uri) {
    if (uri.getZookeeperQuorum().size() != 1) {
      return null;
    }
    final String zkHost = uri.getZookeeperQuorum().get(0);
    if (!zkHost.startsWith(FAKE_HBASE_ID_PREFIX)) {
      return null;
    }
    return zkHost.substring(FAKE_HBASE_ID_PREFIX.length());
  }

  /**
   * Gets the FakeHBase for a given HBase URI.
   *
   * @param uri URI of a fake HBase instance.
   * @return the FakeHBase for the specified URI, or null if the URI does not specify a fake HBase.
   */
  public FakeHBase getFakeHBase(KijiURI uri) {
    final String fakeID = getFakeHBaseID(uri);
    if (fakeID == null) {
      return null;
    }
    synchronized (mFakeHBase) {
      final FakeHBase hbase = mFakeHBase.get(fakeID);
      if (hbase != null) {
        return hbase;
      }
      final FakeHBase newFake = new FakeHBase();
      mFakeHBase.put(fakeID, newFake);
      return newFake;
    }
  }

  /** {@inheritDoc} */
  @Override
  public HTableInterfaceFactory getHTableInterfaceFactory(KijiURI uri) {
    final FakeHBase fake = getFakeHBase(uri);
    if (fake != null) {
      return fake.getHTableFactory();
    }
    return DefaultHTableInterfaceFactory.get();
  }

  /** {@inheritDoc} */
  @Override
  public HBaseAdminFactory getHBaseAdminFactory(KijiURI uri) {
    final FakeHBase fake = getFakeHBase(uri);
    if (fake != null) {
      return fake.getAdminFactory();
    }
    return DefaultHBaseAdminFactory.get();
  }

  /** {@inheritDoc} */
  @Override
  public LockFactory getLockFactory(KijiURI uri, Configuration conf) throws IOException {
    final String fakeID = getFakeHBaseID(uri);
    if (fakeID != null) {
      synchronized (mLock) {
        final LockFactory factory = mLock.get(fakeID);
        if (factory != null) {
          return factory;
        }
        final LockFactory newFactory = new LocalLockFactory();
        mLock.put(fakeID, newFactory);
        return newFactory;
      }
    }
    return new ZooKeeperLockFactory(conf);
  }

  /** Resets the testing HBase factory. */
  public void reset() {
    mFakeHBase.clear();
    mLock.clear();
  }

  /** {@inheritDoc} */
  @Override
  public int getPriority(Map<String, String> runtimeHints) {
    // Higher priority than default factory.
    return Priority.HIGH;
  }
}
