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

package org.kiji.schema;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;


public class TestKijiTablePool extends KijiClientTest {
  private KijiTableFactory mTableFactory;

  @Before
  public void setup() throws IOException {
    mTableFactory = createMock(KijiTableFactory.class);
  }

  @Test(expected=IOException.class)
  public void testNoSuchTable() throws IOException {
    KijiTablePool pool = new KijiTablePool(mTableFactory);

    expect(mTableFactory.openTable("table doesn't exist"))
        .andThrow(new IOException("table not found"));

    replay(mTableFactory);
    try {
      pool.get("table doesn't exist");
    } finally {
      pool.close();
    }
  }

  @Test
  public void testGetCachedTable() throws IOException {
    KijiTablePool pool = new KijiTablePool(mTableFactory);

    KijiTable foo1 = createMock(KijiTable.class);
    expect(foo1.getName()).andReturn("foo").anyTimes();
    KijiTable foo2 = createMock(KijiTable.class);
    expect(foo2.getName()).andReturn("foo").anyTimes();
    KijiTable bar1 = createMock(KijiTable.class);
    expect(bar1.getName()).andReturn("bar").anyTimes();
    expect(mTableFactory.openTable("foo")).andReturn(foo1);
    expect(mTableFactory.openTable("foo")).andReturn(foo2);
    expect(mTableFactory.openTable("bar")).andReturn(bar1);

    foo1.close();
    foo2.close();
    bar1.close();

    replay(foo1);
    replay(foo2);
    replay(bar1);
    replay(mTableFactory);

    KijiTable fooTable1 = pool.get("foo");
    KijiTable fooTable2 = pool.get("foo");
    KijiTable barTable1 = pool.get("bar");

    pool.release(fooTable1);
    assertEquals(fooTable1, pool.get("foo"));

    pool.release(fooTable1);
    pool.release(fooTable2);
    pool.release(barTable1);

    pool.close();

    verify(mTableFactory);
  }

  @Test(expected=KijiTablePool.NoCapacityException.class)
  public void testMaxSize() throws IOException {
    KijiTablePool.Options options = new KijiTablePool.Options()
        .withMaxSize(1);
    KijiTablePool pool = new KijiTablePool(mTableFactory, options);

    KijiTable foo = createMock(KijiTable.class);
    expect(foo.getName()).andReturn("foo").anyTimes();
    expect(mTableFactory.openTable("foo")).andReturn(foo);

    replay(foo);
    replay(mTableFactory);

    KijiTable actual = pool.get("foo");

    // The following should fail because the pool is already at max capacity.
    pool.get("foo");
  }

  @Test
  public void testMaxSizeAfterRelease() throws IOException {
    KijiTablePool.Options options = new KijiTablePool.Options()
        .withMaxSize(1);
    KijiTablePool pool = new KijiTablePool(mTableFactory, options);

    KijiTable foo = createMock(KijiTable.class);
    expect(foo.getName()).andReturn("foo").anyTimes();
    expect(mTableFactory.openTable("foo")).andReturn(foo);

    replay(foo);
    replay(mTableFactory);

    KijiTable first = pool.get("foo");
    assertNotNull(first);
    pool.release(first);

    KijiTable second = pool.get("foo");
    assertTrue("Released table should be reused.", first == second);
  }

  @Test
  public void testMinPoolSize() throws IOException {
    KijiTablePool.Options options = new KijiTablePool.Options()
        .withMinSize(3);
    KijiTablePool pool = new KijiTablePool(mTableFactory, options);

    KijiTable foo = createMock(KijiTable.class);
    expect(foo.getName()).andReturn("foo").anyTimes();
    expect(mTableFactory.openTable("foo")).andReturn(foo).anyTimes();

    replay(foo);
    replay(mTableFactory);

    KijiTable first = pool.get("foo");
    assertEquals("Incorrect number of connections in the pool.", 3, pool.getPoolSize("foo"));
  }

  @Test
  public void testIdleTimeout() throws IOException, InterruptedException {
    KijiTablePool.Options options = new KijiTablePool.Options()
        .withIdleTimeout(10)
        .withIdlePollPeriod(1);
    KijiTablePool pool = new KijiTablePool(mTableFactory, options);

    KijiTable foo1 = createMock(KijiTable.class);
    expect(foo1.getName()).andReturn("foo").anyTimes();
    expect(mTableFactory.openTable("foo")).andReturn(foo1);
    foo1.close();
    KijiTable foo2 = createMock(KijiTable.class);
    expect(foo2.getName()).andReturn("foo").anyTimes();
    expect(mTableFactory.openTable("foo")).andReturn(foo2);
    foo2.close();

    replay(foo1);
    replay(foo2);
    replay(mTableFactory);

    KijiTable first = pool.get("foo");
    pool.release(first);
    long releaseTime = System.currentTimeMillis();
    long acquireTime = releaseTime;

    while (acquireTime - releaseTime < 20) {
      // Keep sleeping until we ensure that at least 2 * idleTimeout has elapsed.
      Thread.sleep(20);
      acquireTime = System.currentTimeMillis();
    }

    // Ensure that the pool has an opportunity to clean idle table connections
    // even if the background thread doesn't get to it due to scheduler nondeterminism.
    pool.cleanIdleConnections();
    KijiTable second = pool.get("foo");

    assertFalse("Released table should not be reused, since it was idle and closed.",
        first == second);

    pool.release(second);
    pool.close();
  }
}
