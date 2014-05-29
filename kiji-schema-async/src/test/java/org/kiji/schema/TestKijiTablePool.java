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
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.KijiTablePool.NoCapacityException;
import org.kiji.schema.util.ResourceUtils;

public class TestKijiTablePool extends KijiClientTest {
  private KijiTableFactory mTableFactory;

  @Before
  public void setup() throws IOException {
    mTableFactory = createMock(KijiTableFactory.class);
  }

  @Test
  public void testNoSuchTable() throws IOException {
    KijiTablePool pool = KijiTablePool.newBuilder(mTableFactory).build();

    expect(mTableFactory.openTable("table doesn't exist"))
        .andThrow(new IOException("table not found"));

    replay(mTableFactory);
    try {
      pool.get("table doesn't exist");
    } catch (IOException ioe) {
      assertEquals("table not found", ioe.getMessage());
    } finally {
      pool.close();
    }
  }

  @Test
  public void testGetCachedTable() throws IOException {
    KijiTablePool pool = KijiTablePool.newBuilder(mTableFactory).build();

    try {
      KijiTable foo1 = createMock(KijiTable.class);
      expect(foo1.getName()).andReturn("foo").anyTimes();
      expect(foo1.getURI()).andReturn(KijiURI.newBuilder("kiji://.env/foo").build()).anyTimes();
      KijiTable foo2 = createMock(KijiTable.class);
      expect(foo2.getName()).andReturn("foo").anyTimes();
      expect(foo2.getURI()).andReturn(KijiURI.newBuilder("kiji://.env/foo").build()).anyTimes();
      KijiTable bar1 = createMock(KijiTable.class);
      expect(bar1.getName()).andReturn("bar").anyTimes();
      expect(bar1.getURI()).andReturn(KijiURI.newBuilder("kiji://.env/bar").build()).anyTimes();
      expect(mTableFactory.openTable("foo")).andReturn(foo1);
      expect(mTableFactory.openTable("foo")).andReturn(foo2);
      expect(mTableFactory.openTable("bar")).andReturn(bar1);

      ResourceUtils.releaseOrLog(foo1);
      ResourceUtils.releaseOrLog(foo2);
      ResourceUtils.releaseOrLog(bar1);

      replay(foo1);
      replay(foo2);
      replay(bar1);
      replay(mTableFactory);

      KijiTable fooTable1 = pool.get("foo");
      KijiTable fooTable2 = pool.get("foo");
      KijiTable barTable1 = pool.get("bar");

      fooTable1.release();
      assertEquals(fooTable1, pool.get("foo"));

      fooTable1.release();
      fooTable2.release();
      barTable1.release();
    } finally {
      pool.close();
    }

    verify(mTableFactory);
  }

  @Test
  public void testMaxSize() throws IOException {
    KijiTablePool pool = KijiTablePool.newBuilder(mTableFactory)
        .withMaxSize(1)
        .build();

    try {
      KijiTable foo = createMock(KijiTable.class);
      expect(foo.getName()).andReturn("foo").anyTimes();
      expect(foo.getURI()).andReturn(KijiURI.newBuilder("kiji://.env/foo").build()).anyTimes();
      expect(mTableFactory.openTable("foo")).andReturn(foo);

      // The table should be closed when the pool is closed.
      foo.release();
      expectLastCall().once();

      replay(foo);
      replay(mTableFactory);

      KijiTable actual = pool.get("foo");

      try {
        // The following should fail because the pool is already at max capacity.
        pool.get("foo");
        fail("An exception should have been thrown.");
      } finally {
        actual.release();
      }
    } catch (NoCapacityException nce) {
      assertEquals("Reached max pool size for table foo. There are 1 tables in the pool.",
          nce.getMessage());
    } finally {
      pool.close();
    }
  }

  @Test
  public void testMaxSizeAfterRelease() throws IOException {
    KijiTablePool pool = KijiTablePool.newBuilder(mTableFactory)
        .withMaxSize(1)
        .build();

    try {
      KijiTable foo = createMock(KijiTable.class);
      expect(foo.getName()).andReturn("foo").anyTimes();
      expect(foo.getURI()).andReturn(KijiURI.newBuilder("kiji://.env/foo").build()).anyTimes();
      expect(mTableFactory.openTable("foo")).andReturn(foo);

      replay(foo);
      replay(mTableFactory);

      KijiTable first = pool.get("foo");
      assertNotNull(first);
      first.release();

      KijiTable second = pool.get("foo");
      assertTrue("Released table should be reused.", first == second);
    } finally {
      pool.close();
    }
  }

  @Test
  public void testMinPoolSize() throws IOException {
    KijiTablePool pool = KijiTablePool.newBuilder(mTableFactory)
        .withMinSize(3)
        .build();

    try {
      KijiTable foo = createMock(KijiTable.class);
      expect(foo.getName()).andReturn("foo").anyTimes();
      expect(foo.getURI()).andReturn(KijiURI.newBuilder("kiji://.env/foo").build()).anyTimes();
      expect(mTableFactory.openTable("foo")).andReturn(foo).anyTimes();

      replay(foo);
      replay(mTableFactory);

      KijiTable first = pool.get("foo");
      KijiTable second = pool.get("foo");
      KijiTable third = pool.get("foo");
      assertEquals("Incorrect number of connections in the pool.", 3, pool.getPoolSize("foo"));
    } finally {
      pool.close();
    }
  }

  @Test
  public void testIdleTimeout() throws IOException, InterruptedException {
    KijiTablePool pool = KijiTablePool.newBuilder(mTableFactory)
        .withIdleTimeout(10)
        .withIdlePollPeriod(1)
        .build();

    try {
      KijiTable foo1 = createMock(KijiTable.class);
      expect(foo1.getName()).andReturn("foo").anyTimes();
      expect(foo1.getURI()).andReturn(KijiURI.newBuilder("kiji://.env/foo").build()).anyTimes();
      expect(mTableFactory.openTable("foo")).andReturn(foo1);
      ResourceUtils.releaseOrLog(foo1);
      KijiTable foo2 = createMock(KijiTable.class);
      expect(foo2.getName()).andReturn("foo").anyTimes();
      expect(foo2.getURI()).andReturn(KijiURI.newBuilder("kiji://.env/foo").build()).anyTimes();
      expect(mTableFactory.openTable("foo")).andReturn(foo2);
      ResourceUtils.releaseOrLog(foo2);

      replay(foo1);
      replay(foo2);
      replay(mTableFactory);

      KijiTable first = pool.get("foo");
      first.release();
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
    } finally {
      pool.close();
    }
  }

  @Test
  public void testRetainOperation() throws IOException {
    KijiTablePool pool = KijiTablePool.newBuilder(mTableFactory).build();

    try {
      KijiTable foo = createMock(KijiTable.class);
      expect(foo.getName()).andReturn("foo").anyTimes();
      expect(foo.getURI()).andReturn(KijiURI.newBuilder("kiji://.env/foo").build()).anyTimes();
      expect(mTableFactory.openTable("foo")).andReturn(foo);
      // When the pool is closed, the Mock KijiTable should be released.
      foo.release();
      expectLastCall().once();

      replay(foo);
      replay(mTableFactory);

      KijiTable fooTable = pool.get("foo");
      fooTable.retain();
      fooTable.release(); // Corresponds to the retain
      fooTable.release(); // It puts the table back in the pool.
    } finally {
      pool.close();
    }
  }

  @Test
  public void testRetainAfterRelease() throws IOException {
    KijiTablePool pool = KijiTablePool.newBuilder(mTableFactory).build();

    try {
      KijiTable foo = createMock(KijiTable.class);
      expect(foo.getName()).andReturn("foo").anyTimes();
      expect(foo.getURI()).andReturn(KijiURI.newBuilder("kiji://.env/foo").build()).anyTimes();
      expect(mTableFactory.openTable("foo")).andReturn(foo);
      // When the pool is closed, the mock KijiTable should be released.
      foo.release();
      expectLastCall().once();

      replay(foo);
      replay(mTableFactory);

      KijiTable fooTable = pool.get("foo");
      fooTable.release();
      try {
        fooTable.retain();
        fail("Should throw an IllegalStateException.");
      } catch (IllegalStateException ise) {
        assertTrue(ise.getMessage().endsWith("retain counter was 2."));
      }
    } finally {
      pool.close();
    }
  }

  @Test
  public void testTooManyReleases() throws IOException {
    KijiTable foo = createMock(KijiTable.class);
    expect(foo.getName()).andReturn("foo").anyTimes();
    expect(foo.getURI()).andReturn(KijiURI.newBuilder("kiji://.env/foo").build()).anyTimes();
    expect(mTableFactory.openTable("foo")).andReturn(foo);

    // The mock KijiTable should be released when the pool is closed.
    foo.release();
    expectLastCall().once();

    replay(foo);
    replay(mTableFactory);

    KijiTablePool pool = KijiTablePool.newBuilder(mTableFactory).build();

    try {
      KijiTable fooTable = pool.get("foo");
      fooTable.retain();
      fooTable.release();
      fooTable.release();
      try {
        fooTable.release();
        fail("Should throw an IllegalStateException.");
      } catch (IllegalStateException ise) {
        assertTrue(ise.getMessage().endsWith("retain counter is now 0."));
      }
    } finally {
      pool.close();
    }
  }

}
