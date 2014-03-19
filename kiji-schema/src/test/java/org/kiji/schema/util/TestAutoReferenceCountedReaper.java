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

package org.kiji.schema.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestAutoReferenceCountedReaper {

  private volatile AutoReferenceCountedReaper mReaper;

  @Before
  public void setUpTestAutoReferenceCountedReaper() throws Exception {
    mReaper = new AutoReferenceCountedReaper();
  }

  @After
  public void tearDownTestAutoReferenceCountedReaper() throws Exception {
    mReaper.close();
  }

  @Test
  public void testReaperWillReapAutoReferenceCounted() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    mReaper.registerAutoReferenceCounted(new LatchAutoReferenceCountable(latch));
    System.gc(); // Force the phantom ref to be enqueued

    Assert.assertTrue(latch.await(1, TimeUnit.SECONDS));
  }

  @Test
  public void testReaperWillReapRegisteredAutoReferenceCountedsWhenClosed() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    final AutoReferenceCounted closeable = new LatchAutoReferenceCountable(latch);
    mReaper.registerAutoReferenceCounted(closeable);
    mReaper.close();
    Assert.assertTrue(latch.await(1, TimeUnit.SECONDS));
  }

  @Test
  public void testCloserWillCloseAutoReferenceCountedInWeakSet() throws Exception {
    Set<AutoReferenceCounted> set =
        Collections.newSetFromMap(
            new MapMaker().weakKeys().<AutoReferenceCounted, Boolean>makeMap());

    final CountDownLatch latch = new CountDownLatch(1);
    AutoReferenceCounted closeable = new LatchAutoReferenceCountable(latch);

    set.add(closeable);
    mReaper.registerAutoReferenceCounted(closeable);
    closeable = null;

    System.gc();
    Assert.assertTrue(latch.await(1, TimeUnit.SECONDS));
  }

  @Test
  public void testCloserWillCloseAutoReferenceCountedInWeakCache() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);

    final LoadingCache<CountDownLatch, AutoReferenceCounted> cache = CacheBuilder
        .newBuilder()
        .weakValues()
        .build(new CacheLoader<CountDownLatch, AutoReferenceCounted>() {
          @Override
          public AutoReferenceCounted load(CountDownLatch latch) throws Exception {
            AutoReferenceCounted closeable = new LatchAutoReferenceCountable(latch);
            mReaper.registerAutoReferenceCounted(closeable);
            return closeable;
          }
        });

    cache.get(latch); // force creation

    System.gc();
    Assert.assertTrue(latch.await(1, TimeUnit.SECONDS));
  }

  @Test
  public void testWeakCacheWillBeCleanedUp() throws Exception {
    CountDownLatch latch = new CountDownLatch(2);

    final LoadingCache<CountDownLatch, AutoReferenceCounted> cache = CacheBuilder
        .newBuilder()
        .weakValues()
        .build(new CacheLoader<CountDownLatch, AutoReferenceCounted>() {
          @Override
          public AutoReferenceCounted load(CountDownLatch latch) throws Exception {
            AutoReferenceCounted closeable = new LatchAutoReferenceCountable(latch);
            mReaper.registerAutoReferenceCounted(closeable);
            return closeable;
          }
        });

    cache.get(latch);
    System.gc();
    cache.get(latch);
    System.gc();
    Assert.assertTrue(latch.await(1, TimeUnit.SECONDS));
  }

  @Test
  public void testWeakCacheWillBeCleanedUp2() throws Exception {
    final LoadingCache<String, Object> cache = CacheBuilder
        .newBuilder()
        .weakValues()
        .build(new CacheLoader<String, Object>() {
          @Override
          public Object load(String key) throws Exception {
            return new Object();
          }
        });

    int hash1 = cache.get("foo").hashCode();
    System.gc();
    int hash2 = cache.get("foo").hashCode();
    System.gc();
    Assert.assertFalse(hash1 == hash2);
  }

  private static final class LatchAutoReferenceCountable implements AutoReferenceCounted {
    private final Closeable mResource;

    private LatchAutoReferenceCountable(CountDownLatch latch) {
      mResource = new LatchCloseable(latch);
    }

    @Override
    public Collection<Closeable> getCloseableResources() {
      return ImmutableList.of(mResource);
    }
  }

  private static final class LatchCloseable implements Closeable {
    private final CountDownLatch mLatch;

    private LatchCloseable(CountDownLatch latch) {
      mLatch = latch;
    }

    @Override
     public void close() throws IOException {
      mLatch.countDown();
    }
  }
}
