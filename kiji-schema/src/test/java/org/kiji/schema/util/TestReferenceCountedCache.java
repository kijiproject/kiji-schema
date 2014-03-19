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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestReferenceCountedCache {
  private static final Logger LOG = LoggerFactory.getLogger(TestReferenceCountedCache.class);

  @Test
  public void referenceCountedCacheKeepsValuesCached() throws Exception {
    String key = "key";
    ReferenceCountedCache<String, CloseableOnce> cache =
        ReferenceCountedCache.create(new KeyedCloseableOnceLoader<String>());

    CloseableOnce ref1 = cache.get(key);
    CloseableOnce ref2 = cache.get(key);
    CloseableOnce ref3 = cache.get(key);

    Assert.assertTrue(ref1.isOpen());
    Assert.assertSame(ref1, ref2);
    Assert.assertSame(ref2, ref3);

    cache.close();
  }

  @Test
  public void referenceCountedCacheInvalidatesUnreferencedEntries() throws Exception {
    String key = "key";
    ReferenceCountedCache<String, CloseableOnce> cache =
        ReferenceCountedCache.create(new KeyedCloseableOnceLoader<String>());

    CloseableOnce ref = cache.get(key);
    cache.get(key);
    cache.get(key);

    cache.release(key);
    cache.release(key);
    cache.release(key);

    Assert.assertFalse(ref.isOpen());
    cache.close();
  }

  @Test
  public void referenceCountedCacheWorksConcurrently() throws Exception {
    // This test basically throws a bunch of threads at a cache, and makes sure that the values
    // returned from the cache are valid (open, and created from the correct key).

    final ReferenceCountedCache<Integer, CloseableOnce> cache =
        ReferenceCountedCache.create(new KeyedCloseableOnceLoader<Integer>());

    final int numThreads = 100;
    final int contentionFactor = 100; // Roughly the number of threads which will be contending for
                                      // each key at a time
    final int numRounds = 1000; // Do it a bunch to trigger non-determinism

    final CyclicBarrier barrier = new CyclicBarrier(numThreads);
    final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    final AtomicLong seed = new AtomicLong(System.currentTimeMillis());

    ImmutableList.Builder<Integer> builder = ImmutableList.builder();
    for (int i = 0; i < numThreads / contentionFactor; i++) {
      builder.add(i);
    }
    final List<Integer> keys = builder.build();

    final List<Callable<Void>> callables = Lists.newArrayList();

    for (int i = 0; i < numThreads; i++) {
      callables.add(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          for (int i = 0; i < numRounds; i++) {
            List<Integer> shuffledKeys = Lists.newArrayList(keys);
            Collections.shuffle(shuffledKeys, new Random(seed.incrementAndGet()));
            barrier.await();
            for (Integer key : shuffledKeys) {
              CloseableOnce closeable = cache.get(key);
              Assert.assertTrue(closeable.isOpen());
              Assert.assertEquals(key, closeable.getKey());
              cache.release(key);
            }
          }
          return null;
        }
      });
    }

    for (Future<Void> result : executor.invokeAll(callables)) {
      result.get(); // will throw if failed
    }

    for (Integer key : keys) {
      Assert.assertFalse(cache.containsKey(key));
    }
    cache.close();
  }

  @Test
  public void referenceCountedCacheClosesCachedValuesOnClose() throws Exception {
    ReferenceCountedCache<String, CloseableOnce> cache =
        ReferenceCountedCache.create(new KeyedCloseableOnceLoader<String>());

    CloseableOnce ref1 = cache.get("a");
    CloseableOnce ref2 = cache.get("b");
    CloseableOnce ref3 = cache.get("c");

    Assert.assertTrue(cache.containsKey("a"));
    Assert.assertTrue(cache.containsKey("b"));
    Assert.assertTrue(cache.containsKey("c"));

    cache.close();

    Assert.assertFalse(ref1.isOpen());
    Assert.assertFalse(ref2.isOpen());
    Assert.assertFalse(ref3.isOpen());

    Assert.assertFalse(cache.containsKey("a"));
    Assert.assertFalse(cache.containsKey("b"));
    Assert.assertFalse(cache.containsKey("c"));
  }

  @Test(expected = IllegalStateException.class)
  public void closeableOnceFailsWhenDoubleClosed() throws Exception {
    Closeable closeable = new CloseableOnce<Object>(null);
    closeable.close();
    closeable.close();
  }

  /**
   * A function which produces a new {@link CloseableOnce} when evaluated with a key.
   */
  private static final class KeyedCloseableOnceLoader<K>
      implements Function<K, CloseableOnce> {
    @Nullable
    @Override
    public CloseableOnce<K> apply(@Nullable K key) {
      return new CloseableOnce<K>(key);
    }
  }

  /**
   * A Closeable which can only be closed once. Note that this class breaks the contract of
   * {@link java.io.Closeable}, but it is convenient for testing the existence of duplicate
   * {@link #close()} calls.
   */
  private static final class CloseableOnce<K> implements Closeable {
    private K mKey;

    /**
     * Create a CloseableOnce with the provided key.
     *
     * @param key of this CloseableOnce.
     */
    private CloseableOnce(K key) {
      mKey = key;
    }

    private final AtomicBoolean mIsOpen = new AtomicBoolean(true);
    @Override
    public void close() throws IOException {
      Preconditions.checkState(mIsOpen.compareAndSet(true, false));
    }

    /**
     * Returns whether this CloseableOnce is still open (has not had {@link #close()} called on it.
     *
     * @return whether this CloseableOnce is open.
     */
    public boolean isOpen() {
      return mIsOpen.get();
    }

    /**
     * Retrieve the key used to create this CloseableOnce.
     *
     * @return the key used to create this CloseableOnce.
     */
    public K getKey() {
      return mKey;
    }
  }
}
