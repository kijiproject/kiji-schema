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

package org.apache.curator.framework.imps;

import java.io.IOException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.curator.framework.CuratorFramework;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.util.DebugResourceTracker;
import org.kiji.schema.util.ReferenceCountedCache;

/**
 * A utility class which provides {@link CuratorFramework} implementations which share an underlying
 * cached connection. The underlying connection is cached in a {@link ReferenceCountedCache}. The
 * {@link CuratorFramework} implementations returned by
 * {@link #create(ReferenceCountedCache, Object, String)}} will automatically manage releasing
 * reference counts from the underlying cache when closed. Additionally, the
 * {@link CuratorFramework}s created by this class will be registered with the
 * {@link DebugResourceTracker} mechanism.
 *
 * Must be in the {@code org.apache.curator.framework.imps} package in order to subclass
 * {@link NamespaceFacade}, which only provides a package private constructor.
 */
@ApiAudience.Private
public class CachedCuratorFramework {

  /**
   * Create a new cached Curator Framework with the provided cache, key and namespace.
   *
   * @param cache holding delegate CuratorFramework instances.
   * @param key key in cache.
   * @param namespace to constrain client to, or null for root.
   * @return a new CuratorFramework with a shared ZooKeeper connection and resource tracking.
   * @param <K> type of key in cache.
   */
  public static <K> CuratorFramework create(
      final ReferenceCountedCache<K, CuratorFramework> cache,
      final K key,
      final String namespace
  ) {
    // Cast is safe; all implementations of CuratorFramework subclass CuratorFrameworkImpl
    final CuratorFrameworkImpl delegate = (CuratorFrameworkImpl) cache.get(key);

    if (namespace == null) {
      return new CachedCuratorFrameworkImpl<K>(cache, key, delegate);
    } else {
      return new CachedNamespacedCuratorFramework<K>(cache, key, delegate, namespace);
    }
  }

  /**
   * Private constructor for utility class.
   */
  private CachedCuratorFramework() {}

  /**
   * A wrapper around a non-namespaced {@link CuratorFrameworkImpl}.
   * @param <K> type of cache key.
   */
  private static final class CachedCuratorFrameworkImpl<K> extends CuratorFrameworkImpl {
    private final ReferenceCountedCache<K, CuratorFramework> mCache;
    private final K mCacheKey;

    /**
     * Create a new cached CachedCuratorFrameworkImpl with the provided cache and key.
     *
     * @param cache holding delegate CuratorFramework instances.
     * @param key key in cache.
     * @param delegate to proxy through ZooKeeper operations.
     */
    private CachedCuratorFrameworkImpl(
        final ReferenceCountedCache<K, CuratorFramework> cache,
        final K key,
        final CuratorFrameworkImpl delegate
    ) {
      super(delegate);
      mCache = cache;
      mCacheKey = key;
      DebugResourceTracker.get().registerResource(this);
    }


    /** {@inheritDoc} */
    @Override
    public void start() {
      throw new UnsupportedOperationException("Kiji-cached curator framework may not be started.");
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
      try {
        mCache.release(mCacheKey);
      } catch (IOException e) {
        // Impossible since {@link CuratorFramework#close()} does not throw IOException.
        throw new InternalKijiError(e);
      } finally {
        DebugResourceTracker.get().unregisterResource(this);
      }
    }
  }

  /**
   * A wrapper around a namespaced {@link CuratorFrameworkImpl}.
   * @param <K> type of cache key.
   */
  private static final class CachedNamespacedCuratorFramework<K> extends NamespaceFacade {
    private final ReferenceCountedCache<K, CuratorFramework> mCache;
    private final K mCacheKey;

    /**
     * Create a new cached CachedCuratorFrameworkImpl with the provided cache and key.
     *
     * @param cache holding delegate CuratorFramework instances.
     * @param key key in cache.
     * @param delegate to proxy through ZooKeeper operations.
     */
    private CachedNamespacedCuratorFramework(
        final ReferenceCountedCache<K, CuratorFramework> cache,
        final K key,
        final CuratorFrameworkImpl delegate,
        final String namespace
    ) {
      super(delegate, namespace);
      mCache = cache;
      mCacheKey = key;
      DebugResourceTracker.get().registerResource(this);
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
      throw new UnsupportedOperationException("Kiji-cached curator framework may not be started.");
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
      try {
        mCache.release(mCacheKey);
      } catch (IOException e) {
        // Impossible since {@link CuratorFramework#close()} does not throw IOException.
        throw new InternalKijiError(e);
      } finally {
        DebugResourceTracker.get().unregisterResource(this);
      }
    }
  }
}
