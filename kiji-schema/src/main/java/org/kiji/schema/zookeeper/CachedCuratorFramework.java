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

import java.io.IOException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.InternalKijiError;
import org.kiji.schema.util.DebugResourceTracker;
import org.kiji.schema.util.ReferenceCountedCache;

/**
 * A wrapper around a {@link CuratorFramework} which is taken from a
 * {@link ReferenceCountedCache} of real {@link CuratorFramework} connections.
 * Upon being closed, the reference count in the cache will be decremented. Additionally, this
 * wrapper tracks unclosed resources through the {@link DebugResourceTracker} mechanism.
 *
 * @param <K> the type of the cache's key.
 */
public final class CachedCuratorFramework<K> extends CuratorFrameworkImpl {
  private static final Logger LOG = LoggerFactory.getLogger(CachedCuratorFramework.class);
  private final ReferenceCountedCache<K, CuratorFramework> mCache;
  private final K mCacheKey;

  /**
   * Create a new cached CuratorFramework with the provided cache and key.
   *
   * @param cache holding delegate CuratorFramework instances.
   * @param key key in cache.
   * @param delegate to proxy through ZooKeeper operations.
   */
  private CachedCuratorFramework(
      final ReferenceCountedCache<K, CuratorFramework> cache,
      final K key,
      final CuratorFrameworkImpl delegate
  ) {
    super(delegate);
    mCache = cache;
    mCacheKey = key;
    DebugResourceTracker
        .get()
        .registerResource(this, ExceptionUtils.getStackTrace(new Exception()));
  }

  /**
   * Create a new cached Curator Framework with the provided cache, key and namespace.
   *
   * @param cache holding delegate CuratorFramework instances.
   * @param key key in cache.
   * @param <K> type of key in cache.
   * @param namespace to constrain client to, or null for root.
   * @return a new CachedCuratorFramework with a shared underlying ZooKeeper connection.
   */
  public static <K> CachedCuratorFramework<K> create(
      final ReferenceCountedCache<K, CuratorFramework> cache,
      final K key,
      final String namespace
  ) {
    // Cast is safe; all implementations of CuratorFramework subclass CuratorFrameworkImpl
    return new CachedCuratorFramework<K>(
        cache,
        key,
        (CuratorFrameworkImpl) cache.get(key).usingNamespace(namespace));
  }

  /** {@inheritDoc} */
  @Override
  public void start() {
    throw new UnsupportedOperationException("Cached CuratorFramework may not be started.");
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
