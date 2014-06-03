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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks users of a table or instance. Allows retrieving the current users, and allows callbacks
 * to be registered which fire on changes to the set of registered users.
 */
public class UsersTracker implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(UsersTracker.class);

  private final File mUsersDir;

  private final PathChildrenCache mCache;

  private final Set<UsersUpdateHandler> mHandlers =
      Collections.newSetFromMap(Maps.<UsersUpdateHandler, Boolean>newConcurrentMap());

  /**
   * Initializes a new users tracker. Package private, users should use the
   * {@code newTableUsersTracker()} or {@code newInstanceUsersTracker()} factory methods in
   * {@link org.kiji.schema.zookeeper.ZooKeeperUtils} to construct a new {@link UsersTracker}.
   *
   * @param zkClient connection to ZooKeeper.
   * @param usersDir path to the directory containing user nodes.
   */
  UsersTracker(
      CuratorFramework zkClient,
      File usersDir
  ) {
    mUsersDir = usersDir;
    mCache = new PathChildrenCache(zkClient, usersDir.getPath(), false);
  }

  /**
   * Register a new update handler on this users tracker.
   *
   * @param handler to register.
   * @return this.
   */
  public UsersTracker registerUpdateHandler(UsersUpdateHandler handler) {
    mHandlers.add(handler);
    return this;
  }

  /**
   * Unregister an existing update handler from this users tracker.
   *
   * @param handler to unregister.
   * @return this.
   */
  public UsersTracker unregisterUpdateHandler(UsersUpdateHandler handler) {
    mHandlers.remove(handler);
    return this;
  }

  /**
   * Update the registered update handlers with the current users.
   *
   * @throws IOException on unrecoverable error.
   */
  public void updateRegisteredHandlers() throws IOException {
    final Multimap<String, String> users = getUsers();
    for (UsersUpdateHandler handler : mHandlers) {
      handler.update(users);
    }
  }

  /**
   * Starts the tracker.  Will block until the users list is initialized. When the block is
   * initialized all handlers will be invoked in another thread.
   *
   * @throws IOException on unrecoverable ZooKeeper error.
   * @return this.
   */
  public UsersTracker start() throws IOException {
    LOG.debug("Starting table user tracker for path {}.", mUsersDir);
    final CountDownLatch initializationLatch = new CountDownLatch(1);

    mCache.getListenable().addListener(
        new PathChildrenCacheListener() {
          @Override
          public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
              throws Exception {
            LOG.debug("Users tracker event received for path {}: {}.", mUsersDir, event);
            switch (event.getType()) {
              case CHILD_ADDED:
              case CHILD_UPDATED:
              case CHILD_REMOVED: {
                updateRegisteredHandlers();
                break;
              }
              case INITIALIZED: {
                initializationLatch.countDown();
                updateRegisteredHandlers();
                break;
              }
              default: break; // Connection state changes are already logged at the client level.
            }
          }
        });

    try {
      mCache.start(StartMode.POST_INITIALIZED_EVENT);
      if (!initializationLatch.await(5, TimeUnit.SECONDS)) {
        throw new IOException("Unable to start ZooKeeper path cache before timeout.");
      }
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }
    return this;
  }

  /**
   * Retrieve a multimap of the current users to their respective value (either table layout version
   * if tracking table users, or system version if tracking instance users).
   *
   * @return a multimap of the current table users to their respective value.
   * @throws IOException on failure.
   */
  public Multimap<String, String> getUsers() throws IOException {
    ImmutableMultimap.Builder<String, String> users = ImmutableSetMultimap.builder();
    for (ChildData child : mCache.getCurrentData()) {
      String nodeName = Iterables.getLast(Splitter.on('/').split(child.getPath()));
      String[] split = nodeName.split(ZooKeeperUtils.ZK_NODE_NAME_SEPARATOR);
      if (split.length != 2) {
        LOG.error("Ignoring invalid ZooKeeper table user node: {}.", nodeName);
        continue;
      }
      final String userID = URLDecoder.decode(split[0], Charsets.UTF_8.displayName());
      final String layoutID = URLDecoder.decode(split[1], Charsets.UTF_8.displayName());

      users.put(userID, layoutID);
    }
    return users.build();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    LOG.debug("Closing UsersTracker {}.", this);
    mCache.close();
    mHandlers.clear();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(UsersTracker.class).add("directory", mUsersDir).toString();
  }
}
