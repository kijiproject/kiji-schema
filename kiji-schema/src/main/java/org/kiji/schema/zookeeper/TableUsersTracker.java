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
import java.io.IOException;
import java.net.URLDecoder;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiURI;

/**
 *
 */
public class TableUsersTracker implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(TableUsersTracker.class);

  private final KijiURI mTableURI;

  private final PathChildrenCache mCache;

  private final TableUsersUpdateHandler mHandler;

  /**
   * Initializes a new users tracker with the given update handler on the specified table.
   *
   * @param zkClient connection to ZooKeeper.
   * @param tableURI Tracks the users of the table with this URI.
   * @param handler Handler to process updates of the table users list.
   */
  public TableUsersTracker(
      CuratorFramework zkClient,
      KijiURI tableURI,
      TableUsersUpdateHandler handler) {
    mTableURI = tableURI;
    mHandler = handler;

    mCache = new PathChildrenCache(
        zkClient,
        ZooKeeperUtils.getTableUsersDir(mTableURI).getPath(),
        false);

    mCache.getListenable().addListener(new PathChildrenCacheListener() {
      @Override
      public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
          throws Exception {
        LOG.debug("Table users tracker event recieved for table {}: {}.", mTableURI, event);
        switch (event.getType()) {
          case CHILD_ADDED:
          case CHILD_UPDATED:
          case CHILD_REMOVED:
          case INITIALIZED: {
            mHandler.update(getTableUsers());
            break;
          }
          default: break; // Connection state changes are already logged at the client level.
        }
      }
    });
  }

  /**
   * Starts the tracker.  Will asynchronously trigger an initial update to the handler.
   *
   * @throws IOException on unrecoverable ZooKeeper error.
   * @return this.
   */
  public TableUsersTracker start() throws IOException {
    LOG.debug("Starting table user tracker for table {}.", mTableURI);
    try {
      mCache.start(StartMode.POST_INITIALIZED_EVENT);
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }
    return this;
  }

  /**
   * Retrieve a multimap of the current table users to their respective table layout version.
   *
   * @return a multimap of the current table users to their respective table layout version.
   * @throws IOException on failure.
   */
  private Multimap<String, String> getTableUsers() throws IOException {
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
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(TableUsersTracker.class).add("tableURI", mTableURI).toString();
  }
}
