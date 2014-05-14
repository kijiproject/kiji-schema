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
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiURI;

/**
 * Tracks the layout of a table and updates a registered handler.
 *
 */
public class TableLayoutTracker implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(TableLayoutTracker.class);
  private static final ThreadFactory THREAD_FACTORY =
      new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat(TableLayoutTracker.class.getCanonicalName() + "-%d")
          .setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
              LOG.warn("Exception thrown in table layout update handler: {}.", e.getMessage());
            }
          })
          .build();

  private final KijiURI mTableURI;

  private final NodeCache mCache;

  private final TableLayoutUpdateHandler mHandler;

  private final ExecutorService mExecutor;

  /**
   * Creates a tracker for a table layout.
   *
   * <p> The handler should not perform any blocking operations, as subsequent ZooKeeper
   * notifications will be blocked. </p>
   *
   * <p> The tracker must be opened and closed. </p>
   *
   * @param zkClient ZooKeeper client.
   * @param tableURI Tracks the layout of the table with this URI.
   * @param handler Handler invoked to process table layout updates.
   */
  public TableLayoutTracker(
      final CuratorFramework zkClient,
      final KijiURI tableURI,
      final TableLayoutUpdateHandler handler
  ) {
    mHandler = handler;
    mTableURI = tableURI;
    mCache = new NodeCache(zkClient, ZooKeeperUtils.getTableLayoutFile(mTableURI).getPath());
    mExecutor = Executors.newSingleThreadExecutor(THREAD_FACTORY);
    mCache.getListenable().addListener(new NodeCacheListener() {
      /** {@inheritDoc}. */
      @Override
      public void nodeChanged() throws Exception {
        mHandler.update(getLayoutID());
      }
    }, mExecutor);
  }

  /**
   * Start this table layout tracker. Asynchronously updates the handler with the initial layout.
   *
   * @return this.
   * @throws IOException on unrecoverable ZooKeeper error.
   */
  public TableLayoutTracker start() throws IOException {
    try {
      mCache.start(true);
      mExecutor.execute(new Runnable() {
        @Override
        public void run() {
          mHandler.update(getLayoutID());
        }
      });
      return this;
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      throw new KijiIOException(e);
    }
  }

  /**
   * Retrieve the current layout ID from ZooKeeper, or null if it does not exist.
   *
   * @return the current layout ID, or null if it does not exist.
   */
  public String getLayoutID() {
    ChildData currentData = mCache.getCurrentData();
    if (currentData == null) {
      return null;
    } else {
      return Bytes.toString(currentData.getData());
    }
  }

  /** {@inheritDoc}. */
  @Override
  public void close() throws IOException {
    mCache.close();
    mExecutor.shutdown();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(TableLayoutTracker.class).add("tableURI", mTableURI).toString();
  }
}
