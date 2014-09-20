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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.util.DebugResourceTracker;
import org.kiji.schema.util.Lock;
import org.kiji.schema.util.Time;

/** Distributed lock on top of ZooKeeper. */
@ApiAudience.Private
public final class ZooKeeperLock implements Lock {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperLock.class);
  private static final String LOCK_NAME_PREFIX = "lock-";

  private final CuratorFramework mZKClient;
  private final File mLockDir;
  private final File mLockPathPrefix;

  @GuardedBy("this")
  private File mCreatedPath = null;

  /**
   * Constructs a ZooKeeper lock object.
   *
   * @param zkClient ZooKeeper client.  Will be not be closed by this lock.
   * @param lockDir Path of the directory node to use for the lock.
   */
  public ZooKeeperLock(CuratorFramework zkClient, File lockDir) {
    mZKClient = zkClient;
    mLockDir = lockDir;
    mLockPathPrefix = new File(lockDir, LOCK_NAME_PREFIX);
    DebugResourceTracker.get().registerResource(this);
  }

  /** Watches the lock directory node. */
  private class LockWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      LOG.debug("ZooKeeperLock.LockWatcher: received event {}", event);
      synchronized (ZooKeeperLock.this) {
        ZooKeeperLock.this.notifyAll();
      }
    }
  }

  private final LockWatcher mLockWatcher = new LockWatcher();

  /** {@inheritDoc} */
  @Override
  public void lock() throws IOException {
    if (!lock(0.0)) {
      // This should never happen, instead lock(0.0) should raise a KeeperException!
      throw new RuntimeException("Unable to acquire ZooKeeper lock.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean lock(double timeout) throws IOException {
    /** Absolute time deadline, in seconds since Epoch */
    final double absoluteDeadline = (timeout > 0.0) ? Time.now() + timeout : 0.0;

    synchronized (this) {
      Preconditions.checkState(null == mCreatedPath, mCreatedPath);
      // Queues for access to the lock:
      try {
        mCreatedPath = new File(
            mZKClient
                .create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(mLockPathPrefix.getPath()));
      } catch (Exception e) {
        ZooKeeperUtils.wrapAndRethrow(e);
      }
      LOG.debug("{}: queuing for lock with node {}", this, mCreatedPath);
    }

    while (true) {
      try {
        // Do we own the lock already, or do we have to wait?
        final List<String> children = mZKClient.getChildren().forPath(mLockDir.getPath());
        Collections.sort(children);
        LOG.debug("{}: lock queue: {}", this, children);
        synchronized (this) {
          final int index = Collections.binarySearch(children, mCreatedPath.getName());
          if (index == 0) {
            // We own the lock:
            LOG.debug("{}: lock acquired", this);
            DebugResourceTracker.get().registerResource(
                mCreatedPath,
                String.format("Locked (acquired) ZooKeeper lock: %s.", this));
            return true;
          } else { // index >= 1
            final String preceding = new File(mLockDir, children.get(index - 1)).getPath();
            LOG.debug("{}: waiting for preceding node {} to disappear", this, preceding);
            if (mZKClient.checkExists().usingWatcher(mLockWatcher).forPath(preceding) != null) {
              if (absoluteDeadline > 0.0) {
                final long timeLeftMS = (long) ((absoluteDeadline - Time.now()) * 1000);
                if (timeLeftMS <= 0) {
                  LOG.debug("{}: out of time while acquiring lock, deleting {}",
                      this, mCreatedPath);
                  mZKClient.delete().forPath(mCreatedPath.getPath());
                  mCreatedPath = null;
                  return false;
                }
                this.wait(timeLeftMS);
              } else {
                this.wait();
              }
            }
          }
        }
      } catch (InterruptedException ie) {
        LOG.debug("Thread interrupted while locking.");
        Thread.currentThread().interrupt();
        return false;
      } catch (Exception e) {
        ZooKeeperUtils.wrapAndRethrow(e);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void unlock() throws IOException {
    synchronized (this) {
      DebugResourceTracker.get().unregisterResource(mCreatedPath);
      unlockInternal();
    }
  }

  /**
   * Releases the lock. Caller must synchronize on {@link this}.
   *
   * @throws IOException on unrecoverable ZooKeeper error.
   */
  @GuardedBy("this")
  private void unlockInternal() throws IOException {
    File pathToDelete;
    Preconditions.checkState(null != mCreatedPath,
        "unlock() cannot be called while lock is unlocked.");
    pathToDelete = mCreatedPath;
    LOG.debug("Releasing lock {}: deleting {}", this, mCreatedPath);
    mCreatedPath = null;
    try {
      mZKClient.delete().forPath(pathToDelete.getPath());
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass()).add("directory", mLockDir).toString();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    synchronized (this) {
      if (mCreatedPath != null) {
        // This will trigger a leaked acquired lock resource error in the cleanup log by not
        // unregistering mCreatedPath with the DebugResourceTracker.
        unlockInternal();
      }
      DebugResourceTracker.get().unregisterResource(this);
    }
  }
}
