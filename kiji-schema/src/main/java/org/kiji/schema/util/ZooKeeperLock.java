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

package org.kiji.schema.util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.base.Preconditions;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.RuntimeInterruptedException;
import org.kiji.schema.layout.impl.ZooKeeperClient;

/**
 * Distributed lock on top of ZooKeeper.
 *
 * @deprecated use a {@link org.kiji.schema.zookeeper.ZooKeeperLock} instead.
 *    Will be removed in KijiSchema 2.0.
 */
@ApiAudience.Private
@Deprecated
public final class ZooKeeperLock implements Lock, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperLock.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger("cleanup." + ZooKeeperLock.class.getName());
  private static final byte[] EMPTY = new byte[0];
  private static final String LOCK_NAME_PREFIX = "lock-";

  private final String mConstructorStack;
  private final ZooKeeperClient mZKClient;
  private final File mLockDir;
  private final File mLockPathPrefix;
  private File mCreatedPath = null;
  private WatchedEvent mPrecedingEvent = null;

  /**
   * Constructs a ZooKeeper lock object.
   *
   * @param zookeeper ZooKeeper client.
   * @param lockDir Path of the directory node to use for the lock.
   */
  public ZooKeeperLock(ZooKeeperClient zookeeper, File lockDir) {
    this.mConstructorStack = CLEANUP_LOG.isDebugEnabled() ? Debug.getStackTrace() : null;

    this.mZKClient = zookeeper;
    this.mLockDir = lockDir;
    this.mLockPathPrefix = new File(lockDir,  LOCK_NAME_PREFIX);
    // ZooKeeperClient.retain() should be the last line of the constructor.
    this.mZKClient.retain();
    DebugResourceTracker.get().registerResource(this);
  }

  /** Watches the lock directory node. */
  private class LockWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      LOG.debug("ZooKeeperLock.LockWatcher: received event {}", event);
      synchronized (ZooKeeperLock.this) {
        mPrecedingEvent = event;
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
    try {
      return lockInternal(timeout);
    } catch (KeeperException ke) {
      throw new IOException(ke);
    }
  }

  /**
   * Creates a ZooKeeper lock node.
   *
   * @param path ZooKeeper lock node path prefix.
   * @return the created ZooKeeper lock node path.
   * @throws KeeperException on error.
   */
  private File createZKLockNode(File path) throws KeeperException {
    boolean parentCreated = false;
    while (true) {
      try {
        return mZKClient.create(path, EMPTY, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
      } catch (NoNodeException nne) {
        LOG.debug("Cannot create ZooKeeper lock node {}: parent node does not exist.", path);
        Preconditions.checkState(!parentCreated);  // creates the parent node at most once.
        mZKClient.createNodeRecursively(path.getParentFile());
        parentCreated = true;
      }
    }
  }

  /**
   * Acquires the lock.
   *
   * @param timeout Deadline, in seconds, to acquire the lock. 0 means no timeout.
   * @return whether the lock is acquired (ie. false means timeout).
   * @throws KeeperException on error.
   */
  private boolean lockInternal(double timeout) throws KeeperException {
    /** Absolute time deadline, in seconds since Epoch */
    final double absoluteDeadline = (timeout > 0.0) ? Time.now() + timeout : 0.0;

    File createdPath = null;
    synchronized (this) {
      Preconditions.checkState(null == mCreatedPath, mCreatedPath);
      // Queues for access to the lock:
      createdPath = createZKLockNode(mLockPathPrefix);
      mCreatedPath = createdPath;
    }
    LOG.debug("{}: queuing for lock with node {}", this, createdPath);

    while (true) {
      try {
        // Do we own the lock already, or do we have to wait?
        final Set<String> childrenSet =
            new TreeSet<String>(mZKClient.getChildren(mLockDir, null, null));
        final String[] children = childrenSet.toArray(new String[childrenSet.size()]);
        LOG.debug("{}: lock queue: {}", this, childrenSet);

        final int index = Arrays.binarySearch(children, createdPath.getName());
        if (index == 0) {
          // We own the lock:
          LOG.debug("{}: lock acquired", this);
          return true;
        } else { // index >= 1
          synchronized (this) {
            final File preceding = new File(mLockDir, children[index - 1]);
            LOG.debug("{}: waiting for preceding node {} to disappear", this, preceding);
            if (mZKClient.exists(preceding, mLockWatcher) != null) {
              if (absoluteDeadline > 0.0) {
                final long timeLeftMS = (long) ((absoluteDeadline - Time.now()) * 1000);
                if (timeLeftMS <= 0) {
                  LOG.debug("{}: out of time while acquiring lock, deleting {}",
                      this, mCreatedPath);
                  mZKClient.delete(mCreatedPath, -1); // -1 means any version
                  mCreatedPath = null;
                  return false;
                }
                this.wait(timeLeftMS);
              } else {
                this.wait();
              }
              if ((mPrecedingEvent != null)
                  && (mPrecedingEvent.getType() == EventType.NodeDeleted)
                  && (index == 1)) {
                LOG.debug("{}: lock acquired after {} disappeared", this, children[index - 1]);
              }
            }
          }
        }
      } catch (InterruptedException ie) {
        throw new RuntimeInterruptedException(ie);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void unlock() throws IOException {
    try {
      unlockInternal();
      return;
    } catch (InterruptedException ie) {
      throw new RuntimeInterruptedException(ie);
    } catch (KeeperException ke) {
      throw new IOException(ke);
    }
  }

  /**
   * Releases the lock.
   *
   * @throws InterruptedException if the thread is interrupted.
   * @throws KeeperException on error.
   */
  private void unlockInternal() throws InterruptedException, KeeperException {
    File pathToDelete = null;
    synchronized (this) {
      Preconditions.checkState(null != mCreatedPath,
          "unlock() cannot be called while lock is unlocked.");
      pathToDelete = mCreatedPath;
      LOG.debug("Releasing lock {}: deleting {}", this, mCreatedPath);
      mCreatedPath = null;
    }
    mZKClient.delete(pathToDelete, -1); // -1 means any version
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    final boolean isLocked;
    synchronized (this) {
      isLocked = mCreatedPath != null;
    }
    if (isLocked) {
      CLEANUP_LOG.warn("Releasing ZooKeeperLock with path: {} in close().  "
          + "Please ensure that locks are unlocked before closing.", mCreatedPath);
      if (CLEANUP_LOG.isDebugEnabled()) {
        CLEANUP_LOG.debug("ZooKeeperLock with path '{}' was constructed through:\n{}",
            mCreatedPath, mConstructorStack);
      }
      unlock();
    }
    this.mZKClient.release();
    DebugResourceTracker.get().unregisterResource(this);
  }
}
