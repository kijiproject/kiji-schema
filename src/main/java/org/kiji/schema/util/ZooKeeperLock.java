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
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distributed lock on top of ZooKeeper.
 */
public class ZooKeeperLock implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperLock.class);
  private static final byte[] EMPTY = new byte[0];

  private final ZooKeeper mZooKeeper;
  private final File mLockDir;
  private final File mLockPathPrefix;
  private File mCreatedPath = null;
  private WatchedEvent mPrecedingEvent = null;

  /**
   * Constructs a ZooKeeper lock object.
   *
   * @param zookeeper
   *          ZooKeeper client.
   * @param lockDir
   *          Path of the directory node to use for the lock.
   */
  public ZooKeeperLock(ZooKeeper zookeeper, File lockDir) {
    this.mZooKeeper = zookeeper;
    this.mLockDir = lockDir;
    this.mLockPathPrefix = new File(lockDir, "lock-");
  }

  /** Watches the lock directory node. */
  private class LockWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      LOG.debug(String.format("%s: received event", this));
      synchronized (ZooKeeperLock.this) {
        mPrecedingEvent = event;
        ZooKeeperLock.this.notifyAll();
      }
    }
  }

  private final LockWatcher mLockWatcher = new LockWatcher();

  /**
   * Unconditionally acquires the lock.
   *
   * @throws IOException on I/O error.
   */
  public void lock() throws IOException {
    if (!lock(0.0)) {
      // This should never happen, instead lock(0.0) should raise a KeeperException!
      throw new RuntimeException("Unable to acquire ZooKeeper lock.");
    }
  }

  /**
   * Acquires the lock.
   *
   * @param timeout
   *          Deadline, in seconds, to acquire the lock. 0 means no timeout.
   * @return whether the lock is acquired (ie. false means timeout).
   * @throws IOException on I/O error.
   */
  public boolean lock(double timeout) throws IOException {
    while (true) {
      try {
        return lockInternal(timeout);
      } catch (InterruptedException ie) {
        LOG.info(ie.toString());
      } catch (KeeperException ke) {
        throw new IOException(ke);
      }
    }
  }

  /**
   * Creates a ZooKeeper node and all its parents, if necessary.
   *
   * @param zkClient ZooKeeper client.
   * @param path ZooKeeper node path.
   * @throws KeeperException on I/O error.
   */
  private static void createZKNodeRecursively(ZooKeeper zkClient, File path)
      throws KeeperException {
    if (path.getPath().equals("/")) {
      return;
    }
    final File parent = path.getParentFile();
    if (parent != null) {
      createZKNodeRecursively(zkClient, parent);
    }
    while (true) {
      try {
        LOG.debug(String.format("Creating ZooKeeper node %s", path));
        final File created = new File(zkClient.create(
            path.toString(), EMPTY, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        Preconditions.checkState(created.equals(path));
        return;
      } catch (NodeExistsException exn) {
        // Node already exists, ignore!
        return;
      } catch (InterruptedException ie) {
        LOG.error(ie.toString());
      }
    }
  }

  /**
   * Creates a ZooKeeper lock node.
   *
   * @param zkClient ZooKeeper client.
   * @param path ZooKeeper lock node path prefix.
   * @return the created ZooKeeper lock node path.
   * @throws KeeperException on error.
   */
  private static File createZKLockNode(ZooKeeper zkClient, File path) throws KeeperException {
    boolean parentCreated = false;
    while (true) {
      try {
        return new File(zkClient.create(
            path.toString(), EMPTY, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL));
      } catch (NoNodeException nne) {
        LOG.debug(nne.toString());
        Preconditions.checkState(!parentCreated);  // creates the parent node at most once.
        createZKNodeRecursively(zkClient, path.getParentFile());
        parentCreated = true;
      } catch (InterruptedException ie) {
        LOG.debug(ie.toString());
        // Retry
      }
    }
  }

  /**
   * Acquires the lock.
   *
   * @param timeout
   *          Deadline, in seconds, to acquire the lock. 0 means no timeout.
   * @return whether the lock is acquired (ie. false means timeout).
   * @throws InterruptedException on interruption.
   * @throws KeeperException on error.
   */
  private boolean lockInternal(double timeout) throws InterruptedException, KeeperException {
    /** Absolute time deadline, in seconds since Epoch */
    final double absoluteDeadline = (timeout > 0.0)
        ? (System.currentTimeMillis() / 1000.0) + timeout
        : 0.0;

    File createdPath = null;
    synchronized (this) {
      Preconditions.checkState(null == mCreatedPath, mCreatedPath);
      // Queues for access to the lock:
      createdPath = createZKLockNode(mZooKeeper, mLockPathPrefix);
      mCreatedPath = createdPath;
    }
    LOG.debug(String.format("%s: queuing for lock with node %s", this, createdPath));

    while (true) {
      try {
        // Do we own the lock already, or do we have to wait?
        final Set<String> childrenSet =
            new TreeSet<String>(mZooKeeper.getChildren(mLockDir.toString(), false));
        final String[] children = childrenSet.toArray(new String[childrenSet.size()]);
        LOG.debug(String.format("%s: lock queue: %s", this, childrenSet));

        final int index = Arrays.binarySearch(children, createdPath.getName());
        if (index == 0) {
          // We own the lock:
          LOG.debug(String.format("%s: lock acquired", this));
          return true;
        } else { // index >= 1
          synchronized (this) {
            final File preceding = new File(mLockDir, children[index - 1]);
            LOG.debug(String.format("%s: waiting for preceding node %s to disappear",
                this, preceding));
            if (mZooKeeper.exists(preceding.toString(), mLockWatcher) != null) {
              if (absoluteDeadline > 0.0) {
                final double timeLeft = absoluteDeadline - (System.currentTimeMillis() / 1000.0);
                if (timeLeft <= 0) {
                  LOG.debug(String.format("%s: out of time while acquiring lock, deleting %s",
                      this, mCreatedPath));
                  mZooKeeper.delete(mCreatedPath.toString(), -1); // -1 means any version
                  mCreatedPath = null;
                  return false;
                }
                this.wait((long)(timeLeft * 1000.0));
              } else {
                this.wait();
              }
              if ((mPrecedingEvent != null)
                  && (mPrecedingEvent.getType() == EventType.NodeDeleted)
                  && (index == 1)) {
                LOG.debug(String.format("%s: lock acquired after %s disappeared",
                    this, children[index - 1]));
              }
            }
          }
        }
      } catch (InterruptedException ie) {
        LOG.info(String.format("%s: interrupted: %s", this, ie));
      }
    }
  }

  /**
   * Releases the lock.
   *
   * @throws IOException on I/O error.
   */
  public void unlock() throws IOException {
    while (true) {
      try {
        unlockInternal();
        return;
      } catch (InterruptedException ie) {
        LOG.info(ie.toString());
      } catch (KeeperException ke) {
        throw new IOException(ke);
      }
    }
  }

  /**
     * Releases the lock.
     *
     * @throws InterruptedException if the thread is interrupted.
     * @throws KeeperException on error.
     */
  public void unlockInternal() throws InterruptedException, KeeperException {
    File pathToDelete = null;
    synchronized (this) {
      Preconditions.checkState(null != mCreatedPath, mCreatedPath);
      pathToDelete = mCreatedPath;
      LOG.debug(String.format("Releasing lock: deleting %s", mCreatedPath));
      mCreatedPath = null;
    }
    mZooKeeper.delete(pathToDelete.toString(), -1); // -1 means any version
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    while (true) {
      try {
        this.mZooKeeper.close();
        return;
      } catch (InterruptedException ie) {
        LOG.error(ie.toString());
      }
    }
  }
}
