/**
 * (c) Copyright 2013 WibiData, Inc.
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

package org.kiji.schema.layout.impl;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.KeeperException.SessionMovedException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.RuntimeInterruptedException;
import org.kiji.schema.util.Lock;
import org.kiji.schema.util.LockFactory;
import org.kiji.schema.util.ReferenceCountable;
import org.kiji.schema.util.Time;
import org.kiji.schema.util.ZooKeeperLock;

/**
 * ZooKeeper client interface.
 *
 * @deprecated use a {@link org.apache.curator.framework.CuratorFramework} instead.
 *    Will be removed in KijiSchema 2.0.
 */
@ApiAudience.Private
@Deprecated
public final class ZooKeeperClient implements ReferenceCountable<ZooKeeperClient> {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperClient.class);

  /** Empty byte array used to create ZooKeeper "directory" nodes. */
  private static final byte[] EMPTY_BYTES = new byte[0];

  /** Time interval, in seconds, between ZooKeeper retries on error. */
  private static final double ZOOKEEPER_RETRY_DELAY = 1.0;

  /** Timeout for ZooKeeper sessions, in milliseconds. */
  private static final int SESSION_TIMEOUT = 60 * 1000; // 60 seconds

  /**
   * Cache of existing ZooKeeperClient instances keyed on quorum addresss.
   * Protected by <code>CACHE_MONITOR</code>.
   */
  private static final Map<String, ZooKeeperClient> ZOOKEEPER_CACHE = Maps.newHashMap();

  /**
   * Protects access to <code>ZOOKEEPER_CACHE</code>. Must always be aquired before
   * <code>mMonitor</code>.
   */
  private static final Object CACHE_MONITOR = new Object();

  /**
   * Factory method for retrieving an instance of ZooKeeperClient connected to the provided quorum
   * address.  Will reuse existing connections if possible.  The caller is responsible for
   * releasing the ZooKeeperClient.
   *
   * @param address of ZooKeeper quorum.
   * @return a ZooKeeperClient.
   */
  public static ZooKeeperClient getZooKeeperClient(String address) {
    synchronized (CACHE_MONITOR) {
      final ZooKeeperClient existingClient = ZOOKEEPER_CACHE.get(address);
      if (existingClient == null) {
        final ZooKeeperClient newClient = new ZooKeeperClient(address).open();
        ZOOKEEPER_CACHE.put(address, newClient);
        return newClient.retain();
      } else {
        return existingClient.retain();
      }
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Watcher for the ZooKeeper session.
   *
   * <p> Processes notifications related to the liveness of the ZooKeeper session. </p>
   */
  private class SessionWatcher implements Watcher {
    /** {@inheritDoc} */
    @Override
    public void process(WatchedEvent event) {
      LOG.debug("ZooKeeper client: process({})", event);

      if (event.getType() != Event.EventType.None) {
        LOG.error("Unexpected event: {}", event);
        return;
      }
      if (event.getPath() != null) {
        LOG.error("Unexpected event: {}", event);
        return;
      }

      switch (event.getState()) {
      case SyncConnected: {
        synchronized (mMonitor) {
          if (mState != State.OPEN) {
            LOG.debug("ZooKeeper client session alive notification received while in state {}.",
                mState);
          } else {
            LOG.debug("ZooKeeper client session {} alive.", mZKClient.getSessionId());
          }
          mMonitor.notifyAll();
        }
        break;
      }
      case AuthFailed:
      case ConnectedReadOnly:
      case SaslAuthenticated: {
        LOG.error("Error establishing ZooKeeper client session: {}", event);
        // Nothing to do here.
        break;
      }
      case Disconnected: {
        LOG.warn("ZooKeeper client disconnected.");
        break;
      }
      case Expired: {
        synchronized (mMonitor) {
          LOG.warn("ZooKeeper client session {} expired.", mZKClient.getSessionId());
          if (mState == State.OPEN) {
            createZKClient();
          } else {
            LOG.debug("ZooKeeperClient in state {}; not reopening ZooKeeper session.", mState);
          }
        }
        break;
      }
      default: {
        throw new RuntimeException("Unexpected ZooKeeper event state: " + event);
      }
      }
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Address of the ZooKeeper quorum to interact with. */
  private final String mZKAddress;

  /** LockFactory which uses this ZooKeeperClient as a connection to ZooKeeper. */
  private final LockFactory mLockFactory = new LockFactory() {
    @Override
    public Lock create(String name) throws IOException {
      return new ZooKeeperLock(ZooKeeperClient.this, new File(name));
    }
  };

  /** States of a ZooKeeper client instance. */
  private static enum State {
    INITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this ZooKeeper client. Access protected by <code>mMonitor</code>. */
  private State mState = State.INITIALIZED;

  /**
   * Represents the number of handles to this ZooKeeperClient. Access protected by
   * <code>mMonitor</code>.
   */
  private int mRetainCount = 1;

  /**
   * Current ZooKeeper session client.  Access protected by <code>mMonitor</code>.
   *
   * <ul>
   *   <li> Null before the client is opened and after the client is closed. </li>
   *   <li> Set when the client is opened, reset when the client dies. </li>
   * </ul>
   *
   * <p> Even when this ZooKeeper client session is non null, it might not be established yet or
   *   may be dead.
   * </p>
   */
  private ZooKeeper mZKClient = null;

  /**
   * Protects access to <code>mState</code>, <code>mRetainCount</code>, and <code>mZKClient</code>.
   * If locking both <code>mMonitor</code> and <code>CACHE_MONITOR</code>,
   * <code>CACHE_MONITOR</code> must be aquired first.
   */
  private final Object mMonitor = new Object();

  // -----------------------------------------------------------------------------------------------

  /**
   * Initializes a ZooKeeper client. The new ZooKeeperClient is returned with a retain count of one.
   *
   * @param zkAddress Address of the ZooKeeper quorum, as a comma-separated list of "host:port".
   */
  private ZooKeeperClient(String zkAddress) {
    this.mZKAddress = zkAddress;
    LOG.debug("Created {}", this);
  }

  /**
   * Starts the ZooKeeper client.
   *
   * @return this
   */
  private ZooKeeperClient open() {
    synchronized (mMonitor) {
      Preconditions.checkState(mState == State.INITIALIZED,
          "Cannot open ZooKeeperClient in state %s.", mState);
      mState = State.OPEN;
      Preconditions.checkState(mZKClient == null);
      createZKClient();
    }
    return this;
  }

  /**
   * Returns whether this ZooKeeperClient has been opened and not closed.
   *
   * @return whether this ZooKeeperClient has been opened and not closed.
   */
  public boolean isOpen() {
    synchronized (mMonitor) {
      return mState == State.OPEN;
    }
  }

  /**
   * Factory for ZooKeeper session clients.
   *
   * <p> This returns immediately, but the ZooKeeper session is established asynchronously. </p>
   *
   * @throws KijiIOException on I/O error.
   */
  private void createZKClient() {
    LOG.debug("Creating new ZooKeeper client for address {} in {}.", mZKAddress, this);
    try {
      synchronized (mMonitor) {
        mZKClient = new ZooKeeper(mZKAddress, SESSION_TIMEOUT, new SessionWatcher());
      }
    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }
  }

  /**
   * Reports the ZooKeeper session client.
   *
   * @param timeout If no ZooKeeper session may be established within the specified timeout,
   *     in seconds, this fails and returns null.
   *     0 or negative means no timeout, ie. potentially wait forever.
   * @return A live ZooKeeper session client, or null.
   */
  public ZooKeeper getZKClient(double timeout) {

    // Absolute deadline, in seconds since the Epoch:
    final double absoluteDeadline = (timeout > 0.0) ? (Time.now() + timeout) : 0.0;

    synchronized (mMonitor) {
      while (absoluteDeadline == 0.0 || absoluteDeadline > Time.now()) {
        Preconditions.checkState(mState == State.OPEN,
            "Cannot get ZKClient %s in state %s.", this, mState);
        if ((mZKClient != null) && mZKClient.getState().isConnected()) {
          return mZKClient;
        } else {
          try {
            if (absoluteDeadline > 0) {
              final double waitTimeout = absoluteDeadline - Time.now();  // seconds
              mMonitor.wait((long) (waitTimeout * 1000.0));
            } else {
              mMonitor.wait();
            }
          } catch (InterruptedException ie) {
            throw new RuntimeInterruptedException(ie);
          }
        }
      }
    }
    return null;
  }

  /**
   * Reports the ZooKeeper session client.
   *
   * <p> This may potentially wait forever: no timeout. </p>
   *
   * @return the ZooKeeper session client.
   */
  public ZooKeeper getZKClient() {
    return getZKClient(0.0);
  }

  /** {@inheritDoc} */
  @Override
  public ZooKeeperClient retain() {
    LOG.debug("Retaining {}", this);
    synchronized (mMonitor) {
      Preconditions.checkState(mState == State.OPEN,
          "Cannot retain ZooKeeperClient %s in state %s.", this, mState);
      mRetainCount += 1;
    }
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public void release() throws IOException {
    LOG.debug("Releasing {}", this);
    synchronized (CACHE_MONITOR) {
      synchronized (mMonitor) {
        Preconditions.checkState(mState == State.OPEN,
            "Cannot release ZooKeeperClient %s in state %s.", this, mState);
        mRetainCount -= 1;
        if (mRetainCount == 0) {
          close();
        }
      }
    }
  }

  /**
   * Closes this ZooKeeper client.  Should only be called by {@link #release()} or
   * {@link #finalize()}.
   */
  private void close() {
    LOG.debug("Closing {}", this);
    synchronized (CACHE_MONITOR) {
      synchronized (mMonitor) {
        Preconditions.checkState(mState == State.OPEN,
            "Cannot close ZooKeeperClient %s in state %s.", this, mState);
        mState = State.CLOSED;
        ZOOKEEPER_CACHE.remove(mZKAddress);
        if (mZKClient == null) {
          // Nothing to close:
          return;
        }
        try {
          mZKClient.close();
          mZKClient = null;
        } catch (InterruptedException ie) {
          throw new RuntimeInterruptedException(ie);
        }
      }
    }
  }

  /**
   * See {@link ZooKeeper#create(String, byte[], List, CreateMode)}.
   *
   * @param path to desired node.
   * @param data the desired node should contain by default.
   * @param acl is the Access Control List the desired node will use.
   * @param createMode specifies whether the node to be created is ephemeral and/or sequential.
   * @return the actual path of the created node.
   * @throws KeeperException on ZooKeeper errors.
   *     Connection related errors are handled by retrying the operations.
   */
  public File create(
      File path,
      byte[] data,
      List<ACL> acl,
      CreateMode createMode)
      throws KeeperException {

    while (true) {
      try {
        return new File(getZKClient().create(path.toString(), data, acl, createMode));
      } catch (InterruptedException ie) {
        throw new RuntimeInterruptedException(ie);
      } catch (ConnectionLossException ke) {
        LOG.debug("ZooKeeper connection lost.", ke);
      } catch (SessionExpiredException see) {
        LOG.debug("ZooKeeper session expired.", see);
      } catch (SessionMovedException sme) {
        LOG.debug("ZooKeeper session moved.", sme);
      }
      Time.sleep(ZOOKEEPER_RETRY_DELAY);
      LOG.debug("Retrying create({}, {}, {}, {}).",
          path, Bytes.toStringBinary(data), acl, createMode);
    }
  }

  /**
   * See {@link ZooKeeper#exists(String, boolean)}.
   *
   * @param path of a node.
   * @return the stat of the node; null if the node does not exist.
   * @throws KeeperException on ZooKeeper errors.
   *     Connection related errors are handled by retrying the operations.
   */
  public Stat exists(File path) throws KeeperException {
    while (true) {
      try {
        return getZKClient().exists(path.toString(), false);
      } catch (InterruptedException ie) {
        throw new RuntimeInterruptedException(ie);
      } catch (ConnectionLossException ke) {
        LOG.debug("ZooKeeper connection lost.", ke);
      } catch (SessionExpiredException see) {
        LOG.debug("ZooKeeper session expired.", see);
      } catch (SessionMovedException sme) {
        LOG.debug("ZooKeeper session moved.", sme);
      }
      Time.sleep(ZOOKEEPER_RETRY_DELAY);
      LOG.debug("Retrying exists({}).", path);
    }
  }

  /**
   * See {@link ZooKeeper#exists(String, Watcher)}.
   *
   * @param path of a node.
   * @param watcher triggered by a successful operation that sets data on the node or
   *     creates/deletes the node.
   * @return the stat of the node.
   * @throws KeeperException on ZooKeeper errors.
   *     Connection related errors are handled by retrying the operations.
   */
  public Stat exists(File path, Watcher watcher) throws KeeperException {
    while (true) {
      try {
        return getZKClient().exists(path.toString(), watcher);
      } catch (InterruptedException ie) {
        throw new RuntimeInterruptedException(ie);
      } catch (ConnectionLossException ke) {
        LOG.debug("ZooKeeper connection lost.", ke);
      } catch (SessionExpiredException see) {
        LOG.debug("ZooKeeper session expired.", see);
      } catch (SessionMovedException sme) {
        LOG.debug("ZooKeeper session moved.", sme);
      }
      Time.sleep(ZOOKEEPER_RETRY_DELAY);
      LOG.debug("Retrying exists({}).", path);
    }
  }

  /**
   * See {@link ZooKeeper#getData(String, Watcher, Stat)}.
   *
   * @param path of a node.
   * @param watcher triggered by a successful operation that sets data on the node or
   *     deletes the node.
   * @param stat of the node.
   * @return the data contained within the node.
   * @throws KeeperException on ZooKeeper errors.
   *     Connection related errors are handled by retrying the operations.
   */
  public byte[] getData(File path, Watcher watcher, Stat stat)
      throws KeeperException {
    while (true) {
      try {
        return getZKClient().getData(path.toString(), watcher, stat);
      } catch (InterruptedException ie) {
        throw new RuntimeInterruptedException(ie);
      } catch (ConnectionLossException ke) {
        LOG.debug("ZooKeeper connection lost.", ke);
      } catch (SessionExpiredException see) {
        LOG.debug("ZooKeeper session expired.", see);
      } catch (SessionMovedException sme) {
        LOG.debug("ZooKeeper session moved.", sme);
      }
      Time.sleep(ZOOKEEPER_RETRY_DELAY);
      LOG.debug("Retrying getData({}).", path);
    }
  }

  /**
   * See {@link ZooKeeper#setData(String, byte[], int)}.
   *
   * @param path of a node.
   * @param data to set.
   * @param version of the node.
   * @return the stat of the node.
   * @throws KeeperException on ZooKeeper errors.
   *     Connection related errors are handled by retrying the operations.
   */
  public Stat setData(File path, byte[] data, int version) throws KeeperException {
    while (true) {
      try {
        return getZKClient().setData(path.toString(), data, version);
      } catch (InterruptedException ie) {
        throw new RuntimeInterruptedException(ie);
      } catch (ConnectionLossException ke) {
        LOG.debug("ZooKeeper connection lost.", ke);
        LOG.debug("Retrying setData({}, {}, {}).", path, Bytes.toStringBinary(data), version);
      } catch (SessionExpiredException see) {
        LOG.debug("ZooKeeper session expired.", see);
      } catch (SessionMovedException sme) {
        LOG.debug("ZooKeeper session moved.", sme);
      }
      Time.sleep(ZOOKEEPER_RETRY_DELAY);
    }
  }

  /**
   * See {@link ZooKeeper#getChildren(String, Watcher, Stat)}.
   *
   * @param path of a parent node.
   * @param watcher to trigger upon parent node deletion or creation/deletion of a child node.
   * @param stat of the provided parent node.
   * @return an unordered array of the parent node's children.
   * @throws KeeperException on ZooKeeper errors.
   *     Connection related errors are handled by retrying the operations.
   */
  public List<String> getChildren(File path, Watcher watcher, Stat stat)
      throws KeeperException {
    while (true) {
      try {
        return getZKClient().getChildren(path.toString(), watcher, stat);
      } catch (InterruptedException ie) {
        throw new RuntimeInterruptedException(ie);
      } catch (ConnectionLossException ke) {
        LOG.debug("ZooKeeper connection lost.", ke);
      } catch (SessionExpiredException see) {
        LOG.debug("ZooKeeper session expired.", see);
      } catch (SessionMovedException sme) {
        LOG.debug("ZooKeeper session moved.", sme);
      }
      Time.sleep(ZOOKEEPER_RETRY_DELAY);
      LOG.debug("Retrying getChildren({}).", path);
    }
  }

  /**
   * See {@link ZooKeeper#delete(String, int)}.
   *
   * @param path of the node to delete.
   * @param version of the node to delete.
   * @throws KeeperException on ZooKeeper errors.
   *     Connection related errors are handled by retrying the operations.
   */
  public void delete(File path, int version) throws KeeperException {
    while (true) {
      try {
        getZKClient().delete(path.toString(), version);
        return;
      } catch (InterruptedException ie) {
        throw new RuntimeInterruptedException(ie);
      } catch (ConnectionLossException ke) {
        LOG.debug("ZooKeeper connection lost.", ke);
      } catch (SessionExpiredException see) {
        LOG.debug("ZooKeeper session expired.", see);
      } catch (SessionMovedException sme) {
        LOG.debug("ZooKeeper session moved.", sme);
      }
      Time.sleep(ZOOKEEPER_RETRY_DELAY);
      LOG.debug("Retrying delete({}, {}).", path, version);
    }
  }

  /**
   * Creates a ZooKeeper node and all its parents, if necessary.
   *
   * @param path of the node to create.
   * @throws KeeperException on I/O error.
   */
  public void createNodeRecursively(File path)
      throws KeeperException {

    if (exists(path) != null) {
      return;
    }

    if (path.getPath().equals("/")) {
      // No need to create the root node "/" :
      return;
    }
    final File parent = path.getParentFile();
    if (parent != null) {
      createNodeRecursively(parent);
    }
    try {
      LOG.debug("Creating ZooKeeper node: {}", path);
      final File createdPath =
          this.create(path, EMPTY_BYTES, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      Preconditions.checkState(createdPath.equals(path));
    } catch (NodeExistsException exn) {
      LOG.debug("ZooKeeper node already exists: {}", path);
    }
  }

  /**
   * Return a <code>LockFactory</code> implementation which utilizes this ZooKeeperClient.
   *
   * @return a LockFactory which uses this ZooKeeperClient.
   */
  public LockFactory getLockFactory() {
    return mLockFactory;
  }

  /**
   * Removes a ZooKeeper node and all of its children, if necessary.
   *
   * @param path of the node to remove.
   * @throws KeeperException on I/O error.
   */
  public void deleteNodeRecursively(File path) throws KeeperException {
    Stat stat = exists(path);  // Race condition if someone else updates the znode in the meantime

    if (stat == null) {
      return;
    }

    List<String> children = getChildren(path, null, null);
    for (String child : children) {
      deleteNodeRecursively(new File(path, child));
    }
    delete(path, stat.getVersion());
  }

  /**
   * Returns a string representation of this object.
   * @return A string representation of this object.
   */
  public String toString() {
    synchronized (mMonitor) {
      return Objects.toStringHelper(getClass())
          .add("this", System.identityHashCode(this))
          .add("zk_address", mZKAddress)
          .add("retain_count", mRetainCount)
          .toString();
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    synchronized (CACHE_MONITOR) {
      synchronized (mMonitor) {
        if (mState != State.CLOSED) {
          LOG.warn("Finalizing unreleased ZooKeeperClient {}.", this);
          close();
        }
      }
    }
    super.finalize();
  }
}
