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

package org.kiji.schema;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.impl.DefaultKijiTableFactory;
import org.kiji.schema.util.Clock;
import org.kiji.schema.util.DefaultClock;

/**
 * Maintains a pool of opened KijiTables.
 *
 * <p>Instead of creating a new KijiTable instance when needed, clients may use a
 * KijiTablePool to keep a pool of opened tables for reuse. When a client asks for a
 * KijiTable, the pool first checks the cache for an already opened and available
 * table. If available, the cached table will be returned. Otherwise, a new one will be
 * opened and returned. When the client is finished, it should call release() to allow
 * other clients or threads the option to reuse the opened table.</p>
 *
 * <p>This class is thread-safe.</p>
 */
public class KijiTablePool implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(KijiTablePool.class);

  /** A factory for creating new opened HTables. */
  private final KijiTableFactory mTableFactory;

  /** A clock. */
  private final Clock mClock;

  /** The maximum number of connections to keep per table. */
  private final int mMaxSize;

  /** Milliseconds before an idle table will be eligible for cleanup. */
  private final long mIdleTimeout;

  /** Number of milliseconds to wait between sweeps for idle tables. */
  private final long mIdlePollPeriod;

  /** A map from table names to their connection pools. */
  private final Map<String, Pool> mTableCache;

  /** A cleanup thread for idle connections. */
  private IdleTimeoutThread mCleanupThread;

  /** Whether the table pool is open. */
  private boolean mIsOpen;

  /**
   * Describes the options that can be configured on the KijiTablePool.
   */
  public static class Options {
    private int mMaxSize;
    private long mIdleTimeout;
    private long mIdlePollPeriod;
    private Clock mClock;

    /**
     * Creates options with default values.
     */
    public Options() {
      mMaxSize = 0;
      mIdleTimeout = 0L;
      mIdlePollPeriod = 10000L; // 10 seconds.
      mClock = new DefaultClock();
    }

    /**
     * Sets the maximum number of connections to keep per table.
     *
     * <p>Use zero(0) to indicate that the pool should be unbounded.</p>
     *
     * @param maxSize The max number of connections to keep per table.
     * @return This options object for method chaining.
     */
    public Options withMaxSize(int maxSize) {
      mMaxSize = maxSize;
      return this;
    }

    /**
     * Gets the maximum number of connections to keep per table.
     *
     * @return The max number of connections to keep per table.
     */
    public int getMaxSize() {
      return mMaxSize;
    }

    /**
     * Sets the amount of time a connection may be idle before being removed from the pool.
     *
     * <p>Use zero (0) to indicate that connections should never be removed.</p>
     *
     * @param timeoutMillis Timeout in milliseconds.
     * @return This options object for method chaining.
     */
    public Options withIdleTimeout(long timeoutMillis) {
      mIdleTimeout = timeoutMillis;
      return this;
    }

    /**
     * Gets the amount of time a connection may be idle before being removed from the pool.
     *
     * @return The timeout in milliseconds.
     */
    public long getIdleTimeout() {
      return mIdleTimeout;
    }

    /**
     * Sets the amount of time between sweeps of the pool for removing idle connections.
     *
     * @param periodMillis Number of milliseconds between sweeps.
     * @return This options object for method chaining.
     */
    public Options withIdlePollPeriod(long periodMillis) {
      mIdlePollPeriod = periodMillis;
      return this;
    }

    /**
     * Gets the amount of time between sweeps of the pool for removing idle connections.
     *
     * @return Number of milliseconds between sweeps.
     */
    public long getIdlePollPeriod() {
      return mIdlePollPeriod;
    }

    /**
     * Sets a clock.
     *
     * @param clock A clock.
     * @return This options object for method chaining.
     */
    public Options withClock(Clock clock) {
      mClock = clock;
      return this;
    }

    /**
     * Gets a clock.
     *
     * @return A clock.
     */
    public Clock getClock() {
      return mClock;
    }
  }

  /**
   * Constructs a new pool of Kiji tables.
   *
   * @param kiji The kiji instance.
   */
  public KijiTablePool(Kiji kiji) {
    this(new DefaultKijiTableFactory(kiji));
  }

  /**
   * Constructs a new pool of Kiji tables.
   *
   * @param tableFactory A KijiTable factory.
   */
  public KijiTablePool(KijiTableFactory tableFactory) {
    this(tableFactory, new Options());
  }

  /**
   * Constructs a new pool of Kiji tables.
   *
   * @param tableFactory A KijiTable factory.
   * @param options Configurable options for the pool.
   */
  public KijiTablePool(KijiTableFactory tableFactory, Options options) {
    mTableFactory = tableFactory;
    mClock = options.getClock();
    mMaxSize = (0 == options.getMaxSize()) ? Integer.MAX_VALUE : options.getMaxSize();
    mIdleTimeout = options.getIdleTimeout();
    mIdlePollPeriod = options.getIdlePollPeriod();
    mTableCache = new HashMap<String, Pool>();
    mIsOpen = true;
  }

  /**
   * Thrown when an attempt to get a table connection fails because there is no room in the pool.
   */
  public static class NoCapacityException extends IOException {
    /**
     * Creates a new <code>NoCapacityException</code> with the specified detail message.
     * @param message The exception message.
     */
    public NoCapacityException(String message) {
      super(message);
    }
  }

  /**
   * Gets a previously opened table from the pool, or open a new connection.
   * Clients should not call close() on the returned table. Instead, they should release the
   * table back to the pool when finished by passing it in call to release().
   *
   * @param name The name of the Kiji table.
   * @return An opened Kiji table.
   * @throws IOException If there is an error.
   * @throws KijiTablePool.NoCapacityException If the table pool is at capacity.
   */
  public synchronized KijiTable get(String name) throws IOException {
    LOG.debug("Retrieving a connection for " + name + " from the table pool.");
    if (!mIsOpen) {
      throw new IllegalStateException("Table pool is closed.");
    }

    if (!mTableCache.containsKey(name)) {
      mTableCache.put(name, new Pool());
    }

    return mTableCache.get(name).get(name);
  }

  /**
   * Releases a table back to the pool.
   *
   * <p>Only open tables that were retrieved from this pool should be released.</p>
   *
   * @param table The table to release to the pool. If null, will be a no-op.
   */
  public synchronized void release(KijiTable table) {
    LOG.debug("Releasing a KijiTable " + table + " back to the pool.");
    if (!mIsOpen) {
      throw new IllegalStateException("Table pool is closed.");
    }

    if (null == table) {
      return;
    }

    // TODO: Check that this table came from this pool.
    // Throw an IllegalArgumentException if not.
    //
    // Verify that the table is still open.  Throw an IllegalStateException if not.
    mTableCache.get(table.getName()).release(table);

    // Start the cleanup thread if necessary.
    if (mIdleTimeout > 0L && null == mCleanupThread) {
      mCleanupThread = new IdleTimeoutThread();
      mCleanupThread.start();
    }
  }

  /**
   * Explicitly force a cleanup of table connections that have been idle too long.
   */
  synchronized void cleanIdleConnections() {
    if (mIdleTimeout > 0) {
      for (Pool pool: mTableCache.values()) {
        pool.clean(mIdleTimeout);
      }
    }
  }

  /**
   * Closes the tables in the pool.
   *
   * @throws IOException If there is an error closing the pool.
   */
  @Override
  public synchronized void close() throws IOException {
    if (!mIsOpen) {
      LOG.warn("Called close() on a KijiTablePool that was already closed.");
      return;
    }
    if (null != mCleanupThread) {
      mCleanupThread.interrupt();
      try {
        mCleanupThread.join();
      } catch (InterruptedException e) {
        // Oh well.
      }
    }
    for (Pool pool : mTableCache.values()) {
      IOUtils.closeQuietly(pool);
    }
    mTableCache.clear();
    mIsOpen = false;
  }

  @Override
  protected void finalize() throws Throwable {
    if (mIsOpen) {
      LOG.warn("Closing KijiTablePool in finalize(). You should close it explicitly");
      close();
    }
    super.finalize();
  }

  /**
   * A pool of connections for a single table. Maintains a number of
   * connections in use, and a queue of available ones for re-use.
   */
  private class Pool implements Closeable {
    private final Queue<Connection> mConnections;
    private int mNumInUse;

    /**
     * Constructor.
     */
    public Pool() {
      mConnections = new ArrayDeque<Connection>();
      mNumInUse = 0;
    }

    /**
     * Gets a table connection from the pool.
     *
     * @param tableName The name of the table.
     * @return The table connection.
     * @throws IOException If there is an error opening the table.
     * @throws KijiTablePool.NoCapacityException If there is no more room in the
     *     pool to open a new connection.
     */
    public synchronized KijiTable get(String tableName) throws IOException {
      Connection availableConnection = mConnections.poll();
      if (null == availableConnection) {
        LOG.debug("Cache miss for table " + tableName);
        if (mNumInUse >= mMaxSize) {
          throw new NoCapacityException("Reached max pool size for table " + tableName);
        }
        mNumInUse++;
        return mTableFactory.openTable(tableName);
      }
      LOG.debug("Cache hit for table " + tableName);
      mNumInUse++;
      return availableConnection.getTable();
    }

    /**
     * Releases a table back to the pool so it may be reused.
     *
     * @param table The table to release.
     */
    public synchronized void release(KijiTable table) {
      mNumInUse--;
      mConnections.add(new Connection(table, mClock));
    }

    /**
     * Cleans any connections from the pool that have been idle.
     *
     * @param idleTimeout Milliseconds idle required to be closed and
     *     removed from the pool.
     */
    public synchronized void clean(long idleTimeout) {
      long currentTime = mClock.getTime();
      Iterator<Connection> iterator = mConnections.iterator();
      while (iterator.hasNext()) {
        Connection connection = iterator.next();
        if (currentTime - connection.getLastAccessTime() > idleTimeout) {
          LOG.info("Closing idle KijiTable connection to " + connection.getTable().getName());
          iterator.remove();
          IOUtils.closeQuietly(connection.getTable());
        }
      }
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void close() throws IOException {
      while (!mConnections.isEmpty()) {
        IOUtils.closeQuietly(mConnections.remove().getTable());
      }
    }
  }

  /**
   * A connection in the pool.
   */
  private static class Connection {
    private final KijiTable mTable;
    private long mLastAccessTime;

    /**
     * Constructor.
     *
     * @param table The table connection.
     * @param clock A clock.
     */
    public Connection(KijiTable table, Clock clock) {
      mTable = table;
      mLastAccessTime = clock.getTime();
    }

    /**
     * Gets the table connection.
     *
     * @return The table connection.
     */
    public KijiTable getTable() {
      return mTable;
    }

    /**
     * Gets the last access time.
     *
     * @return The last access time.
     */
    public long getLastAccessTime() {
      return mLastAccessTime;
    }
  }

  /**
   * A thread that deletes any connections that have been idle for too long.
   */
  private class IdleTimeoutThread extends Thread {
    /** Default constructor. */
    public IdleTimeoutThread() {
      setDaemon(true); // This thread should not block system exit.
    }

    /** {@inheritDoc} */
    @Override
    public void run() {
      while (true) {
        for (Pool pool : mTableCache.values()) {
          pool.clean(mIdleTimeout);
        }
        try {
          sleep(mIdlePollPeriod);
        } catch (InterruptedException e) {
          LOG.info("Idle connection cleanup thread interrupted. Exiting...");
          break;
        }
      }
    }
  }
}
