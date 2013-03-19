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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.Clock;
import org.kiji.schema.util.ResourceUtils;

/**
 * Maintains a pool of opened KijiTables for reuse.
 *
 * <p>Instead of creating a new KijiTable instance when needed, clients may use a
 * KijiTablePool to keep a pool of opened tables for reuse. When a client asks for a
 * KijiTable, the pool first checks the cache for an already opened and available
 * table. If available, the cached table will be returned. Otherwise, a new one will be
 * opened and returned. When the client is finished, it should call release() to allow
 * other clients or threads the option to reuse the opened table.</p>
 *
 * <h2>Building a KijiTablePool:</h2>
 * KijiTablePools are constructed using a {@link KijiTablePoolBuilder}.
 * <pre><code>
 *   KijiTablePool pool = KijiTablePool.newBuilder(mKiji)
 *       .withIdleTimeout(10)
 *       .withIdlePollPeriod(1)
 *       .build();
 * </code></pre>
 *
 * <h2>Obtaining and releasing KijiTables from the pool:</h2>
 * <p>
 *   Once you have the pool, KijiTables can be obtained using {@link #get}.  These tables can are
 *   returned the pool using the {@link org.kiji.schema.KijiTable#release()} method.
 * </p>
 * <pre><code>
 *   KijiTable fooTable = pool.get("foo");
 *   // Do some magic.
 *   fooTable.release();
 * </code></pre>
 *
 * <p>
 *   This class is thread-safe, but the individual KijiTables that are returned from it are not.
 * </p>
 */
@ApiAudience.Public
public final class KijiTablePool implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(KijiTablePool.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger(KijiTablePool.class.getName() + ".Cleanup");

  /** Default minimum pool size. */
  public static final int DEFAULT_MIN_POOL_SIZE = 0;

  /** Default maximum pool size. */
  public static final int DEFAULT_MAX_POOL_SIZE = Integer.MAX_VALUE;

  /** Default idle timeout in milliseconds. */
  public static final long DEFAULT_IDLE_TIMEOUT = 0L;

  /** Default idle polling period in milliseconds (10 seconds). */
  public static final long DEFAULT_IDLE_POLL_PERIOD = 10000L;

  /** A factory for creating new opened HTables. */
  private final KijiTableFactory mTableFactory;

  /** A clock. */
  private final Clock mClock;

  /** The minimum number of connections to keep per table. */
  private final int mMinSize;

  /** The maximum number of connections to keep per table. */
  private final int mMaxSize;

  /** Milliseconds before an idle table will be eligible for cleanup. */
  private final long mIdleTimeout;

  /** Number of milliseconds to wait between sweeps for idle tables. */
  private final long mIdlePollPeriod;

  /** A map from table names to their connection pools. */
  private final Map<String, Pool> mPoolCache;

  /** A cleanup thread for idle connections. */
  private IdleTimeoutThread mCleanupThread;

  /** Whether the table pool is open. */
  private boolean mIsOpen;

  /**
   * Builder class for KijiTablePool instances.  These should be constructed with
   * {@link #newBuilder} instead.
   */
  public static final class KijiTablePoolBuilder {
    private KijiTableFactory mKijiTableFactory;
    private int mMinSize;
    private int mMaxSize;
    private long mIdleTimeout;
    private long mIdlePollPeriod;
    private Clock mClock;

    /**
     * Creates a KijiTablePoolBuilder with for the specified Kiji instance and the default options.
     *
     * @param kiji TableFactory to be used for constructing tables for the table pool.  A kiji
     *             instance is the normal source for this.
     */
    KijiTablePoolBuilder(KijiTableFactory kiji) {
      mKijiTableFactory = kiji;
      mMinSize = DEFAULT_MIN_POOL_SIZE;
      mMaxSize = DEFAULT_MAX_POOL_SIZE;
      mIdleTimeout = DEFAULT_IDLE_TIMEOUT;
      mIdlePollPeriod = DEFAULT_IDLE_POLL_PERIOD;
      mClock = Clock.getDefaultClock();
    }

    /**
     * Sets the minimum number of connections to keep per table.
     *
     * @param minSize The min number of connections to keep per table.
     * @return This options object for method chaining.
     */
    public KijiTablePoolBuilder withMinSize(int minSize) {
      mMinSize = minSize;
      return this;
    }

    /**
     * Sets the maximum number of connections to keep per table.
     *
     * <p>Use zero(0) to indicate that the pool should be unbounded.</p>
     *
     * @param maxSize The max number of connections to keep per table.
     * @return This options object for method chaining.
     */
    public KijiTablePoolBuilder withMaxSize(int maxSize) {
      mMaxSize = (0 == maxSize) ? Integer.MAX_VALUE : maxSize;
      return this;
    }

    /**
     * Sets the amount of time a connection may be idle before being removed from the pool.
     *
     * <p>Use zero (0) to indicate that connections should never be removed.</p>
     *
     * @param timeoutMillis Timeout in milliseconds.
     * @return This options object for method chaining.
     */
    public KijiTablePoolBuilder withIdleTimeout(long timeoutMillis) {
      mIdleTimeout = timeoutMillis;
      return this;
    }

    /**
     * Sets the amount of time between sweeps of the pool for removing idle connections.
     *
     * @param periodMillis Number of milliseconds between sweeps.
     * @return This options object for method chaining.
     */
    public KijiTablePoolBuilder withIdlePollPeriod(long periodMillis) {
      mIdlePollPeriod = periodMillis;
      return this;
    }

    /**
     * Sets a clock.
     *
     * @param clock A clock.
     * @return This options object for method chaining.
     */
    public KijiTablePoolBuilder withClock(Clock clock) {
      mClock = clock;
      return this;
    }

    /**
     * Builds the configured KijiTablePool.
     *
     * @return KijiTablePool with the specified parameters.
     */
    public KijiTablePool build() {
      return new KijiTablePool(this);
    }
  }

  /**
   * Constructs a new KijiTablePoolBuilder for the specified Kiji instance.
   *
   * @param kijiTableFactory table factory to be used for the table pool.  Can be a Kiji instance.
   * @return a new KijiTablePoolBuilder with the default options.
   */
  public static KijiTablePoolBuilder newBuilder(KijiTableFactory kijiTableFactory) {
    return new KijiTablePoolBuilder(kijiTableFactory);
  }

  /**
   * Constructs a new pool of Kiji tables with the specified parameters.  This class should not
   * be instantiated outside of the builder {@link KijiTablePoolBuilder}.
   *
   * @param builder KijiTablePoolBuilder which contains the configuration parameters to build
   *                this KijiTablePool with.
   */
  private KijiTablePool(KijiTablePoolBuilder builder) {
    mTableFactory = builder.mKijiTableFactory;
    mClock = builder.mClock;
    mMinSize = builder.mMinSize;
    mMaxSize = builder.mMaxSize;
    mIdleTimeout = builder.mIdleTimeout;
    mIdlePollPeriod = builder.mIdlePollPeriod;
    mPoolCache = new HashMap<String, Pool>();
    mIsOpen = true;
  }

  /**
   * Thrown when an attempt to get a table connection fails because there is no room in the pool.
   */
  @ApiAudience.Public
  public static final class NoCapacityException extends IOException {
    /**
     * Creates a new <code>NoCapacityException</code> with the specified detail message.
     * @param message The exception message.
     */
    public NoCapacityException(String message) {
      super(message);
    }
  }

  /**
   * Gets a previously opened table from the pool, or open a new connection. Clients should release
   * the table back to the pool when finished by passing it in call to release().
   *
   * @param name The name of the Kiji table.
   * @return An opened Kiji table.
   * @throws IOException If there is an error.
   * @throws KijiTablePool.NoCapacityException If the table pool is at capacity.
   */
  public synchronized KijiTable get(String name) throws IOException {
    // Starts a cleanup thread if necessary.
    if (mIdleTimeout > 0L && null == mCleanupThread) {
      LOG.debug("Starting cleanup thread for table pool.");
      mCleanupThread = new IdleTimeoutThread();
      mCleanupThread.start();
    }

    LOG.debug("Retrieving a connection for {} from the table pool.", name);
    if (!mIsOpen) {
      throw new IllegalStateException("Table pool is closed.");
    }

    if (!mPoolCache.containsKey(name)) {
      mPoolCache.put(name, new Pool(name));
    }

    return mPoolCache.get(name).getTable();
  }

  /**
   * Explicitly force a cleanup of table connections that have been idle too long.
   */
  synchronized void cleanIdleConnections() {
    if (mIdleTimeout > 0) {
      for (Pool pool: mPoolCache.values()) {
        pool.clean(mIdleTimeout);
      }
    }
  }

  /**
   * Releases the tables in the pool.
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
    for (Pool pool : mPoolCache.values()) {
      ResourceUtils.closeOrLog(pool);
    }
    mPoolCache.clear();
    mIsOpen = false;
  }

  @Override
  protected void finalize() throws Throwable {
    if (mIsOpen) {
      CLEANUP_LOG.warn("Closing KijiTablePool in finalize(). You should close it explicitly");
      close();
    }
    super.finalize();
  }

  /**
   * Gets the total number of connections, active and cached, for the specified table.
   *
   * @param tableName The name of the table you wish to know the pool size of.
   * @return The size of the table pool.
   */
  public int getPoolSize(String tableName) {
    return mPoolCache.get(tableName).getPoolSize();
  }

  /**
   * A pool of connections for a single table. Maintains a number of
   * connections in use, and a queue of available ones for re-use.
   */
  private final class Pool implements Closeable {
    private final Queue<Connection> mConnections;
    // The total pool size is the total number of tables in use and available connections.
    private int mPoolSize;

    // The name of the table for this pool.
    private final String mTableName;

    /**
     * Constructor.
     * @param tableName The name of the table that this pool is for.
     */
    private Pool(String tableName) {
      mConnections = new ArrayDeque<Connection>();
      mPoolSize = 0;
      mTableName = tableName;
    }

    /**
     * Gets a table connection from the pool.
     *
     * @return The table connection.
     * @throws IOException If there is an error opening the table.
     * @throws KijiTablePool.NoCapacityException If there is no more room in the
     *     pool to open a new connection.
     */
    public synchronized KijiTable getTable() throws IOException {
      Connection availableConnection = mConnections.poll();
      if (null == availableConnection) {
        if (mPoolSize >= mMaxSize) {
          throw new NoCapacityException("Reached max pool size for table " + mTableName + ". There"
            + " are " + mPoolSize + " tables in the pool.");
        }
        LOG.debug("Cache miss for table {}", mTableName);
        KijiTable tableConnection = new Connection(mTableFactory.openTable(mTableName), this);
        mPoolSize++;
        if (mPoolSize < mMinSize) {
          LOG.debug("Below the min pool size for table {}. Adding to the pool.", mTableName);
          while (mPoolSize < mMinSize) {
            mConnections.add(new Connection(mTableFactory.openTable(mTableName), this));
            mPoolSize++;
          }
        }
        return tableConnection;
      }
      LOG.debug("Cache hit for table {}", mTableName);
      final int counter = availableConnection.mRetainCount.incrementAndGet();
      // TODO(SCHEMA-246): Instead of failing here, open a new connection and return it.
      Preconditions.checkState(counter == 1,
          "Cannot get retained KijiTable %s: retain counter was %s.",
          availableConnection.getURI(), counter);
      return availableConnection;
    }

    /**
     * Returns a table back to the pool so it may be reused.  Private so that only a wrapped
     * table can be returned back to the queue.
     *
     * @param table The table to return back into the pool.
     */
    private synchronized void returnConnection(Connection table) {
      mConnections.add(table);
    }

    /** @return the clock used by this KijiTablePool for updating KijiTable access times. */
    private Clock getClock() {
      return mClock;
    }

    /**
     * Cleans any connections from the pool that have been idle, while maintaining the minimum pool
     * size.
     *
     * @param idleTimeout Milliseconds idle required to be closed and
     *     removed from the pool.
     */
    public synchronized void clean(long idleTimeout) {
      long currentTime = mClock.getTime();
      Iterator<Connection> iterator = mConnections.iterator();
      while (iterator.hasNext() && mPoolSize > mMinSize) {
        Connection connection = iterator.next();
        if (currentTime - connection.getLastAccessTime() > idleTimeout) {
          LOG.info("Closing idle KijiTable connection to {}.", connection.getName());
          iterator.remove();
          ResourceUtils.releaseOrLog(connection.mTable);
          mPoolSize--;
        }
      }
    }

    /**
     * Gets the total number of connections, active and cached, in the pool.
     *
     * @return The size of the table pool.
     */
    public synchronized int getPoolSize() {
        return mPoolSize;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void close() throws IOException {
      while (!mConnections.isEmpty()) {
        ResourceUtils.releaseOrLog(mConnections.remove().mTable);
      }
    }
  }

  /**
   * A connection in the pool.  This class wraps a KijiTable, and {@link #release()} can be
   * called to return this connection to the pool.
   *
   * {@link #retain()} and {@link #close()} throw UnsupportedOptionException to avoid
   * improper states.
   */
  private static class Connection implements KijiTable {
    private final KijiTable mTable;
    private long mLastAccessTime;
    private Pool mPool;

    /** Internal retention count for wrapped pool connections. */
    private AtomicInteger mRetainCount = new AtomicInteger(1);

    /**
     * Constructor.
     * @param table The table connection to wrap.
     * @param pool The pool that this Connection is associated with.
     */
    public Connection(KijiTable table, Pool pool) {
      mTable = table;
      mPool = pool;
      mLastAccessTime = pool.getClock().getTime();
    }

    /**
     * Gets the last access time.
     *
     * @return The last access time.
     */
    public long getLastAccessTime() {
      return mLastAccessTime;
    }

    // Unwrapped methods to manage the lifecycle of KijiTables obtained from a KijiTablePool.

    /**
     * Allows clients to express interest in retaining KijiTables that are retrieved from the
     * pool.  These semantics are not recommended, as this would be circumventing the features of
     * the KijiTablePools.
     *
     * {@inheritDoc}
     */
    @Override
    public KijiTable retain() {
      LOG.warn("Retaining KijiTable obtained from a KijiTablePool is not recommended.");
      final int counter = mRetainCount.incrementAndGet();
      Preconditions.checkState(counter >= 2,
          "Cannot retain a closed KijiTable %s: retain counter was %s.", getURI(), counter);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public void release() throws IOException {
      final int counter = mRetainCount.decrementAndGet();
      Preconditions.checkState(counter >= 0,
          "Cannot release KijiTable %s that has been returned: retain counter is now %s.",
          getURI(), counter);
      if (counter == 0) {
        mLastAccessTime = mPool.getClock().getTime();
        mPool.returnConnection(this);
      }
    }

    // Methods that use the wrapped KijiTable.
    /** {@inheritDoc} */
    @Override
    public Kiji getKiji() {
      return mTable.getKiji();
    }

    /** {@inheritDoc} */
    @Override
    public String getName() {
      return mTable.getName();
    }

    /** {@inheritDoc} */
    @Override
    public KijiURI getURI() {
      return mTable.getURI();
    }

    /** {@inheritDoc} */
    @Override
    public KijiTableLayout getLayout() {
      return mTable.getLayout();
    }

    /** {@inheritDoc} */
    @Override
    public EntityId getEntityId(Object... kijiRowKey) {
      return mTable.getEntityId(kijiRowKey);
    }

    /** {@inheritDoc} */
    @Override
    public KijiTableReader openTableReader() {
      return mTable.openTableReader();
    }

    /** {@inheritDoc} */
    @Override
    public KijiTableWriter openTableWriter() {
      return mTable.openTableWriter();
    }

    /** {@inheritDoc} */
    @Override
    public KijiWriterFactory getWriterFactory() throws IOException {
      return mTable.getWriterFactory();
    }

    /** {@inheritDoc} */
    @Override
    public List<KijiRegion> getRegions() throws IOException {
      return mTable.getRegions();
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
        for (Pool pool : mPoolCache.values()) {
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
