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
package org.kiji.schema;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiTableReaderBuilder.OnDecoderCacheMiss;
import org.kiji.schema.KijiTableReaderPool.PooledKijiTableReader;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.ColumnReaderSpec;

/**
 * Maintains a pool of opened KijiTableReaders for reuse.
 *
 * <p>
 *   KijiTableReaders retrieved from the pool should be closed as normal when no longer needed.
 *   Closing a KijiTableReader from this pool will return it to the pool and make it available to
 *   other users.
 * </p>
 *
 * <p>
 *   KijiTableReaderPool instances retain the KijiTable associated with the KijiReaderFactory which
 *   provides readers for the pool. Closing the pool will release the table.
 * </p>
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class KijiTableReaderPool
    extends GenericObjectPool<PooledKijiTableReader>
    implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(KijiTableReaderPool.class);

  /** Builder for KijiTableReaderPool instances. */
  public static final class Builder {

    /**
     * Create a new KijiTableReaderPool.Builder.
     *
     * @return a new KijiTableReaderPool.Builder.
     */
    public static Builder create() {
      return new Builder();
    }

    /**
     * Possible behaviors of {@link org.kiji.schema.KijiTableReaderPool#borrowObject()} when a pool
     * is full.
     */
    public static enum WhenExhaustedAction {
      BLOCK, FAIL, GROW
    }

    /**
     * When build succeeds, the pool will retain the KijiTable associated with this
     * KijiReaderFactory. Closing the pool will release that table.
     */
    private KijiReaderFactory mReaderFactory = null;
    private Map<KijiColumnName, CellSpec> mCellSpecOverrides = null;
    private Map<KijiColumnName, ColumnReaderSpec> mColumnReaderSpecOverrides = null;
    private Multimap<KijiColumnName, ColumnReaderSpec> mColumnReaderSpecAlternatives = null;
    private OnDecoderCacheMiss mOnDecoderCacheMiss = null;
    private Integer mMinIdle = null;
    private Integer mMaxIdle = null;
    private Integer mMaxActive = null;
    private Long mMinEvictableIdleTime = null;
    private Long mTimeBetweenEvictionRuns = null;
    private WhenExhaustedAction mWhenExhaustedAction = null;
    private Long mMaxWait = null;

    /**
     * Set the KijiReaderFactory which will provide readers for this pool. Obtainable via
     * {@link org.kiji.schema.KijiTable#getReaderFactory()}. This method is required to build a
     * KijiTableReaderPool.
     *
     * @param readerFactory KijiReaderFactory which will be used to provide readers for this pool.
     * @return this.
     */
    public Builder withReaderFactory(
        final KijiReaderFactory readerFactory
    ) {
      Preconditions.checkNotNull(readerFactory, "KijiReaderFactory may not be null.");
      Preconditions.checkState(
          null == mReaderFactory, "KijiReaderFactory is already set to: %s", mReaderFactory);
      mReaderFactory = readerFactory;
      return this;
    }

    /**
     * Set the CellSpec overrides to use to build readers for this pool. This field is optional and
     * defaults to no overrides.
     *
     * @param overrides CellSpec overrides which will be used to configure all readers served by
     *     this pool.
     * @return this.
     */
    public Builder withCellSpecOverrides(
        final Map<KijiColumnName, CellSpec> overrides
    ) {
      Preconditions.checkNotNull(overrides, "CellSpec overrides may not be null.");
      Preconditions.checkState(null == mCellSpecOverrides,
          "CellSpec overrides are already set to: %s", mCellSpecOverrides);
      mCellSpecOverrides = overrides;
      return this;
    }

    /**
     * Set the minimum number of idle readers which the pool will attempt to maintain. If active +
     * idle would exceed max, this minimum will not be enforced. This field is optional and defaults
     * to {@link GenericObjectPool#DEFAULT_MIN_IDLE}.
     *
     * @param minIdle the minimum number of idle readers which the pool will attempt to maintain.
     * @return this.
     */
    public Builder withMinIdle(
        final int minIdle
    ) {
      Preconditions.checkArgument(
          0 <= minIdle, "Minimum idle count must be greater than or equal to 0.");
      Preconditions.checkState(
          null == mMinIdle, "Minimum idle count is already set to: %s", mMinIdle);
      mMinIdle = minIdle;
      return this;
    }

    /**
     * Set the maximum number of idle readers which may be maintained at a time. This field is
     * optional and defaults to {@link GenericObjectPool#DEFAULT_MAX_IDLE}.
     *
     * @param maxIdle maximum number of idle readers.
     * @return this.
     */
    public Builder withMaxIdle(
        final int maxIdle
    ) {
      Preconditions.checkArgument(
          0 <= maxIdle, "Maximum idle count must be greater than or equal to 0.");
      Preconditions.checkState(
          null == mMaxIdle, "Maximum idle count is already set to: %s", mMaxIdle);
      mMaxIdle = maxIdle;
      return this;
    }

    /**
     * Set the maximum number of simultaneously active readers. This field is optional and defaults
     * to {@link GenericObjectPool#DEFAULT_MAX_ACTIVE}.
     *
     * @param maxActive maximum number of active readers before
     *     {@link org.kiji.schema.KijiTableReaderPool#borrowObject()} changes its behavior according
     *     to {@link WhenExhaustedAction}.
     * @return this.
     */
    public Builder withMaxActive(
        final int maxActive
    ) {
      Preconditions.checkArgument(
          0 <= maxActive, "Maximum active count must be greater than or equal to 0.");
      Preconditions.checkState(
          null == mMaxActive, "Maximum active count is already set to: %s", mMaxActive);
      mMaxActive = maxActive;
      return this;
    }

    /**
     * Set the time in milliseconds after which a reader may be evicted for idleness. Readers are
     * not guaranteed to be evicted when this time has elapsed, but they may not be evicted before
     * it. This field is optional and defaults to
     * {@link GenericObjectPool#DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS}.
     *
     * @param minEvictableIdleTime time in milliseconds a reader must remain idle before it may be
     *     evicted.
     * @return this.
     */
    public Builder withMinEvictableIdleTime(
        final long minEvictableIdleTime
    ) {
      Preconditions.checkArgument(0 <= minEvictableIdleTime,
          "Minimum idle time before eviction must be greater than or equal to 0.");
      Preconditions.checkState(null == mMinEvictableIdleTime,
          "Minimum idle time before eviction is already set to: %s", mMinEvictableIdleTime);
      mMinEvictableIdleTime = minEvictableIdleTime;
      return this;
    }

    /**
     * Set the time in milliseconds between automatic eviction of idle readers. This field is
     * optional and defaults to {@link GenericObjectPool#DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS}.
     *
     * @param timeBetweenEvictionRuns time in milliseconds to wait between evicting readers which
     *     have been idle longer than the allowed duration.
     * @return this.
     */
    public Builder withTimeBetweenEvictionRuns(
        final long timeBetweenEvictionRuns
    ) {
      Preconditions.checkArgument(
          0 < timeBetweenEvictionRuns, "Time between eviction runs must be greater than 0.");
      Preconditions.checkState(null == mTimeBetweenEvictionRuns,
          "Time between eviction runs is already set to: %s", mTimeBetweenEvictionRuns);
      mTimeBetweenEvictionRuns = timeBetweenEvictionRuns;
      return this;
    }

    /**
     * Set the behavior of {@link org.kiji.schema.KijiTableReaderPool#borrowObject()} when the pool
     * is at maximum capacity. This field is optional and defaults to
     * {@link GenericObjectPool#DEFAULT_WHEN_EXHAUSTED_ACTION}.
     *
     * @param whenExhaustedAction behavior of
     *     {@link org.kiji.schema.KijiTableReaderPool#borrowObject()} when the pool is at maximum
     *     capacity.
     * @return this.
     */
    public Builder withExhaustedAction(
        final WhenExhaustedAction whenExhaustedAction
    ) {
      Preconditions.checkNotNull(whenExhaustedAction, "When exhausted action may not be null.");
      Preconditions.checkState(null == mWhenExhaustedAction,
          "When exhaused action is already set to: %s", mWhenExhaustedAction);
      mWhenExhaustedAction = whenExhaustedAction;
      return this;
    }

    /**
     * Set the maximum time (in milliseconds) to wait for an object to become available when using
     * {@link WhenExhaustedAction#BLOCK}. This field is optional and defaults to
     * {@link GenericObjectPool#DEFAULT_MAX_WAIT}.
     *
     * @param maxWait time in milliseconds to wait for an object to become available when borrowing.
     * @return this.
     */
    public Builder withMaxWaitToBorrow(
        final long maxWait
    ) {
      Preconditions.checkArgument(
          0 <= maxWait, "Max wait to borrow must be greater than or equal to 0.");
      Preconditions.checkState(
          null == mMaxWait, "Max wait to borrow is already set to: %s", mMaxWait);
      mMaxWait = maxWait;
      return this;
    }

    /**
     * Set the ColumnReaderSpec overrides which will be used as the default read behavior for
     * readers served from this pool. This field may not be set with CellSpec overrides.
     *
     * @param overrides mapping from column names to overriding ColumnReaderSpecs which will be the
     *     default read behavior for readers served from this pool.
     * @return this.
     */
    public Builder withColumnReaderSpecOverrides(
        final Map<KijiColumnName, ColumnReaderSpec> overrides
    ) {
      Preconditions.checkNotNull(overrides, "ColumnReaderSpec overrides may not be null.");
      Preconditions.checkState(null == mColumnReaderSpecOverrides,
          "ColumnReaderSpec overrides are already set to: %s", mColumnReaderSpecOverrides);
      Preconditions.checkState(null == mCellSpecOverrides, "ColumnReaderSpec overrides are mutually"
          + " exclusive with CellSpec CellSpec overrides are already set to: %s",
          mCellSpecOverrides);
      mColumnReaderSpecOverrides = overrides;
      return this;
    }

    /**
     * Set the ColumnReaderSpec alternatives for which cell decoders will be available, but which
     * will not change the default behavior of read requests. Column name, ColumnReaderSpec pairs in
     * this list will not trigger failures when OnDecoderCacheMiss is set to FAIL. This field may
     * not be set with CellSpec overrides.
     *
     * @param alternatives mapping from column names to alternative ColumnReaderSpecs which will be
     *     available from readers served by this pool.
     * @return this.
     */
    public Builder withColumnReaderSpecAlternatives(
        final Multimap<KijiColumnName, ColumnReaderSpec> alternatives
    ) {
      Preconditions.checkNotNull(alternatives, "ColumnReaderSpec alternatives may not be null.");
      Preconditions.checkState(null == mColumnReaderSpecAlternatives,
          "ColumnReaderSpec alternatives are already set to: %s", mColumnReaderSpecAlternatives);
      Preconditions.checkState(null == mCellSpecOverrides, "ColumnReaderSpec alternatives are "
          + "mutually exclusive with CellSpec overrides. CellSpec overrides are already set to: %s",
          mCellSpecOverrides);
      mColumnReaderSpecAlternatives = alternatives;
      return this;
    }

    /**
     * Set the behavior of readers served by this pool when they cannot find a cell decoder. This
     * field may not be set with CellSpec overrides.
     *
     * @param onDecoderCacheMissBehavior behavior of readers served by this pool when they cannot
     *     find a cell decoder.
     * @return this.
     */
    public Builder withOnDecoderCacheMissBehavior(
        final OnDecoderCacheMiss onDecoderCacheMissBehavior
    ) {
      Preconditions.checkNotNull(onDecoderCacheMissBehavior,
          "OnDecoderCacheMiss behavior may not be null.");
      Preconditions.checkState(null == mOnDecoderCacheMiss,
          "OnDecoderCacheMiss behavior is already set to: %s", mOnDecoderCacheMiss);
      mOnDecoderCacheMiss = onDecoderCacheMissBehavior;
      return this;
    }

    /**
     * Get the KijiReaderFactory configured in this Builder, or null if none has been set.
     *
     * @return the KijiReaderFactory.
     */
    public KijiReaderFactory getReaderFactory() {
      return mReaderFactory;
    }

    /**
     * Get the CellSpec overrides configured in this Builder, or null if none has been set.
     * @return the CellSpec overrides.
     */
    public Map<KijiColumnName, CellSpec> getCellSpecOverrides() {
      return mCellSpecOverrides;
    }

    /**
     * Get the minimum number of idle readers before the reaper thread may begin garbage collection
     * configured in this Builder, or null if none has been set.
     *
     * @return the minimum number of idle readers before the reaper thread may begin garbage
     *     collection.
     */
    public Integer getMinIdle() {
      return mMinIdle;
    }

    /**
     * Get the maximum number of idle readers the pool will allow to exist at a time configured in
     * this Builder, or null if none has been set.
     *
     * @return the maximum number of idle readers the pool will allow to exist at a time.
     */
    public Integer getMaxIdle() {
      return mMaxIdle;
    }

    /**
     * Get the maximum number of active (borrowed + idle) readers the pool will allow to exist at a
     * time configured in this Builder, or null if none has been set.
     *
     * @return the maximum number of active (borrowed + idle) readers the pool will allow to exist
     *     at a time.
     */
    public Integer getMaxActive() {
      return mMaxActive;
    }

    /**
     * Get the minimum idle time in milliseconds before a reader may be garbage collected configured
     * in this Builder, or null if none has been set.
     *
     * @return the minimum idle time in milliseconds before a reader may be garbage collected.
     */
    public Long getMinEvictableIdleTime() {
      return mMinEvictableIdleTime;
    }

    /**
     * Get the time in milliseconds between eviction runs configured in this Builder, or null if
     * none has been set.
     *
     * @return the time in milliseconds between eviction runs.
     */
    public Long getTimeBetweenEvictionRuns() {
      return mTimeBetweenEvictionRuns;
    }

    /**
     * Get the WhenExhaustedAction configured in this Builder, or null if none has been set.
     *
     * @return the WhenExhaustedAction.
     */
    public WhenExhaustedAction getWhenExhaustedAction() {
      return mWhenExhaustedAction;
    }

    /**
     * Get the maximum time in milliseconds the borrowObject method should block before throwing an
     * exception when the WhenExhaustedAction is set to BLOCK configured in this Builder, or null if
     * none has been set.
     *
     * @return the maximum time in milliseconds the borrowObject method should block before throwing
     * an exception when the WhenExhaustedAction is set to BLOCK.
     */
    public Long getMaxWait() {
      return mMaxWait;
    }

    /**
     * Get the ColumnReaderSpec overrides configured in this Builder, or null if none have been set.
     *
     * @return  the ColumnReaderSpec overrides configured in this Builder, or null if none have been
     *     set.
     */
    public Map<KijiColumnName, ColumnReaderSpec> getColumnReaderSpecOverrides() {
      return mColumnReaderSpecOverrides;
    }

    /**
     * Get the ColumnReaderSpec alternatives configured in this Builder, or null if none have been
     * set.
     *
     * @return  the ColumnReaderSpec alternatives configured in this Builder, or null if none have
     *     been set.
     */
    public Multimap<KijiColumnName, ColumnReaderSpec> getColumnReaderSpecAlternatives() {
      return mColumnReaderSpecAlternatives;
    }

    /**
     * Get the configured OnDecoderCacheMiss behavior from this Builder, or null if none has been
     * set.
     *
     * @return the configured OnDecoderCacheMiss behavior from this Builder, or null if none has
     * been set.
     */
    public OnDecoderCacheMiss getOnDecoderCacheMiss() {
      return mOnDecoderCacheMiss;
    }

    /**
     * Build a new KijiTableReaderPool from the configured options.
     *
     * @return a new KijiTableReaderPool.
     * @throws IOException in case of an error getting the reader factory.
     */
    public KijiTableReaderPool build() throws IOException {
      Preconditions.checkNotNull(mReaderFactory, "KijiReaderFactory may not be null.");

      final GenericObjectPool.Config config = new Config();
      config.minIdle = (null != mMinIdle) ? mMinIdle : GenericObjectPool.DEFAULT_MIN_IDLE;
      config.maxIdle = (null != mMaxIdle) ? mMaxIdle : GenericObjectPool.DEFAULT_MAX_IDLE;
      config.maxActive = (null != mMaxActive) ? mMaxActive : GenericObjectPool.DEFAULT_MAX_ACTIVE;
      config.minEvictableIdleTimeMillis = (null != mMinEvictableIdleTime)
          ? mMinEvictableIdleTime : GenericObjectPool.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS;
      config.timeBetweenEvictionRunsMillis = (null != mTimeBetweenEvictionRuns)
          ? mTimeBetweenEvictionRuns : GenericObjectPool.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS;
      if (null != mWhenExhaustedAction) {
        switch (mWhenExhaustedAction) {
          case BLOCK: {
            config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
            break;
          }
          case FAIL: {
            config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_FAIL;
            break;
          }
          case GROW: {
            config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_GROW;
            break;
          }
          default: throw new InternalKijiError(
              String.format("Unknown WhenExhaustedAction: %s", mWhenExhaustedAction));
        }
      } else {
        config.whenExhaustedAction = GenericObjectPool.DEFAULT_WHEN_EXHAUSTED_ACTION;
      }
      config.maxWait = (null != mMaxWait) ? mMaxWait : GenericObjectPool.DEFAULT_MAX_WAIT;
      // Set all unsupported options to default.
      config.lifo = GenericObjectPool.DEFAULT_LIFO;
      config.numTestsPerEvictionRun = GenericObjectPool.DEFAULT_NUM_TESTS_PER_EVICTION_RUN;
      config.softMinEvictableIdleTimeMillis =
          GenericObjectPool.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS;
      config.testOnBorrow = GenericObjectPool.DEFAULT_TEST_ON_BORROW;
      config.testOnReturn = GenericObjectPool.DEFAULT_TEST_ON_RETURN;
      config.testWhileIdle = GenericObjectPool.DEFAULT_TEST_WHILE_IDLE;

      // Nothing that may fail may come after retain().
      mReaderFactory.getTable().retain();

      if (null == mCellSpecOverrides) {
        if (null == mColumnReaderSpecOverrides) {
          mColumnReaderSpecOverrides = KijiTableReaderBuilder.DEFAULT_READER_SPEC_OVERRIDES;
        }
        if (null == mColumnReaderSpecAlternatives) {
          mColumnReaderSpecAlternatives = KijiTableReaderBuilder.DEFAULT_READER_SPEC_ALTERNATIVES;
        }
        if (null == mOnDecoderCacheMiss) {
          mOnDecoderCacheMiss = KijiTableReaderBuilder.DEFAULT_CACHE_MISS;
        }
        return KijiTableReaderPool.create(
            mReaderFactory,
            mColumnReaderSpecOverrides,
            mColumnReaderSpecAlternatives,
            mOnDecoderCacheMiss,
            config);
      } else {
        return KijiTableReaderPool.create(mReaderFactory, mCellSpecOverrides, config);
      }
    }
  }

  /** Factory which provides readers for this pool. */
  private final PooledKijiTableReaderFactory mFactory;

  /**
   * Initialize a new KijiTableReaderPool with the given reader factory and configuration.
   *
   * @param factory reader factory which will provide pooled readers for this pool.
   * @param config pool configuration which determines the pools behavior.
   */
  private KijiTableReaderPool(
      final PooledKijiTableReaderFactory factory,
      final GenericObjectPool.Config config
  ) {
    super(factory, config);
    factory.setPool(this);
    mFactory = factory;
  }

  /**
   * Create a new KijiTableReaderPool which uses the given reader factory to create reader objects.
   *
   * @param readerFactory KijiReaderFactory from which to get new reader instances.
   * @param overrides Optional CellSpec overrides with which to build the readers.
   * @param config Configuration which describes the behavior of the pool.
   * @return a new KijiTableReaderPool.
   */
  public static KijiTableReaderPool create(
      final KijiReaderFactory readerFactory,
      final Map<KijiColumnName, CellSpec> overrides,
      final GenericObjectPool.Config config
  ) {
    final PooledKijiTableReaderFactory factory =
        new PooledKijiTableReaderFactory(readerFactory, overrides);
    return new KijiTableReaderPool(factory, config);
  }

  /**
   * Create a new KijiTableReaderPool which uses the given reader factory to create reader objects.
   *
   * @param readerFactory KijiReaderFactory from which to get new reader instances.
   * @param overrides ColumnReaderSpec overrides with which to build the readers.
   * @param alternatives ColumnReaderSpec alternatives with which to build the readers.
   * @param onDecoderCacheMiss Behavior of the reader when a cell decoder cannot be found.
   * @param config Configuration which describes the behavior of the pool.
   * @return a new KijiTableReaderPool.
   */
  public static KijiTableReaderPool create(
      final KijiReaderFactory readerFactory,
      final Map<KijiColumnName, ColumnReaderSpec> overrides,
      final Multimap<KijiColumnName, ColumnReaderSpec> alternatives,
      final OnDecoderCacheMiss onDecoderCacheMiss,
      final GenericObjectPool.Config config
  ) {
    final PooledKijiTableReaderFactory factory = new PooledKijiTableReaderFactory(
        readerFactory, overrides, alternatives, onDecoderCacheMiss);
    return new KijiTableReaderPool(factory, config);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    try {
      super.close();
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      throw new KijiIOException(e);
    }
    mFactory.mFactoryDelegate.getTable().release();
  }

  /**
   * Factory for pooled KijiTableReaders. Users of KijiTableReaderPool should never need to see this
   * class. An instance of this class may only serve one KijiTableReaderPool at a time.
   */
  public static final class PooledKijiTableReaderFactory
      implements PoolableObjectFactory<PooledKijiTableReader> {

    private final KijiReaderFactory mFactoryDelegate;
    private final Map<KijiColumnName, CellSpec> mCellSpecOverrides;
    private final Map<KijiColumnName, ColumnReaderSpec> mColumnReaderSpecOverrides;
    private final Multimap<KijiColumnName, ColumnReaderSpec> mColumnReaderSpecAlternatives;
    private final OnDecoderCacheMiss mOnDecoderCacheMiss;
    /**
     * This pool must be set using {@link #setPool(KijiTableReaderPool)} before any other operations
     * are attempted.
     */
    private KijiTableReaderPool mPool;

    /**
     * Initialize a new PooledKijiTableReaderFactory wrapping the given KijiReaderFactory.
     *
     * @param factoryDelegate KijiReaderFactory to be wrapped by this factory.
     * @param overrides CellSpec overrides which will be used to construct all readers built by this
     *     factory.
     */
    public PooledKijiTableReaderFactory(
        final KijiReaderFactory factoryDelegate,
        final Map<KijiColumnName, CellSpec> overrides
    ) {
      mFactoryDelegate = factoryDelegate;
      mCellSpecOverrides = overrides;
      mColumnReaderSpecOverrides = null;
      mColumnReaderSpecAlternatives = null;
      mOnDecoderCacheMiss = null;
    }

    /**
     * Initialize a new PooledKijiTableReaderFactory wrapping the given KijiReaderFactory.
     *
     * @param factoryDelegate KijiReaderFactory to be wrapped by this factory.
     * @param overrides ColumnReaderSpec overrides with which to build the readers.
     * @param alternatives ColumnReaderSpec alternatives with which to build the readers.
     * @param onDecoderCacheMiss Behavior of the reader when a cell decoder cannot be found.
     */
    public PooledKijiTableReaderFactory(
        final KijiReaderFactory factoryDelegate,
        final Map<KijiColumnName, ColumnReaderSpec> overrides,
        final Multimap<KijiColumnName, ColumnReaderSpec> alternatives,
        final OnDecoderCacheMiss onDecoderCacheMiss
    ) {
      mFactoryDelegate = factoryDelegate;
      mColumnReaderSpecOverrides = overrides;
      mColumnReaderSpecAlternatives = alternatives;
      mOnDecoderCacheMiss = onDecoderCacheMiss;
      mCellSpecOverrides = null;
    }

    /**
     * Set the pool this factory serves. This can only be set once in the lifetime of the factory.
     * Must be set before any other operations are called on the factory.
     *
     * @param pool KijiTableReaderPool which this factory will serve.
     */
    private void setPool(
        final KijiTableReaderPool pool
    ) {
      Preconditions.checkState(null == mPool, "Pool is already set to: %s", mPool);
      mPool = pool;
    }

    /** {@inheritDoc} */
    @Override
    public PooledKijiTableReader makeObject() throws Exception {
      final KijiTableReader innerReader;
      if (null == mCellSpecOverrides) {
        innerReader = mFactoryDelegate.readerBuilder()
            .withColumnReaderSpecOverrides(mColumnReaderSpecOverrides)
            .withColumnReaderSpecAlternatives(mColumnReaderSpecAlternatives)
            .withOnDecoderCacheMiss(mOnDecoderCacheMiss)
            .buildAndOpen();
      } else {
        innerReader = mFactoryDelegate.openTableReader(mCellSpecOverrides);
      }

      return new PooledKijiTableReader(innerReader, mPool);
    }

    /** {@inheritDoc} */
    @Override
    public void destroyObject(final PooledKijiTableReader obj) throws Exception {
      obj.mInnerReader.close();
    }

    /** {@inheritDoc} */
    @Override
    public boolean validateObject(final PooledKijiTableReader obj) {
      return obj.mAvailable;
    }

    /** {@inheritDoc} */
    @Override
    public void activateObject(final PooledKijiTableReader obj) throws Exception {
      obj.mAvailable = false;
    }

    /** {@inheritDoc} */
    @Override
    public void passivateObject(final PooledKijiTableReader obj) throws Exception {
      obj.mAvailable = true;
    }
  }

  /**
   * KijiTableReader implementation which can be served from a pool. {@link #close()} returns the
   * reader to the pool.
   */
  public static final class PooledKijiTableReader implements KijiTableReader {

    private final KijiTableReader mInnerReader;
    private final KijiTableReaderPool mPool;
    /** True indicates that this reader is available to be served from the pool. */
    private boolean mAvailable = true;

    /**
     * Initialize a new PooledKijiTableReader which uses the given inner reader to fulfill reads and
     * which is served from the given pool.
     *
     * @param innerReader KijiTableReader to which this pooled reader delegates for read operations.
     * @param pool KijiTableReaderPool from which this reader is served.
     */
    public PooledKijiTableReader(
        final KijiTableReader innerReader,
        final KijiTableReaderPool pool
    ) {
      mInnerReader = innerReader;
      mPool = pool;
    }

    /** {@inheritDoc} */
    @Override
    public KijiRowData get(
        final EntityId entityId,
        final KijiDataRequest dataRequest
    ) throws IOException {
      return mInnerReader.get(entityId, dataRequest);
    }

    /** {@inheritDoc} */
    @Override
    public List<KijiRowData> bulkGet(
        final List<EntityId> entityIds,
        final KijiDataRequest dataRequest
    ) throws IOException {
      return mInnerReader.bulkGet(entityIds, dataRequest);
    }

    /** {@inheritDoc} */
    @Override
    public KijiRowScanner getScanner(
        final KijiDataRequest dataRequest
    ) throws IOException {
      return mInnerReader.getScanner(dataRequest);
    }

    /** {@inheritDoc} */
    @Override
    public KijiRowScanner getScanner(
        final KijiDataRequest dataRequest,
        final KijiScannerOptions scannerOptions
    ) throws IOException {
      return mInnerReader.getScanner(dataRequest, scannerOptions);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      try {
        mPool.returnObject(this);
      } catch (Exception e) {
        if (e instanceof IOException) {
          throw (IOException) e;
        } else {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
