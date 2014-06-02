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
package org.kiji.schema.layout.impl;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.RuntimeInterruptedException;
import org.kiji.schema.impl.LayoutConsumer;
import org.kiji.schema.impl.LayoutConsumer.Registration;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.JvmId;
import org.kiji.schema.util.ReferenceCountedCache;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.schema.zookeeper.TableLayoutTracker;
import org.kiji.schema.zookeeper.TableLayoutUpdateHandler;
import org.kiji.schema.zookeeper.TableUserRegistration;

/**
 * TableLayoutMonitor provides three services for users of table layouts:
 *
 *  1) it acts as a KijiTableLayout cache which is automatically refreshed when the table layout is
 *     updated.
 *  2) it allows LayoutConsumer instances to register to receive a callback when the table layout
 *     changes.
 *  3) it registers as a table user in ZooKeeper, and keeps that registration up-to-date with the
 *     oldest version of table layout being used by registered LayoutConsumers.
 */
@ApiAudience.Private
public interface TableLayoutMonitor extends Closeable {
  /**
   * Get the Kiji table layout of the table.
   *
   * @return the table's layout.
   */
  KijiTableLayout getLayout();

  /**
   * Register a LayoutConsumer to receive a callback when this table's layout is updated. This
   * method returns a registration object which should be closed when layout updates are no longer
   * needed.
   *
   * The consumer will immediately be notified of the current layout before returning.
   *
   * @param consumer to notify when the table's layout is updated.
   * @return a registration object which should be closed when layout updates are no longer needed.
   * @throws java.io.IOException if the consumer's {@code #update} method throws IOException.
   */
  LayoutConsumer.Registration registerLayoutConsumer(LayoutConsumer consumer)
      throws IOException;

  /**
   * Get the set of registered layout consumers.  All layout consumers should be updated using
   *
   * {@link LayoutConsumer#update} before this table reports that it has successfully update its
   * layout.
   *
   * This method is for testing purposes only.  It should not be used externally.
   *
   * @return the set of registered layout consumers.
   */
  Set<LayoutConsumer> getLayoutConsumers();

  /**
   * Update all registered LayoutConsumers with a new KijiTableLayout.
   *
   * This method is for testing purposes only.  It should not be used externally.
   *
   * @param layout the new KijiTableLayout with which to update consumers.
   * @throws IOException in case of an error updating LayoutConsumers.
   */
  void updateLayoutConsumers(KijiTableLayout layout) throws IOException;

  /**
   * The default implementation for {@link TableLayoutMonitor}.  Closing this table layout monitor
   * will cancel any ZooKeeper nodes and watches it has registered.
   */
  public static final class DefaultTableLayoutMonitor implements TableLayoutMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(TableLayoutMonitor.class);

    private static final AtomicLong TABLE_COUNTER = new AtomicLong(0);

    private final KijiURI mTableURI;

    private final KijiSchemaTable mSchemaTable;

    private final KijiMetaTable mMetaTable;

    private final TableLayoutTracker mTableLayoutTracker;

    private final TableUserRegistration mUserRegistration;

    private final CountDownLatch mInitializationLatch;

    /** States of a table layout monitor. */
    private static enum State {
      /** The table layout monitor has been created, but not yet started. */
      INITIALIZED,

      /** This instance monitor is started, and is currently monitoring the table. */
      STARTED,

      /** This instance monitor is closed. */
      CLOSED,
    }

    private final AtomicReference<State> mState = new AtomicReference<State>();

    /**
     * A reference to the latest {@link KijiTableLayout} for the table.  Updated automatically by a
     * ZooKeeper watcher when the layout is updated.
     */
    private final AtomicReference<KijiTableLayout> mLayout = new AtomicReference<KijiTableLayout>();

    /** Holds the set of LayoutConsumers who should be notified of layout updates. */
    private final Set<LayoutConsumer> mConsumers =
        Collections.newSetFromMap(new ConcurrentHashMap<LayoutConsumer, Boolean>());

    /**
     * Create a new table layout monitor for the provided user and table.
     *
     * @param tableURI of table being registered.
     * @param schemaTable of Kiji table.
     * @param metaTable of Kiji table.
     * @param zkClient ZooKeeper connection to register monitor with, or null if ZooKeeper is
     *        unavailable (SYSTEM_1_0).
     */
    public DefaultTableLayoutMonitor(
        KijiURI tableURI,
        KijiSchemaTable schemaTable,
        KijiMetaTable metaTable,
        CuratorFramework zkClient) {
      mTableURI = tableURI;
      mSchemaTable = schemaTable;
      mMetaTable = metaTable;
      if (zkClient == null) {
        mTableLayoutTracker = null;
        mUserRegistration = null;
        mInitializationLatch = null;
      } else {
        mUserRegistration = new TableUserRegistration(zkClient, tableURI, generateTableUserID());
        mInitializationLatch = new CountDownLatch(1);
        mTableLayoutTracker = new TableLayoutTracker(
            zkClient,
            mTableURI,
            new InnerLayoutUpdater(
                mUserRegistration,
                mInitializationLatch,
                mLayout,
                mTableURI,
                mConsumers,
                mMetaTable,
                mSchemaTable));
      }
      mState.compareAndSet(null, State.INITIALIZED);
    }

    /**
     * Start this table layout monitor.  Must be called before any other method.
     *
     * @return this table layout monitor.
     * @throws IOException on unrecoverable ZooKeeper or meta table error.
     */
    public TableLayoutMonitor start() throws IOException {
      Preconditions.checkState(
          mState.compareAndSet(State.INITIALIZED, State.STARTED),
          "Cannot start TableLayoutMonitor in state %s.", mState.get());
      if (mTableLayoutTracker != null) {
        mTableLayoutTracker.start();
        try {
          if (!mInitializationLatch.await(20, TimeUnit.SECONDS)) {
            throw new IOException("Timed-out while waiting for TableLayoutMonitor initialization."
                + " Check logs for details.");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeInterruptedException(e);
        }
      } else {
        final KijiTableLayout layout =
            mMetaTable.getTableLayout(mTableURI.getTable()).setSchemaTable(mSchemaTable);
        mLayout.set(layout);
      }
      return this;
    }

    @Override
    /** {@inheritDoc} */
    public void close() throws IOException {
      Preconditions.checkState(
          mState.compareAndSet(State.STARTED, State.CLOSED),
          "TableLayoutMonitor is not started.");
      ResourceUtils.closeOrLog(mUserRegistration);
      ResourceUtils.closeOrLog(mTableLayoutTracker);
      mLayout.set(null);
      mConsumers.clear();
    }

    @Override
    /** {@inheritDoc} */
    public KijiTableLayout getLayout() {
      Preconditions.checkState(mState.get() == State.STARTED,
          "TableLayoutMonitor has not been started.");
      return mLayout.get();
    }

    @Override
    /** {@inheritDoc} */
    public LayoutConsumer.Registration registerLayoutConsumer(LayoutConsumer consumer)
        throws IOException {
      Preconditions.checkState(mState.get() == State.STARTED,
          "TableLayoutMonitor has not been started.");
      mConsumers.add(consumer);
      consumer.update(getLayout());
      return new LayoutConsumerRegistration(mConsumers, consumer);
    }

    @Override
    /** {@inheritDoc} */
    public Set<LayoutConsumer> getLayoutConsumers() {
      Preconditions.checkState(mState.get() == State.STARTED,
          "TableLayoutMonitor has not been started.");
      return ImmutableSet.copyOf(mConsumers);
    }

    @Override
    /** {@inheritDoc} */
    public void updateLayoutConsumers(KijiTableLayout layout) throws IOException {
      Preconditions.checkState(mState.get() == State.STARTED,
          "TableLayoutMonitor has not been started.");
      layout.setSchemaTable(mSchemaTable);
      for (LayoutConsumer consumer : getLayoutConsumers()) {
        consumer.update(layout);
      }
    }

    /**
     * Generates a uniquely identifying ID for a table user.
     *
     * @return a uniquely identifying ID for a table user.
     */
    private static String generateTableUserID() {
      return String.format("%s;HBaseKijiTable@%s", JvmId.get(), TABLE_COUNTER.getAndIncrement());
    }

    /** {@inheritDoc}. */
    private static final class LayoutConsumerRegistration implements LayoutConsumer.Registration {
      private final Set<LayoutConsumer> mConsumers;
      private final LayoutConsumer mConsumer;

      /**
       * Create a Registration which will remove the consumer from the set of consumers upon close.
       * @param consumers set of consumers.
       * @param consumer to be removed from the set.
       */
      private LayoutConsumerRegistration(Set<LayoutConsumer> consumers, LayoutConsumer consumer) {
        mConsumers = consumers;
        mConsumer = consumer;
      }

      @Override
      /** {@inheritDoc} */
      public void close() throws IOException {
        mConsumers.remove(mConsumer);
      }
    }

    /**
     * Updates the layout of this table in response to a layout update pushed from ZooKeeper.
     */
    private static final class InnerLayoutUpdater implements TableLayoutUpdateHandler {

      private final TableUserRegistration mUserRegistration;

      private final CountDownLatch mInitializationLatch;

      private final AtomicReference<KijiTableLayout> mLayout;

      private final KijiURI mTableURI;

      private final Set<LayoutConsumer> mConsumers;

      private final KijiMetaTable mMetaTable;

      private final KijiSchemaTable mSchemaTable;

      /**
       * Create an InnerLayoutUpdater to update the layout of this table in response to a layout
       * node change in ZooKeeper.
       *
       * @param userRegistration ZooKeeper table user registration.
       * @param initializationLatch latch that will be counted down upon successful initialization.
       * @param layout layout reference to store most recent layout in.
       * @param tableURI URI of table whose layout is to be tracked.
       * @param consumers Set of layout consumers to notify on table layout update.
       * @param metaTable containing meta information.
       * @param schemaTable containing schema information.
       */
      private InnerLayoutUpdater(
          final TableUserRegistration userRegistration,
          final CountDownLatch initializationLatch,
          final AtomicReference<KijiTableLayout> layout,
          final KijiURI tableURI,
          final Set<LayoutConsumer> consumers,
          final KijiMetaTable metaTable,
          final KijiSchemaTable schemaTable
      ) {
        mUserRegistration = userRegistration;
        mInitializationLatch = initializationLatch;
        mLayout = layout;
        mTableURI = tableURI;
        mConsumers = consumers;
        mMetaTable = metaTable;
        mSchemaTable = schemaTable;
      }

      /** {@inheritDoc} */
      @Override
      public void update(final String notifiedLayoutID) {
        try {
          final String currentLayoutId =
              (mLayout.get() == null)
                  ? null
                  : mLayout.get().getDesc().getLayoutId();
          if (currentLayoutId == null) {
            LOG.debug(
                "Setting initial layout for table {} to layout ID {}.",
                mTableURI, notifiedLayoutID);
          } else {
            LOG.debug(
                "Updating layout for table {} from layout ID {} to layout ID {}.",
                mTableURI, currentLayoutId, notifiedLayoutID);
          }

          if (notifiedLayoutID == null) {
            LOG.warn(
                "Received a null layout update for table {}. Check the table metadata integrity.",
                mTableURI);
            mLayout.set(null);
            return;
          }

          final KijiTableLayout newLayout =
              mMetaTable.getTableLayout(mTableURI.getTable()).setSchemaTable(mSchemaTable);

          Preconditions.checkState(
              Objects.equal(newLayout.getDesc().getLayoutId(), notifiedLayoutID),
              "New layout ID %s does not match most recent layout ID %s from meta-table.",
              notifiedLayoutID, newLayout.getDesc().getLayoutId());

          mLayout.set(newLayout);

          // Propagates the new layout to all consumers. A copy of mConsumers is made in order to
          // avoid concurrent modifications while iterating. The contract of Guava's ImmutableSet
          // specifies that #copyOf is safe on concurrent collections.
          for (LayoutConsumer consumer : ImmutableSet.copyOf(mConsumers)) {
            consumer.update(mLayout.get());
          }

          // Registers this KijiTable in ZooKeeper as a user of the new table layout,
          // and unregisters as a user of the former table layout.
          if (currentLayoutId == null) {
            mUserRegistration.start(notifiedLayoutID);
          } else {
            mUserRegistration.updateLayoutID(notifiedLayoutID);
          }
        } catch (IOException e) {
          throw new KijiIOException(e);
        } finally {
          mInitializationLatch.countDown();
        }
      }
    }
  }

  /**
   * A TableLayoutMonitor which delegates to another table layout monitor for all operations, and
   * releases the table layout monitor from a reference counted cache on close.
   *
   * Public for testing purposes only.
   */
  public static final class ReferencedTableLayoutMonitor implements TableLayoutMonitor {
    private final TableLayoutMonitor mTableLayoutMonitor;
    private final String mTableName;
    private final ReferenceCountedCache<String, TableLayoutMonitor> mCache;

    /**
     * Get a referenced table layout monitor for a table from the cache.
     * @param tableName table whose layout monitor should be returned.
     * @param cache of table layout monitors.
     */
    public ReferencedTableLayoutMonitor(
        String tableName,
        ReferenceCountedCache<String, TableLayoutMonitor> cache) {
      mTableName = tableName;
      mCache = cache;
      mTableLayoutMonitor = mCache.get(mTableName);
    }

    /** {@inheritDoc} */
    @Override
    public KijiTableLayout getLayout() {
      return mTableLayoutMonitor.getLayout();
    }

    /** {@inheritDoc} */
    @Override
    public Registration registerLayoutConsumer(LayoutConsumer consumer) throws IOException {
      return mTableLayoutMonitor.registerLayoutConsumer(consumer);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      mCache.release(mTableName);
    }

    /** {@inheritDoc} */
    @Override
    public Set<LayoutConsumer> getLayoutConsumers() {
      return mTableLayoutMonitor.getLayoutConsumers();
    }

    /** {@inheritDoc} */
    @Override
    public void updateLayoutConsumers(KijiTableLayout layout) throws IOException {
      mTableLayoutMonitor.updateLayoutConsumers(layout);
    }
  }
}
