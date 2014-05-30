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

package org.kiji.schema.impl.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.RuntimeInterruptedException;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.TableLayoutUpdateValidator;
import org.kiji.schema.util.Lock;
import org.kiji.schema.util.Time;
import org.kiji.schema.zookeeper.TableLayoutTracker;
import org.kiji.schema.zookeeper.TableLayoutUpdateHandler;
import org.kiji.schema.zookeeper.UsersTracker;
import org.kiji.schema.zookeeper.UsersUpdateHandler;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

/**
 * Updates the layout of an HBase Kiji table.
 *
 * <p>
 *   The parameters of the updater include a function to compute a layout update given the
 *   current layout of the table. This function may be invoked several times during the update
 *   process. For instance, it may be invoked first to pre-validate the update, and then a second
 *   time after the table layout lock has been acquired, to re-validate the layout update.
 * </p>
 */
public class HBaseTableLayoutUpdater {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseTableLayoutUpdater.class);

  private final HBaseKiji mKiji;
  private final KijiURI mTableURI;
  private final CuratorFramework mZKClient;

  private final UpdaterUsersUpdateHandler mUsersUpdateHandler = new UpdaterUsersUpdateHandler();
  private final UpdaterLayoutUpdateHandler mLayoutUpdateHandler = new UpdaterLayoutUpdateHandler();

  /**  */
  private final Function<KijiTableLayout, TableLayoutDesc> mLayoutUpdate;

  /** New table layout, set after the layout update completed. */
  private KijiTableLayout mNewLayout = null;

  // -----------------------------------------------------------------------------------------------

  /** Handles update notifications of the users list of the table. */
  private final class UpdaterUsersUpdateHandler implements UsersUpdateHandler {
    /** Monitor for table users notifications. */
    private final Object mLock = new Object();

    /** Map: user ID -> layout ID. */
    private Multimap<String, String> mUserMap = null;

    /** {@inheritDoc} */
    @Override
    public void update(Multimap<String, String> userMap) {
      LOG.debug("Layout updater received user map update for table {}: {}.",
          mTableURI, userMap);
      synchronized (mLock) {
        mUserMap = userMap;
        mLock.notifyAll();
      }
    }

    /**
     * Waits for all users of the table to have a consistent view on the table layout.
     *
     * @return the layout ID as seen consistently by all users.
     */
    public String waitForConsistentView() {
      synchronized (mLock) {
        while (true) {
          if (mUserMap != null) {
            final Map<String, List<String>> mLayoutMap = Maps.newHashMap();
            for (Map.Entry<String, String> entry : mUserMap.entries()) {
              final String userId = entry.getKey();
              final String layoutId = entry.getValue();
              List<String> userIds = mLayoutMap.get(layoutId);
              if (null == userIds) {
                userIds = Lists.newArrayList();
                mLayoutMap.put(layoutId, userIds);
              }
              userIds.add(userId);
            }
            LOG.info("User map for table {}: {}", mTableURI, mLayoutMap);
            switch (mLayoutMap.size()) {
              case 0: return null;
              case 1: return mLayoutMap.keySet().iterator().next();
              default: break;
            }
          } else {
            LOG.debug("Waiting for table users notification.");
          }
          try {
            mLock.wait();
          } catch (InterruptedException ie) {
            throw new RuntimeInterruptedException(ie);
          }
        }
      }
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Handles update notifications of the table layout. */
  private final class UpdaterLayoutUpdateHandler implements TableLayoutUpdateHandler {
    /** Monitor for table layout notifications. */
    private final Object mLock = new Object();

    /** Current layout. */
    private String mCurrentLayoutId = null;

    /** {@inheritDoc} */
    @Override
    public void update(String layout) {
      synchronized (mLock) {
        mCurrentLayoutId = layout;
        LOG.debug("Layout updater received layout update for table {}: {}.",
            mTableURI, mCurrentLayoutId);
        mLock.notifyAll();
      }
    }

    /**
     * Reports the ID of the current table layout.
     *
     * @return the ID of the current table layout.
     */
    public String getCurrentLayoutId() {
      synchronized (mLock) {
        while (null == mCurrentLayoutId) {
          try {
            mLock.wait();
          } catch (InterruptedException ie) {
            throw new RuntimeInterruptedException(ie);
          }
        }
        return mCurrentLayoutId;
      }
    }

    /**
     * Waits for the current table layout to switch to the specified layout ID.
     *
     * @param layoutId ID of the layout to wait for.
     */
    public void waitForLayoutNotification(String layoutId) {
      synchronized (mLock) {
        while (!Objects.equal(getCurrentLayoutId(), layoutId)) {
          LOG.info("Waiting for layout notification with ID {}, current layout ID is {}.",
              layoutId, mCurrentLayoutId);
          try {
            mLock.wait();
          } catch (InterruptedException ie) {
            throw new RuntimeInterruptedException(ie);
          }
        }
      }
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Initializes a new layout updater for the specified table and with the specified layout update.
   *
   * @param kiji Opened Kiji instance the table belongs to.
   * @param tableURI Update the layout of this table.
   * @param layoutUpdate Function to generate the layout update descriptor based on the current
   *     layout of the table.
   * @throws IOException on I/O error.
   * @throws KeeperException on ZooKeeper error.
   */
  public HBaseTableLayoutUpdater(
      final HBaseKiji kiji,
      final KijiURI tableURI,
      final Function<KijiTableLayout, TableLayoutDesc> layoutUpdate)
      throws IOException, KeeperException {
    mKiji = kiji;
    mKiji.retain();
    mTableURI = tableURI;
    mZKClient = mKiji.getZKClient();
    mLayoutUpdate = layoutUpdate;
  }

  /**
   * Initializes a new layout updater for the specified table and with the specified layout update.
   *
   * @param kiji Opened Kiji instance the table belongs to.
   * @param tableURI Update the layout of this table.
   * @param layoutUpdate Static layout update descriptor to update the table with.
   * @throws IOException on I/O error.
   * @throws KeeperException on ZooKeeper error.
   */
  public HBaseTableLayoutUpdater(
      final HBaseKiji kiji,
      final KijiURI tableURI,
      final TableLayoutDesc layoutUpdate)
      throws IOException, KeeperException {
    this(kiji, tableURI, new Function<KijiTableLayout, TableLayoutDesc>() {
      /** {@inheritDoc} */
      @Override
      public TableLayoutDesc apply(KijiTableLayout input) {
        return layoutUpdate;
      }
    });
  }

  /**
   * Releases the resources maintained by this updater.
   *
   * @throws IOException on I/O error.
   */
  public void close() throws IOException {
    mKiji.release();
  }

  /**
   * Performs the specified table layout update.
   *
   * @throws IOException on I/O error.
   * @throws KeeperException on ZooKeeper error.
   */
  public void update() throws IOException, KeeperException {
    final KijiMetaTable metaTable = mKiji.getMetaTable();

    final Lock lock = ZooKeeperUtils.newTableLayoutLock(mZKClient, mTableURI);
    lock.lock();
    try {
      final NavigableMap<Long, KijiTableLayout> layoutMap =
          metaTable.getTimedTableLayoutVersions(mTableURI.getTable(), Integer.MAX_VALUE);

      final KijiTableLayout currentLayout = layoutMap.lastEntry().getValue();
      final TableLayoutDesc update = mLayoutUpdate.apply(currentLayout);
      if (!Objects.equal(currentLayout.getDesc().getLayoutId(), update.getReferenceLayout())) {
        throw new InvalidLayoutException(String.format(
            "Reference layout ID %s does not match current layout ID %s.",
            update.getReferenceLayout(), currentLayout.getDesc().getLayoutId()));
      }

      final TableLayoutUpdateValidator validator = new TableLayoutUpdateValidator(mKiji);
      validator.validate(
          currentLayout,
          KijiTableLayout.createUpdatedLayout(update , currentLayout));

      final TableLayoutTracker layoutTracker =
          new TableLayoutTracker(mZKClient, mTableURI, mLayoutUpdateHandler);
      try {
        layoutTracker.start();
        final UsersTracker usersTracker =
            ZooKeeperUtils
                .newTableUsersTracker(mZKClient, mTableURI)
                .registerUpdateHandler(mUsersUpdateHandler);
        try {
          usersTracker.start();
          final String currentLayoutId = mLayoutUpdateHandler.getCurrentLayoutId();
          LOG.info("Table {} has current layout ID {}.", mTableURI, currentLayoutId);
          if (!Objects.equal(currentLayoutId, currentLayout.getDesc().getLayoutId())) {
            throw new InternalKijiError(String.format(
                "Inconsistency between meta-table and ZooKeeper: "
                + "meta-table layout has ID %s while ZooKeeper has layout ID %s.",
                currentLayout.getDesc().getLayoutId(), currentLayoutId));
          }

          final String consistentLayoutId = waitForConsistentView();
          if ((consistentLayoutId != null) && !Objects.equal(consistentLayoutId, currentLayoutId)) {
            throw new InternalKijiError(String.format(
                "Consistent layout ID %s does not match current layout %s for table %s.",
                consistentLayoutId, currentLayout, mTableURI));
          }

          writeMetaTable(update);
          final TableLayoutDesc newLayoutDesc = mNewLayout.getDesc();
          writeZooKeeper(newLayoutDesc);

          mLayoutUpdateHandler.waitForLayoutNotification(newLayoutDesc.getLayoutId());

          // The following is not necessary:
          while (true) {
            final String newLayoutId = waitForConsistentView();
            if (newLayoutId == null) {
              LOG.info("Layout update complete for table {}: table has no users.", mTableURI);
              break;
            } else if (Objects.equal(newLayoutId, newLayoutDesc.getLayoutId())) {
              LOG.info("Layout update complete for table {}: all users switched to layout ID {}.",
                  mTableURI, newLayoutId);
              break;
            } else {
              LOG.info("Layout update in progress for table {}: users still using layout ID {}.",
                  mTableURI, newLayoutId);
              Time.sleep(1.0);
            }
          }

        } finally {
          usersTracker.close();
        }
      } finally {
        layoutTracker.close();
      }
    } finally {
      lock.unlock();
      lock.close();
    }
  }

  /**
   * Waits for all clients of the table to have a consistent view on the table layout.
   *
   * @return the layout ID being used consistently by all users, or null if no users.
   * @throws IOException on I/O error.
   */
  private String waitForConsistentView() throws IOException {
    return mUsersUpdateHandler.waitForConsistentView();
  }

  /**
   * Writes the new table layout to the meta-table.
   *
   * @param update Layout update to write to the meta-table.
   * @throws IOException on I/O error.
   */
  private void writeMetaTable(TableLayoutDesc update) throws IOException {
    LOG.info("Updating layout for table {} from layout ID {} to layout ID {} in meta-table.",
        mTableURI, update.getReferenceLayout(), update.getLayoutId());
    final String table = update.getName();
    mNewLayout = mKiji.getMetaTable().updateTableLayout(table, update);
  }

  /**
   * Writes the new layout to ZooKeeper.
   *
   * <p> This pushes a layout update to all table users. </p>
   *
   * @param update Layout update to push to ZooKeeper.
   *
   * @throws IOException on I/O error.
   * @throws KeeperException on ZooKeeper error.
   */
  private void writeZooKeeper(TableLayoutDesc update) throws IOException, KeeperException {
    LOG.info("Updating layout for table {} from layout ID {} to layout ID {} in ZooKeeper.",
        mTableURI, update.getReferenceLayout(), update.getLayoutId());
    ZooKeeperUtils.setTableLayout(mZKClient, mTableURI, update.getLayoutId());
  }

  /**
   * Returns the new layout, after it has been applied to the table.
   *
   * @return the new layout, after it has been applied to the table.
   *     Null before the update completes.
   */
  public KijiTableLayout getNewLayout() {
    return mNewLayout;
  }
}
