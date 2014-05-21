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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.impl.TableLayoutMonitor.DefaultTableLayoutMonitor;
import org.kiji.schema.layout.impl.TableLayoutMonitor.ReferencedTableLayoutMonitor;
import org.kiji.schema.util.JvmId;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.schema.util.ReferenceCountedCache;
import org.kiji.schema.zookeeper.InstanceUserRegistration;

/**
 * A Kiji instance monitor. Registers a client as an instance user in ZooKeeper, and provides
 * table layout monitors.
 */
@ApiAudience.Private
public final class InstanceMonitor implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(InstanceMonitor.class);

  private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger(0);

  private final String mUserID;

  private final KijiURI mInstanceURI;

  private final KijiSchemaTable mSchemaTable;

  private final KijiMetaTable mMetaTable;

  private final CuratorFramework mZKClient;

  private final ReferenceCountedCache<String, TableLayoutMonitor> mTableLayoutMonitors;

  private final InstanceUserRegistration mUserRegistration;

  /** States of an instance monitor. */
  private static enum State {
    /** The instance monitor has been created, but not yet started. */
    INITIALIZED,

    /** This instance monitor is started, and is currently monitoring the instance. */
    OPEN,

    /** This instance monitor is closed. Stateful methods will fail. */
    CLOSED
  }

  private final AtomicReference<State> mState = new AtomicReference<State>();

  /**
   * Create an instance monitor for a Kiji instance.
   *
   * @param systemVersion of instance user.
   * @param instanceURI uri of instance to monitor.
   * @param schemaTable of instance.
   * @param metaTable of instance.
   * @param zkClient ZooKeeper connection to use for monitoring, or null if ZooKeeper is
   *        unavailable (SYSTEM_1_0).
   */
  public InstanceMonitor(
      final ProtocolVersion systemVersion,
      final KijiURI instanceURI,
      final KijiSchemaTable schemaTable,
      final KijiMetaTable metaTable,
      final CuratorFramework zkClient) {
    mUserID = generateInstanceUserID();
    mInstanceURI = instanceURI;
    mSchemaTable = schemaTable;
    mMetaTable = metaTable;
    mZKClient = zkClient;

    LOG.debug("Creating InstanceMonitor for instance {} with userID {}.", mInstanceURI, mUserID);

    mTableLayoutMonitors = ReferenceCountedCache.create(new TableLayoutMonitorFactoryFn());

    if (zkClient != null) {
      mUserRegistration =
          new InstanceUserRegistration(
              zkClient,
              instanceURI,
              mUserID,
              systemVersion.toCanonicalString());
    } else {
      mUserRegistration = null;
    }

    mState.compareAndSet(null, State.INITIALIZED);
  }

  /**
   * Get a table layout monitor for the provided table.  The returned table layout monitor should
   * be closed when no longer needed.
   *
   * @param tableName of table's monitor to retrieve.
   * @return a table layout monitor for the table.
   */
  public TableLayoutMonitor getTableLayoutMonitor(String tableName) {
    Preconditions.checkState(mState.get() == State.OPEN, "InstanceMonitor is closed.");
    LOG.debug("Retrieving TableLayoutMonitor for table {} with userID {}.",
        KijiURI.newBuilder(mInstanceURI).withTableName(tableName).build(), mUserID);
    return new ReferencedTableLayoutMonitor(tableName, mTableLayoutMonitors);
  }

  /**
   * Start this InstanceMonitor.  Must be called before any other method is valid.
   *
   * @return this.
   * @throws IOException on unrecoverable ZooKeeper exception.
   */
  public InstanceMonitor start() throws IOException {
    Preconditions.checkState(mState.compareAndSet(State.INITIALIZED, State.OPEN),
        "Cannot start instance monitor in state {}.", mState.get());
    if (mUserRegistration != null) {
      mUserRegistration.start();
    }
    return this;
  }

  /**
   * Close this InstanceMonitor.  Should be called when this instance monitor is no longer needed.
   * All table monitor objects owned by this instance monitor will also be closed.
   *
   * @throws IOException on unrecoverable ZooKeeper exception.
   */
  @Override
  public void close() throws IOException {
    mState.set(State.CLOSED);
    LOG.debug("Closing InstanceMonitor for instance {} with userID {}.", mInstanceURI, mUserID);

    if (mUserRegistration != null) {
      mUserRegistration.close();
    }
    mTableLayoutMonitors.close();
  }

  /**
   * Generates a uniquely identifying ID for an instance user.
   *
   * @return a uniquely identifying ID for an instance user.
   */
  private static String generateInstanceUserID() {
    return String.format("%s;HBaseKiji@%s", JvmId.get(), INSTANCE_COUNTER.getAndIncrement());
  }


  /**
   * A factory function for creating TableLayoutMonitor instances.
   */
  private final class TableLayoutMonitorFactoryFn implements Function<String, TableLayoutMonitor> {
    @Override
    /** {@inheritDoc} */
    public TableLayoutMonitor apply(String tableName) {
      final KijiURI tableURI = KijiURI.newBuilder(mInstanceURI).withTableName(tableName).build();
      try {
        return new DefaultTableLayoutMonitor(tableURI, mSchemaTable, mMetaTable, mZKClient).start();
      } catch (IOException e) {
        throw new KijiIOException(e);
      }
    }
  }
}
