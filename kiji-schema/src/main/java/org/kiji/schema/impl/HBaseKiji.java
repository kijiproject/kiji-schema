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

package org.kiji.schema.impl;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiAdmin;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.hbase.HBaseFactory;
import org.kiji.schema.util.LockFactory;
import org.kiji.schema.util.VersionInfo;
import org.kiji.schema.util.ZooKeeperLockFactory;

/**
 * Kiji instance class that contains configuration and table
 * information.  Multiple instances of Kiji can be installed onto a
 * single HBase cluster.  This class represents a single one of those
 * instances.
 */
@ApiAudience.Public
public final class HBaseKiji implements Kiji {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseKiji.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger(HBaseKiji.class.getName() + ".Cleanup");

  /** The kiji configuration. */
  private final KijiConfiguration mKijiConf;

  /** Factory for HTable instances. */
  private final HTableInterfaceFactory mHTableFactory;

  /** Factory for locks. */
  private final LockFactory mLockFactory;

  /** URI for this HBaseKiji instance. */
  private final KijiURI mURI;

  /** The schema table for this kiji instance, or null if it has not been opened yet. */
  private HBaseSchemaTable mSchemaTable;

  /** The system table for this kiji instance, or null if it has not been opened yet. */
  private HBaseSystemTable mSystemTable;

  /** The meta table for this kiji instance, or null if it has not been opened yet. */
  private HBaseMetaTable mMetaTable;

  /** Admin interface. */
  private HBaseKijiAdmin mAdmin;

  /** Whether the kiji instance is open. */
  private boolean mIsOpen;

  /** Retain counter. When decreased to 0, the HBase Kiji may be closed and disposed of. */
  private AtomicInteger mRetainCount = new AtomicInteger(1);

  /**
   * String representation of the call stack at the time this object is constructed.
   * Used for debugging
   **/
  private String mConstructorStack;

  /**
   * Creates a new <code>HBaseKiji</code> instance.
   * This should only be used by Kiji.Factory.open();
   *
   * @see org.kiji.schema.Kiji#open(KijiConfiguration)
   *
   * @param kijiConf The kiji configuration.
   * @throws IOException If there is an error.
   * @deprecated KijiConfiguration is going away. Use open(KijiURI, …).
   */
  @Deprecated
  HBaseKiji(KijiConfiguration kijiConf) throws IOException {
    this(kijiConf,
        true,
        DefaultHTableInterfaceFactory.get(),
        new ZooKeeperLockFactory(kijiConf.getConf()));
  }

  /**
   * Creates a new <code>HBaseKiji</code> instance.
   *
   * <p> Should only be used by Kiji.Factory.open().
   * <p> Caller does not need to use retain(), but must call release() when done with it.
   *
   * @param kijiConf Kiji configuration.
   * @param validateVersion Validate that the installed version of kiji is compatible with
   *     this client.
   * @param tableFactory HTableInterface factory.
   * @param lockFactory Factory for locks.
   * @throws IOException on I/O error.
   */
  HBaseKiji(
      KijiConfiguration kijiConf,
      boolean validateVersion,
      HTableInterfaceFactory tableFactory,
      LockFactory lockFactory)
      throws IOException {
    // Keep a deep copy of the kiji configuration.
    mKijiConf = new KijiConfiguration(kijiConf);
    mHTableFactory = Preconditions.checkNotNull(tableFactory);
    mLockFactory = Preconditions.checkNotNull(lockFactory);
    mURI = KijiURI.newBuilder(String.format("kiji://%s:%d/%s",
        mKijiConf.getConf().get(HConstants.ZOOKEEPER_QUORUM),
        mKijiConf.getConf().getInt(HConstants.ZOOKEEPER_CLIENT_PORT,
            HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT),
        mKijiConf.getName())).build();

    LOG.debug(String.format("Opening kiji instance '%s'", mURI));

    // Load these lazily.
    mSchemaTable = null;
    mSystemTable = null;
    mMetaTable = null;
    mAdmin = null;

    mIsOpen = true;

    checkHBaseConf();
    Preconditions.checkArgument(
        getConf().get("hbase.zookeeper.property.clientPort") != null,
        String.format(
            "Configuration for Kiji instance '%s' "
            + "lacks HBase resources (hbase-default.xml, hbase-site.xml), "
            + "use HBaseConfiguration.create().",
            mURI));

    if (validateVersion) {
      // Make sure the data version for the client matches the cluster.
      LOG.debug("Validating version...");
      VersionInfo.validateVersion(this);
    }

    if (CLEANUP_LOG.isDebugEnabled()) {
      try {
        throw new Exception();
      } catch (Exception e) {
        mConstructorStack = StringUtils.stringifyException(e);
      }
    }
    LOG.debug("Opened.");
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    return mKijiConf.getConf();
  }

  /** {@inheritDoc} */
  @Override
  public KijiURI getURI() {
    return mURI;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized KijiSchemaTable getSchemaTable() throws IOException {
    Preconditions.checkState(mIsOpen);
    if (null == mSchemaTable) {
      mSchemaTable = new HBaseSchemaTable(mKijiConf, mHTableFactory, mLockFactory);
    }
    return mSchemaTable;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized KijiSystemTable getSystemTable() throws IOException {
    Preconditions.checkState(mIsOpen);
    if (null == mSystemTable) {
      mSystemTable = new HBaseSystemTable(mKijiConf, mHTableFactory);
    }
    return mSystemTable;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized KijiMetaTable getMetaTable() throws IOException {
    Preconditions.checkState(mIsOpen);
    if (null == mMetaTable) {
      mMetaTable = new HBaseMetaTable(mKijiConf, getSchemaTable(), mHTableFactory);
    }
    return mMetaTable;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized KijiAdmin getAdmin() throws IOException {
    Preconditions.checkState(mIsOpen);
    if (null == mAdmin) {
      final HBaseFactory hbaseFactory = HBaseFactory.Provider.get();
      final HBaseAdmin hbaseAdmin = hbaseFactory.getHBaseAdminFactory(getURI()).create(getConf());
      mAdmin = new HBaseKijiAdmin(hbaseAdmin, this);
    }
    return mAdmin;
  }

  /** {@inheritDoc} */
  @Override
  public KijiTable openTable(String tableName) throws IOException {
    return new HBaseKijiTable(this, tableName, mKijiConf.getConf(), mHTableFactory);
  }

  /**
   * Releases all the resources used by this Kiji instance.
   *
   * @throws IOException on I/O error.
   */
  private void close() throws IOException {
    synchronized (this) {
      if (!mIsOpen) {
        LOG.warn("Called close() on a HBaseKiji instance that was already closed.");
        return;
      }
      mIsOpen = false;
    }

    LOG.debug(String.format("Closing Kiji instance '%s'.", mURI));
    IOUtils.closeQuietly(mMetaTable);
    IOUtils.closeQuietly(mSystemTable);
    IOUtils.closeQuietly(mSchemaTable);
    IOUtils.closeQuietly(mAdmin);
    mSchemaTable = null;
    mMetaTable = null;
    mSystemTable = null;
    mAdmin = null;
    LOG.debug(String.format("Kiji instance '%s' closed.", mURI));
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    if (mIsOpen) {
      CLEANUP_LOG.warn(String.format("Finalizing opened HBaseKiji '%s'.", mURI));
      if (CLEANUP_LOG.isDebugEnabled()) {
        CLEANUP_LOG.debug(String.format(
            "HBaseKiji '%s' was constructed through:%n%s",
            mURI, mConstructorStack));
      }
      close();
    }
    super.finalize();
  }

  /**
   * Checks that the user has added the HBase configuration to the Configuration options.
   * Failure to do so will cause version validation to fail with an obscure error message.
   */
  private void checkHBaseConf() {
  }

  /** {@inheritDoc} */
  @Override
  public synchronized Kiji retain() {
    mRetainCount.incrementAndGet();
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void release() throws IOException {
    final int counter = mRetainCount.decrementAndGet();
    Preconditions.checkState(counter >= 0, "Cannot release resource: retain counter is 0.");
    if (counter == 0) {
      close();
    }
  }
}
