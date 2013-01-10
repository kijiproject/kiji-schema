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

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.impl.DefaultHTableInterfaceFactory;
import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.impl.HBaseMetaTable;
import org.kiji.schema.impl.HBaseSchemaTable;
import org.kiji.schema.impl.HBaseSystemTable;
import org.kiji.schema.impl.HTableInterfaceFactory;
import org.kiji.schema.util.LockFactory;
import org.kiji.schema.util.VersionInfo;
import org.kiji.schema.util.ZooKeeperLockFactory;

/**
 * <p>Kiji instance class that contains configuration and table
 * information.  Multiple instances of Kiji can be installed onto a
 * single HBase cluster.  This class represents a single one of those
 * instances.</p>
 *
 * <p>An installed Kiji instance contains several tables:
 *   <ul>
 *     <li>System table: Kiji system and version information.</li>
 *     <li>Meta table: Metadata about the existing Kiji tables.</li>
 *     <li>Schema table: Avro Schemas of the cells in Kiji tables.</li>
 *     <li>User tables: The user-space Kiji tables that you create.</li>
 *   </ul>
 * </p>
 * <p>The default Kiji instance name is <em>default</em>.</p>
 */
@ApiAudience.Public
public class Kiji implements KijiTableFactory, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Kiji.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger(Kiji.class.getName() + ".Cleanup");

  /** The kiji configuration. */
  private final KijiConfiguration mKijiConf;

  /** Factory for HTable instances. */
  private final HTableInterfaceFactory mHTableFactory;

  /** Factory for locks. */
  private final LockFactory mLockFactory;

  /** URI for this Kiji instance. */
  private final KijiURI mURI;

  /** The schema table for this kiji instance, or null if it has not been opened yet. */
  private KijiSchemaTable mSchemaTable;

  /** The system table for this kiji instance, or null if it has not been opened yet. */
  private KijiSystemTable mSystemTable;

  /** The meta table for this kiji instance, or null if it has not been opened yet. */
  private KijiMetaTable mMetaTable;

  /** Whether the kiji instance is open. */
  private boolean mIsOpen;

  /**
   * String representation of the call stack at the time this object is constructed.
   * Used for debugging
   **/
  private String mConstructorStack;

  /**
   * Creates a new <code>Kiji</code> instance.
   * This should only be used by Kiji.open();
   *
   * @see org.kiji.schema.Kiji#open(KijiConfiguration)
   *
   * @param kijiConf The kiji configuration.
   * @throws IOException If there is an error.
   */
  public Kiji(KijiConfiguration kijiConf) throws IOException {
    this(kijiConf,
        true,
        DefaultHTableInterfaceFactory.get(),
        new ZooKeeperLockFactory(kijiConf.getConf()));
  }

  /**
   * Creates a new <code>Kiji</code> instance.
   *
   * Should only be used by Kiji.open().
   *
   * @param kijiConf Kiji configuration.
   * @param validateVersion Validate that the installed version of kiji is compatible with
   *     this client.
   * @param tableFactory HTableInterface factory.
   * @param lockFactory Factory for locks.
   * @throws IOException on I/O error.
   */
  public Kiji(
      KijiConfiguration kijiConf,
      boolean validateVersion,
      HTableInterfaceFactory tableFactory,
      LockFactory lockFactory)
      throws IOException {
    // Keep a deep copy of the kiji configuration.
    mKijiConf = new KijiConfiguration(kijiConf);
    mHTableFactory = Preconditions.checkNotNull(tableFactory);
    mLockFactory = Preconditions.checkNotNull(lockFactory);
    mURI = KijiURI.parse(String.format("kiji://%s:%d/%s",
        mKijiConf.getConf().get(HConstants.ZOOKEEPER_QUORUM),
        mKijiConf.getConf().getInt(HConstants.ZOOKEEPER_CLIENT_PORT,
            HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT),
        mKijiConf.getName()));

    LOG.debug(String.format("Opening kiji instance '%s'", mURI));

    // Load these lazily.
    mSchemaTable = null;
    mSystemTable = null;
    mMetaTable = null;

    checkHBaseConf();

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
    mIsOpen = true;
    LOG.debug("Opened.");
  }

  /**
   * Opens a Kiji instance.
   *
   * @param kijiConf The configuration.
   * @return An opened kiji instance.
   * @throws IOException If there is an error.
   */
  public static Kiji open(KijiConfiguration kijiConf) throws IOException {
    return new Kiji(kijiConf);
  }

  /**
   * Opens a Kiji instance by URI.
   *
   * @param uri URI specifying the Kiji instance to open.
   * @param conf Hadoop configuration.
   * @return the specified Kiji instance.
   * @throws IOException on I/O error.
   */
  public static Kiji open(KijiURI uri, Configuration conf) throws IOException {
    final HBaseFactory hbaseFactory = HBaseFactory.Provider.get();
    final Configuration confCopy = new Configuration(conf);
    final KijiConfiguration kijiConf = new KijiConfiguration(confCopy, uri);
    return new Kiji(
        kijiConf,
        true,
        hbaseFactory.getHTableInterfaceFactory(uri),
        hbaseFactory.getLockFactory(uri, confCopy));
  }

  /**
   * Opens a Kiji instance by URI.
   *
   * @param uri URI specifying the Kiji instance to open.
   * @return the specified Kiji instance.
   * @throws IOException on I/O error.
   */
  public static Kiji open(KijiURI uri) throws IOException {
    return open(uri, HBaseConfiguration.create());
  }

  /**
   * Gets the name of the kiji instance.
   *
   * @return The name of the kiji instance.
   */
  public String getName() {
    return mKijiConf.getName();
  }

  /** @return the URI for this Kiji instance. */
  public KijiURI getURI() {
    return mURI;
  }

  /**
   * Gets the kiji instance configuration.
   *
   * @return The kiji configuration.
   */
  public KijiConfiguration getKijiConf() {
    return mKijiConf;
  }

  /**
   * Gets the hadoop configuration.
   *
   * @return The hadoop configuration.
   */
  public Configuration getConf() {
    return mKijiConf.getConf();
  }

  /**
   * Checks that the user has added the HBase configuration to the Configuration options.
   * Failure to do so will cause version validation to fail with an obscure error message.
   */
  private void checkHBaseConf() {
    if (null == getConf().get("hbase.zookeeper.property.clientPort")) {
      // Doesn't appear so.
      throw new RuntimeException("Configuration passed to Kiji.open() does not appear "
          + "to contain HBase resources (hbase-default.xml, hbase-site.xml).\n"
          + "Try using HBaseConfiguration.create().");
    }
  }

  /**
   * Gets the schema table for this Kiji instance.
   *
   * @return The kiji schema table.
   * @throws IOException If there is an error.
   */
  public synchronized KijiSchemaTable getSchemaTable() throws IOException {
    if (null == mSchemaTable) {
      mSchemaTable = new HBaseSchemaTable(getKijiConf(), mHTableFactory, mLockFactory);
    }
    return mSchemaTable;
  }

  /**
   * Gets the system table for this Kiji instance.
   *
   * @return The kiji system table.
   * @throws IOException If there is an error.
   */
  public synchronized KijiSystemTable getSystemTable() throws IOException {
    if (null == mSystemTable) {
      mSystemTable = new HBaseSystemTable(getKijiConf(), mHTableFactory);
    }
    return mSystemTable;
  }

  /**
   * Gets the meta table for this Kiji instance.
   *
   * @return The kiji meta table.
   * @throws IOException If there is an error.
   */
  public synchronized KijiMetaTable getMetaTable() throws IOException {
    if (null == mMetaTable) {
      mMetaTable = new HBaseMetaTable(getKijiConf(), getSchemaTable(), mHTableFactory);
    }
    return mMetaTable;
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    if (!mIsOpen) {
      LOG.warn("Called close() on a Kiji instance that was already closed.");
      return;
    }
    mIsOpen = false;

    LOG.debug("Closing kiji...");
    IOUtils.closeQuietly(mMetaTable);
    IOUtils.closeQuietly(mSystemTable);
    IOUtils.closeQuietly(mSchemaTable);

    LOG.debug("Kiji closed.");
  }

  @Override
  protected void finalize() throws Throwable {
    if (mIsOpen) {
      CLEANUP_LOG.warn("Closing a Kiji instance in finalize(). You should close it explicitly.");
      if (CLEANUP_LOG.isDebugEnabled()) {
        CLEANUP_LOG.debug("Call stack when this Kiji was constructed: ");
        CLEANUP_LOG.debug(mConstructorStack);
      }
      close();
    }
    super.finalize();
  }

  /** {@inheritDoc} */
  @Override
  public KijiTable openTable(String tableName) throws IOException {
    return new HBaseKijiTable(this, tableName, mHTableFactory);
  }

  /**
   * @return a KijiAdmin for this Kiji instance.
   * @throws IOException on I/O error.
   */
  public KijiAdmin getAdmin() throws IOException {
    final HBaseFactory hbaseFactory = HBaseFactory.Provider.get();
    final HBaseAdmin hbaseAdmin = hbaseFactory.getHBaseAdminFactory(getURI()).create(getConf());
    return new KijiAdmin(hbaseAdmin, this);
  }
}
