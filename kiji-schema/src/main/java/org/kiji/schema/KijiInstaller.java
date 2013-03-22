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

import java.io.IOException;

import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.hbase.HBaseFactory;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.impl.HBaseAdminFactory;
import org.kiji.schema.impl.HBaseMetaTable;
import org.kiji.schema.impl.HBaseSchemaTable;
import org.kiji.schema.impl.HBaseSystemTable;
import org.kiji.schema.impl.HTableInterfaceFactory;
import org.kiji.schema.util.LockFactory;
import org.kiji.schema.util.ResourceUtils;

/** Installs or uninstalls Kiji instances from an HBase cluster. */
@ApiAudience.Public
@ApiStability.Evolving
public final class KijiInstaller {
  private static final Logger LOG = LoggerFactory.getLogger(KijiInstaller.class);

  /** Singleton KijiInstaller. **/
  private static final KijiInstaller SINGLETON = new KijiInstaller();

  /** Constructs a KijiInstaller. */
  private KijiInstaller() {
  }

  /**
   * Installs the specified Kiji instance.
   *
   * @param uri URI of the Kiji instance to install.
   * @param conf Hadoop configuration.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the Kiji instance name is invalid or already exists.
   */
  public void install(KijiURI uri, Configuration conf) throws IOException {
    install(uri, HBaseFactory.Provider.get(), conf);
  }

  /**
   * Uninstalls the specified Kiji instance.
   *
   * @param uri URI of the Kiji instance to uninstall.
   * @param conf Hadoop configuration.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the instance name is invalid or already exists.
   */
  public void uninstall(KijiURI uri, Configuration conf) throws IOException {
    uninstall(uri, HBaseFactory.Provider.get(), conf);
  }

  /**
   * Installs a Kiji instance.
   *
   * @param uri URI of the Kiji instance to install.
   * @param hbaseFactory Factory for HBase instances.
   * @param conf Hadoop configuration.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the instance name is invalid or already exists.
   */
  public void install(KijiURI uri, HBaseFactory hbaseFactory, Configuration conf)
      throws IOException {
    if (uri.getInstance() == null) {
      throw new KijiInvalidNameException(String.format(
          "Kiji URI '%s' does not specify a Kiji instance name", uri));
    }

    final HBaseAdminFactory adminFactory = hbaseFactory.getHBaseAdminFactory(uri);
    final HTableInterfaceFactory tableFactory = hbaseFactory.getHTableInterfaceFactory(uri);
    final LockFactory lockFactory = hbaseFactory.getLockFactory(uri, conf);

    // TODO: Factor this in HBaseKiji
    conf.set(HConstants.ZOOKEEPER_QUORUM, Joiner.on(",").join(uri.getZookeeperQuorumOrdered()));
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());

    final HBaseAdmin hbaseAdmin = adminFactory.create(conf);
    try {
      if (hbaseAdmin.tableExists(
          KijiManagedHBaseTableName.getSystemTableName(uri.getInstance()).toString())) {
        throw new KijiAlreadyExistsException(String.format(
            "Kiji instance '%s' already exists.", uri), uri);
      }
      LOG.info(String.format("Installing kiji instance '%s'.", uri));
      HBaseSystemTable.install(hbaseAdmin, uri, conf, tableFactory);
      HBaseMetaTable.install(hbaseAdmin, uri);
      HBaseSchemaTable.install(hbaseAdmin, uri, conf, tableFactory, lockFactory);

    } finally {
      ResourceUtils.closeOrLog(hbaseAdmin);
    }
    LOG.info(String.format("Installed kiji instance '%s'.", uri));
  }

  /**
   * Removes a kiji instance from the HBase cluster including any user tables.
   *
   * @param uri URI of the Kiji instance to install.
   * @param hbaseFactory Factory for HBase instances.
   * @param conf Hadoop configuration.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the instance name is invalid.
   * @throws KijiNotInstalledException if the specified instance does not exist.
   */
  public void uninstall(KijiURI uri, HBaseFactory hbaseFactory, Configuration conf)
      throws IOException {
    if (uri.getInstance() == null) {
      throw new KijiInvalidNameException(String.format(
          "Kiji URI '%s' does not specify a Kiji instance name", uri));
    }
    final HBaseAdminFactory adminFactory = hbaseFactory.getHBaseAdminFactory(uri);

    // TODO: Factor this in HBaseKiji
    conf.set(HConstants.ZOOKEEPER_QUORUM, Joiner.on(",").join(uri.getZookeeperQuorumOrdered()));
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());

    LOG.info(String.format("Removing the kiji instance '%s'.", uri.getInstance()));

    final Kiji kiji = Kiji.Factory.open(uri, conf);
    try {
      for (String tableName : kiji.getTableNames()) {
        LOG.debug("Deleting kiji table " + tableName + "...");
        kiji.deleteTable(tableName);
      }

      // Delete the user tables:
      final HBaseAdmin hbaseAdmin = adminFactory.create(conf);
      try {

        // Delete the system tables:
        HBaseSystemTable.uninstall(hbaseAdmin, uri);
        HBaseMetaTable.uninstall(hbaseAdmin, uri);
        HBaseSchemaTable.uninstall(hbaseAdmin, uri);

      } finally {
        ResourceUtils.closeOrLog(hbaseAdmin);
      }
    } finally {
      kiji.release();
    }
    LOG.info(String.format("Removed kiji instance '%s'.", uri.getInstance()));
  }

  /**
   * Gets an instance of a KijiInstaller.
   *
   * @return An instance of a KijiInstaller.
   */
  public static KijiInstaller get() {
    return SINGLETON;
  }
}
