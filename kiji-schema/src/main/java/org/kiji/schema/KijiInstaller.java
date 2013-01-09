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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.impl.HBaseAdminFactory;
import org.kiji.schema.impl.HBaseMetaTable;
import org.kiji.schema.impl.HBaseSchemaTable;
import org.kiji.schema.impl.HBaseSystemTable;
import org.kiji.schema.impl.HTableInterfaceFactory;
import org.kiji.schema.util.LockFactory;

/** Installs or uninstalls Kiji instances from an HBase cluster. */
@ApiAudience.Public
public final class KijiInstaller {
  private static final Logger LOG = LoggerFactory.getLogger(KijiInstaller.class);

  /**
   * Installs the specified Kiji instance.
   *
   * @param uri URI of the Kiji instance to install.
   * @param conf Hadoop configuration.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the Kiji instance name is invalid or already exists.
   */
  public static void install(KijiURI uri, Configuration conf)
      throws IOException, KijiInvalidNameException {
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
  public static void uninstall(KijiURI uri, Configuration conf)
      throws IOException, KijiInvalidNameException {
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
  public static void install(KijiURI uri, HBaseFactory hbaseFactory, Configuration conf)
      throws IOException, KijiInvalidNameException {
    if (uri.getInstance() == null) {
      throw new KijiInvalidNameException(String.format(
          "Kiji URI '%s' does not specify a Kiji instance name", uri));
    }
    final KijiConfiguration kijiConf = new KijiConfiguration(conf, uri);
    final HBaseAdminFactory adminFactory = hbaseFactory.getHBaseAdminFactory(uri);
    final HTableInterfaceFactory tableFactory = hbaseFactory.getHTableInterfaceFactory(uri);
    final LockFactory lockFactory = hbaseFactory.getLockFactory(uri, conf);

    final HBaseAdmin hbaseAdmin = adminFactory.create(kijiConf.getConf());
    try {
      if (kijiConf.exists(hbaseAdmin)) {
        throw new KijiAlreadyExistsException(String.format(
            "Kiji instance '%s' already exists.", uri), uri);
      }
      LOG.info(String.format("Installing kiji instance '%s'.", uri));
      HBaseSystemTable.install(hbaseAdmin, kijiConf, tableFactory);
      HBaseMetaTable.install(hbaseAdmin, kijiConf);
      HBaseSchemaTable.install(hbaseAdmin, kijiConf, tableFactory, lockFactory);

    } finally {
      IOUtils.closeQuietly(hbaseAdmin);
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
  public static void uninstall(KijiURI uri, HBaseFactory hbaseFactory, Configuration conf)
      throws IOException, KijiInvalidNameException {
    if (uri.getInstance() == null) {
      throw new KijiInvalidNameException(String.format(
          "Kiji URI '%s' does not specify a Kiji instance name", uri));
    }
    final KijiConfiguration kijiConf = new KijiConfiguration(conf, uri);
    final HBaseAdminFactory adminFactory = hbaseFactory.getHBaseAdminFactory(uri);
    final HTableInterfaceFactory tableFactory = hbaseFactory.getHTableInterfaceFactory(uri);
    final LockFactory lockFactory = hbaseFactory.getLockFactory(uri, conf);

    LOG.info(String.format("Removing the kiji instance '%s'.", uri.getInstance()));

    final Kiji kiji = new Kiji(kijiConf, true, tableFactory, lockFactory);
    try {
      // Delete the user tables:
      final HBaseAdmin hbaseAdmin = adminFactory.create(kijiConf.getConf());
      try {
        final KijiAdmin kijiAdmin = new KijiAdmin(hbaseAdmin, kiji);
        for (String tableName : kijiAdmin.getTableNames()) {
          LOG.debug("Deleting kiji table " + tableName + "...");
          kijiAdmin.deleteTable(tableName);
        }

        // Delete the system tables:
        HBaseSystemTable.uninstall(hbaseAdmin, kijiConf);
        HBaseMetaTable.uninstall(hbaseAdmin, kijiConf);
        HBaseSchemaTable.uninstall(hbaseAdmin, kijiConf);

      } finally {
        IOUtils.closeQuietly(hbaseAdmin);
      }
    } finally {
      IOUtils.closeQuietly(kiji);
    }
    LOG.info(String.format("Removed kiji instance '%s'.", uri.getInstance()));
  }

  /** Utility class may not be instantiated. */
  private KijiInstaller() {
  }
}
