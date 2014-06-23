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

package org.kiji.schema.impl.hbase;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiAlreadyExistsException;
import org.kiji.schema.KijiInstaller;
import org.kiji.schema.KijiInvalidNameException;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.hbase.HBaseFactory;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.impl.HBaseAdminFactory;
import org.kiji.schema.impl.HTableInterfaceFactory;
import org.kiji.schema.impl.Versions;
import org.kiji.schema.security.KijiSecurityManager;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.schema.zookeeper.UsersTracker;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

/** Installs or uninstalls Kiji instances from an HBase cluster. */
@ApiAudience.Private
public final class HBaseKijiInstaller extends KijiInstaller {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseKijiInstaller.class);
  /** Singleton KijiInstaller. **/
  private static final HBaseKijiInstaller SINGLETON = new HBaseKijiInstaller();

  /** Constructs an HBaseKijiInstaller. */
  private HBaseKijiInstaller() {
    super();
  }

  /** {@inheritDoc} */
  @Override
  public void install(
      final KijiURI uri,
      final HBaseFactory hbaseFactory,
      final Map<String, String> properties,
      final Configuration conf
  ) throws IOException {
    if (uri.getInstance() == null) {
      throw new KijiInvalidNameException(String.format(
          "Kiji URI '%s' does not specify a Kiji instance name", uri));
    }

    final HBaseAdminFactory adminFactory = hbaseFactory.getHBaseAdminFactory(uri);
    final HTableInterfaceFactory tableFactory = hbaseFactory.getHTableInterfaceFactory(uri);

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
      HBaseSystemTable.install(hbaseAdmin, uri, conf, properties, tableFactory);
      HBaseMetaTable.install(hbaseAdmin, uri);
      HBaseSchemaTable.install(hbaseAdmin, uri, conf, tableFactory);
      // Grant the current user all privileges on the instance just created, if security is enabled.
      final Kiji kiji = Kiji.Factory.open(uri, conf);
      try {
        if (kiji.isSecurityEnabled()) {
          KijiSecurityManager.Installer.installInstanceCreator(uri, conf, tableFactory);
        }
      } finally {
        kiji.release();
      }
    } finally {
      ResourceUtils.closeOrLog(hbaseAdmin);
    }
    LOG.info(String.format("Installed kiji instance '%s'.", uri));
  }

  /** {@inheritDoc} */
  @Override
  public void uninstall(
      final KijiURI uri,
      final HBaseFactory hbaseFactory,
      final Configuration conf
  ) throws IOException {
    if (uri.getInstance() == null) {
      throw new KijiInvalidNameException(String.format(
          "Kiji URI '%s' does not specify a Kiji instance name", uri));
    }
    final HBaseAdminFactory adminFactory = hbaseFactory.getHBaseAdminFactory(uri);
    final HTableInterfaceFactory htableFactory = hbaseFactory.getHTableInterfaceFactory(uri);

    // TODO: Factor this in HBaseKiji
    conf.set(HConstants.ZOOKEEPER_QUORUM, Joiner.on(",").join(uri.getZookeeperQuorumOrdered()));
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());

    LOG.info(String.format("Removing the kiji instance '%s'.", uri.getInstance()));

    final ProtocolVersion systemVersion = getSystemVersion(uri, conf, htableFactory);
    if (systemVersion.compareTo(Versions.SYSTEM_2_0) < 0) {
      uninstallSystem_1_0(uri, conf, adminFactory);
    } else if (systemVersion.compareTo(Versions.SYSTEM_2_0) == 0) {
      uninstallSystem_2_0(uri, conf, adminFactory);
    } else {
      throw new InternalKijiError(String.format("Unknown System version %s.", systemVersion));
    }

    LOG.info("Removed kiji instance '{}'.", uri.getInstance());
  }

  // CSOFF: MethodName
  /**
   * Uninstall a Kiji SYSTEM_1_0 instance.
   *
   * @param uri of instance.
   * @param conf configuration to connect to instance.
   * @param adminFactory to connect to instance.
   * @throws java.io.IOException on unrecoverable error.
   */
  private static void uninstallSystem_1_0(
      final KijiURI uri,
      final Configuration conf,
      final HBaseAdminFactory adminFactory
  ) throws IOException {
    final Kiji kiji = new HBaseKijiFactory().open(uri, conf);
    try {
      // If security is enabled, make sure the user has GRANT access on the instance
      // before uninstalling.
      if (kiji.isSecurityEnabled()) {
        KijiSecurityManager securityManager = kiji.getSecurityManager();
        try {
          securityManager.checkCurrentGrantAccess();
        } finally {
          securityManager.close();
        }
      }

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
        hbaseAdmin.close();
      }
    } finally {
      kiji.release();
    }
  }

  /**
   * Uninstall a Kiji SYSTEM_2_0 instance.
   *
   * @param uri of instance.
   * @param conf configuration to connect to instance.
   * @param adminFactory to connect to instance.
   * @throws java.io.IOException on unrecoverable error.
   */
  private static void uninstallSystem_2_0(
      final KijiURI uri,
      final Configuration conf,
      final HBaseAdminFactory adminFactory
  ) throws IOException {
    final CuratorFramework zkClient = ZooKeeperUtils.getZooKeeperClient(uri);
    final String instanceZKPath = ZooKeeperUtils.getInstanceDir(uri).getPath();
    try {
      final UsersTracker usersTracker = ZooKeeperUtils.newInstanceUsersTracker(zkClient, uri);
      try {
        usersTracker.start();
        final Set<String> users = usersTracker.getUsers().keySet();
        if (!users.isEmpty()) {
          LOG.error(
              "Uninstalling Kiji instance '{}' with registered users."
                  + " Current registered users: {}. Stale instance metadata will remain in"
                  + " ZooKeeper at path {}.", uri.getInstance(), users, instanceZKPath);
        }
      } finally {
        usersTracker.close();
      }

      // The uninstall of tables from HBase is the same as System_1_0
      uninstallSystem_1_0(uri, conf, adminFactory);

      // Try to delete instance ZNodes from ZooKeeper
      ZooKeeperUtils.atomicRecursiveDelete(zkClient, instanceZKPath);
    } finally {
      zkClient.close();
    }
  }
  // CSON

  /**
   * Get the system version of an installed Kiji instance.
   *
   * @param instanceURI of Kiji instance.
   * @param conf to connect to instance.
   * @param htableFactory to connect to instance.
   * @return the system version.
   * @throws java.io.IOException if unrecoverable error.
   */
  private static ProtocolVersion getSystemVersion(
      final KijiURI instanceURI,
      final Configuration conf,
      final HTableInterfaceFactory htableFactory
  ) throws IOException {
    final KijiSystemTable systemTable = new HBaseSystemTable(instanceURI, conf, htableFactory);
    try {
      return systemTable.getDataVersion();
    } finally {
      systemTable.close();
    }
  }

  /**
   * Gets an instance of a KijiInstaller.
   *
   * @return An instance of a KijiInstaller.
   */
  public static HBaseKijiInstaller get() {
    return SINGLETON;
  }
}
