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

package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.Map;

import com.datastax.driver.core.exceptions.AlreadyExistsException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiAlreadyExistsException;
import org.kiji.schema.KijiInstaller;
import org.kiji.schema.KijiInvalidNameException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.cassandra.CassandraFactory;
import org.kiji.schema.hbase.HBaseFactory;
import org.kiji.schema.security.CassandraKijiSecurityManager;
import org.kiji.schema.util.ResourceUtils;

/** Installs or uninstalls Kiji instances from an Cassandra cluster. */
@ApiAudience.Public
@ApiStability.Evolving
public final class CassandraKijiInstaller extends KijiInstaller {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiInstaller.class);
  /** Singleton KijiInstaller. **/
  private static final CassandraKijiInstaller SINGLETON = new CassandraKijiInstaller();

  /** Constructs a CassandraKijiInstaller. */
  private CassandraKijiInstaller() {
    super();
  }

  /** {@inheritDoc} */
  @Override
  public void install(
      KijiURI uri,
      HBaseFactory hbaseFactory,
      Map<String, String> properties,
      Configuration conf
    ) throws IOException {
    final CassandraFactory cassandraFactory = CassandraFactory.Provider.get();
    if (uri.getInstance() == null) {
      throw new KijiInvalidNameException(String.format(
          "Kiji URI '%s' does not specify a Kiji instance name", uri));
    }

    try {
      LOG.info(String.format("Installing Cassandra Kiji instance '%s'.", uri));

      CassandraAdminFactory cassandraAdminFactory = cassandraFactory.getCassandraAdminFactory(uri);
      LOG.debug("Creating CassandraAdmin for Kiji installation.");
      CassandraAdmin cassandraAdmin = cassandraAdminFactory.create(uri);

      // Install the system, meta, and schema tables.
      CassandraSystemTable.install(cassandraAdmin, uri, properties);
      CassandraMetaTable.install(cassandraAdmin, uri);
      CassandraSchemaTable.install(cassandraAdmin, uri);

      // Grant the current user all privileges on the instance just created, if security is enabled.
      //final Kiji kiji = CassandraKijiFactory.get().open(uri, cassandraAdmin, lockFactory);
      final Kiji kiji = CassandraKijiFactory.get().open(uri);
      try {
        if (kiji.isSecurityEnabled()) {
          CassandraKijiSecurityManager.installInstanceCreator(uri, cassandraAdmin);
        }
      } finally {
        kiji.release();
      }

    } catch (AlreadyExistsException aee) {
      throw new KijiAlreadyExistsException(String.format(
          "Cassandra Kiji instance '%s' already exists.", uri), uri);
    }
    // TODO (SCHEMA-706): Add security checks when we have a plan for security in Cassandra Kiji.

    LOG.info(String.format("Installed Cassandra Kiji instance '%s'.", uri));
  }

  /** {@inheritDoc} */
  @Override
  public void uninstall(
      KijiURI uri,
      HBaseFactory hbaseFactory,
      Configuration conf
  ) throws IOException {
    if (uri.getInstance() == null) {
      throw new KijiInvalidNameException(String.format(
          "Kiji URI '%s' does not specify a Kiji instance name", uri));
    }
    final CassandraAdminFactory adminFactory =
        CassandraFactory.Provider.get().getCassandraAdminFactory(uri);

    LOG.info(String.format("Removing the Cassandra Kiji instance '%s'.", uri.getInstance()));

    final Kiji kiji = CassandraKijiFactory.get().open(uri);
    try {
      // TODO (SCHEMA-706): Add security checks when we have a plan for security in Cassandra Kiji.

      for (String tableName : kiji.getTableNames()) {
        LOG.info("Deleting Kiji table " + tableName + "...");
        kiji.deleteTable(tableName);
      }
      // Delete the user tables:
      final CassandraAdmin admin = adminFactory.create(uri);
      try {

        // Delete the system tables:
        CassandraSystemTable.uninstall(admin, uri);
        CassandraMetaTable.uninstall(admin, uri);
        CassandraSchemaTable.uninstall(admin, uri);

      } finally {
        ResourceUtils.closeOrLog(admin);
      }

      // Assert that there are no tables left and delete the keyspace.
      assert(admin.keyspaceIsEmpty());
      admin.deleteKeyspace();

    } finally {
      kiji.release();
    }
    LOG.info(String.format("Removed Cassandra Kiji instance '%s'.", uri.getInstance()));
  }

  /**
   * Gets an instance of a CassandraKijiInstaller.
   *
   * @return An instance of a CassandraKijiInstaller.
   */
  public static CassandraKijiInstaller get() {
    return SINGLETON;
  }
}
