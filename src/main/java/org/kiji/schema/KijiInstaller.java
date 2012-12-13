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
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.impl.HBaseMetaTable;
import org.kiji.schema.impl.HBaseSchemaTable;
import org.kiji.schema.impl.HBaseSystemTable;
import org.kiji.schema.impl.HTableInterfaceFactory;
import org.kiji.schema.util.KijiNameValidator;

/**
 * A kiji installer installs or uninstalls kiji instances from an HBase cluster.
 **/
@ApiAudience.Public
public final class KijiInstaller {
  private static final Logger LOG = LoggerFactory.getLogger(KijiInstaller.class);

  /**
   * Installs a kiji instance into the HBase cluster.
   *
   * @param kijiConf The configuration for the kiji instance to install.
   * @param tableFactory HTableInterface factory.
   * @throws IOException If there is an error.
   * @throws org.kiji.schema.KijiInvalidNameException If the kiji instance already exists.
   */
  public void install(
      KijiConfiguration kijiConf,
      HTableInterfaceFactory tableFactory)
      throws IOException, KijiInvalidNameException {

    LOG.info("Installing a kiji instance named '" + kijiConf.getName() + "'...");
    HBaseAdmin hbaseAdmin = new HBaseAdmin(kijiConf.getConf());
    try {
      if (kijiConf.exists()) {
        throw new KijiInvalidNameException(
            "A Kiji instance named " + kijiConf.getName() + " already exists.");
      } else {
        KijiNameValidator.validateKijiName(kijiConf.getName());
      }
      HBaseSystemTable.install(hbaseAdmin, kijiConf, tableFactory);
      HBaseMetaTable.install(hbaseAdmin, kijiConf);
      HBaseSchemaTable.install(hbaseAdmin, kijiConf, tableFactory);

    } finally {
      IOUtils.closeQuietly(hbaseAdmin);
    }

    LOG.info("Installed kiji '" + kijiConf.getName() + "'");
  }

  /**
   * Removes a kiji instance from the HBase cluster including any user tables.
   *
   * @param kijiConf The configuration for the kiji instance to uninstall.
   * @throws IOException If there is an error.
   */
  public void uninstall(KijiConfiguration kijiConf) throws IOException {
    LOG.info("Removing the kiji instance named '" + kijiConf.getName() + "'...");

    Kiji kiji = null;
    HBaseAdmin hbaseAdmin = null;
    try {
      // Delete the user tables.
      // Explicitly instantiate Kiji with license checks disabled.
      kiji = new Kiji(kijiConf);
      hbaseAdmin = new HBaseAdmin(kijiConf.getConf());
      KijiAdmin kijiAdmin = new KijiAdmin(hbaseAdmin, kiji);
      for (String tableName : kijiAdmin.getTableNames()) {
        LOG.debug("Deleting kiji table " + tableName + "...");
        kijiAdmin.deleteTable(tableName);
      }

      // Delete the system tables.
      HBaseSystemTable.uninstall(hbaseAdmin, kijiConf);
      HBaseMetaTable.uninstall(hbaseAdmin, kijiConf);
      HBaseSchemaTable.uninstall(hbaseAdmin, kijiConf);
    } finally {
      IOUtils.closeQuietly(hbaseAdmin);
      IOUtils.closeQuietly(kiji);
    }
    LOG.info("Removed '" + kijiConf.getName() + "'");
  }
}
