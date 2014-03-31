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

package org.kiji.schema.impl;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KConstants;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiAlreadyExistsException;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.avro.MetadataBackup;
import org.kiji.schema.avro.TableBackup;
import org.kiji.schema.avro.TableLayoutBackupEntry;
import org.kiji.schema.hbase.HBaseFactory;
import org.kiji.schema.impl.hbase.HBaseMetaTable;
import org.kiji.schema.util.ResourceUtils;

/**
 * The MetadataRestorer is responsible for backing up and restoring the metadata stored in Kiji's
 * Meta, System, and Schema tables, including information about table layouts, schemas, and system
 * variables.
 *
 * <p>To create a backup file, use {@link MetadataRestorer#exportMetadata(String, Kiji)} a filename
 * and a Kiji instance. Backups are stored as {@link MetadataBackup} records. To restore a Kiji
 * instance from a MetadataBackup record, use the various public restore methods.</p>
 */
@ApiAudience.Private
public final class MetadataRestorer {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataRestorer.class);

  /**
   * Exports all Kiji metadata to an Avro file with the specified filename.
   *
   * @param outputFile the output filename. This file must not exist.
   * @param kiji the Kiji instance to backup.
   * @throws IOException when an error communicating with HBase or the filesystem occurs.
   */
  public void exportMetadata(String outputFile, Kiji kiji) throws IOException {
    Preconditions.checkNotNull(outputFile);
    Preconditions.checkNotNull(kiji);

    final File file = new File(outputFile);
    if (file.exists()) {
      throw new IOException("Output file '" + outputFile + "' already exists. Won't overwrite.");
    }
    final MetadataBackup backup = MetadataBackup.newBuilder()
        .setLayoutVersion(kiji.getSystemTable().getDataVersion().toString())
        .setSystemTable(kiji.getSystemTable().toBackup())
        .setSchemaTable(kiji.getSchemaTable().toBackup())
        .setMetaTable(kiji.getMetaTable().toBackup())
        .build();

    // Now write out the file itself.
    final DatumWriter<MetadataBackup> datumWriter =
      new SpecificDatumWriter<MetadataBackup>(MetadataBackup.class);
    final DataFileWriter<MetadataBackup> fileWriter =
      new DataFileWriter<MetadataBackup>(datumWriter);
    try {
      fileWriter.create(backup.getSchema(), file);
      fileWriter.append(backup);
    } finally {
      ResourceUtils.closeOrLog(fileWriter);
    }
  }

  /**
   * Restores the specified table definition from the metadata backup into the running Kiji
   * instance.
   *
   * @param tableName the name of the table to restore.
   * @param tableBackup the deserialized backup of the TableLayout to restore.
   * @param metaTable the MetaTable of the connected Kiji instance.
   * @param kiji the connected Kiji instance.
   * @throws IOException if there is an error communicating with HBase
   */
  private void restoreTable(
    String tableName,
    TableBackup tableBackup,
    KijiMetaTable metaTable,
    Kiji kiji)
    throws IOException {
    Preconditions.checkNotNull(tableBackup);
    Preconditions.checkNotNull(tableBackup.getTableLayoutsBackup());

    List<TableLayoutBackupEntry> layouts = tableBackup.getTableLayoutsBackup().getLayouts();
    Preconditions.checkArgument(!layouts.isEmpty(),
        "Backup for table '%s' contains no layout.", tableName);
    LOG.info("Creating table '{}'.", tableName);

    // The initial entry is the entry with the lowest timestamp.
    long lowestTimestamp = KConstants.END_OF_TIME;
    TableLayoutBackupEntry initialEntry = null;
    for (TableLayoutBackupEntry entry : layouts) {
      if (entry.getTimestamp() < lowestTimestamp) {
        lowestTimestamp = entry.getTimestamp();
        initialEntry = entry;
      }
    }

    try {
      kiji.createTable(initialEntry.getLayout());
    } catch (KijiAlreadyExistsException kaee) {
      LOG.info("Table already exists in HBase. Continuing with restore operation.");
    }

    LOG.info("Restoring layout history for table '%s' (%d layouts).", tableName,
        tableBackup.getTableLayoutsBackup().getLayouts().size());
    metaTable.restoreLayoutsFromBackup(tableName, tableBackup.getTableLayoutsBackup());
    metaTable.restoreKeyValuesFromBackup(tableName, tableBackup.getKeyValueBackup());
  }


  /**
   * Restores all tables from the metadata backup into the running Kiji instance.
   *
   * @param backup the deserialized backup of the metadata.
   * @param kiji the connected Kiji instance.
   * @throws IOException if there is an error communicating with HBase.
   */
  public void restoreTables(MetadataBackup backup, Kiji kiji) throws IOException {
    final HBaseFactory hbaseFactory = HBaseFactory.Provider.get();
    final HBaseAdmin hbaseAdmin =
        hbaseFactory.getHBaseAdminFactory(kiji.getURI()).create(kiji.getConf());

    final KijiMetaTable metaTable = kiji.getMetaTable();
    try {
      HBaseMetaTable.uninstall(hbaseAdmin, kiji.getURI());
      HBaseMetaTable.install(hbaseAdmin, kiji.getURI());

      final Map<String, TableBackup> tables = backup.getMetaTable().getTables();
      for (Map.Entry<String, TableBackup> layoutEntry : tables.entrySet()) {
        final String tableName = layoutEntry.getKey();
        LOG.debug("Found table backup entry for " + tableName);
        final TableBackup tableBackup = layoutEntry.getValue();
        restoreTable(tableName, tableBackup, metaTable, kiji);
      }
    } finally {
      ResourceUtils.closeOrLog(hbaseAdmin);
    }
  }

   /**
   * Restores all SchemaTable entries from the metadata backup.
   *
   * @param backup the deserialized backup of the metadata.
   * @param kiji the connected Kiji instance.
   * @throws IOException if there is an error communicating with HBase.
   */
  public void restoreSchemas(MetadataBackup backup, Kiji kiji) throws IOException {
    // Restore all Schema table entries in the file.
    final KijiSchemaTable schemaTable = kiji.getSchemaTable();
    LOG.info("Restoring schema table entries...");
    schemaTable.fromBackup(backup.getSchemaTable());
    LOG.info("Restored " + backup.getSchemaTable().getEntries().size() + " entries.");
  }

  /**
   * Restores all SystemTable entries from the backup.
   *
   * @param backup the deserialized backup of the metadata.
   * @param kiji the connected Kiji instance.
   * @throws IOException if there is an error communicating with HBase.
   */
  public void restoreSystemVars(MetadataBackup backup, Kiji kiji) throws IOException {
    // Restore all System table entries from the file.
    final KijiSystemTable systemTable = kiji.getSystemTable();
    LOG.info("Restoring system table entries...");
    systemTable.fromBackup(backup.getSystemTable());
    LOG.info(String.format("Restored %d entries.", backup.getSystemTable().getEntries().size()));
  }
}
