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

package org.kiji.schema.tools;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.odiago.common.flags.Flag;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiAdmin;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.avro.MetadataBackup;
import org.kiji.schema.avro.TableBackup;
import org.kiji.schema.avro.TableLayoutBackupEntry;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * A tool to backup and restore Metadata.
 */
public class MetadataTool extends VersionValidatedTool  {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataTool.class);

  @Flag(name="backup", usage="Output filename for Kiji metadata")
  private String mOutFile = "";

  @Flag(name="restore", usage="Input filename to restore Kiji metadata from")
  private String mInFile = "";

  @Flag(name="confirm", usage="Must be set to use the restore feature")
  private boolean mConfirm;

  // The following flags specify whether only certain parts of the backup should be restored.

  @Flag(name="tables", usage="Restore table definitions")
  private boolean mAllTables;

  @Flag(name="schemas", usage="Restore schema definitions")
  private boolean mSchemas;

  @Flag(name="table", usage="Restore a single table definition")
  private String mTableName = "";

  /**
   * Sets flags to indicate which aspects of a backup to restore.
   */
  private void initRestoreOps() {
    if (!mAllTables && !mSchemas && mTableName.isEmpty()) {
      // User has selected no restore operations. Restore everything.
      mAllTables = true;
      mSchemas = true;
    }
  }


  /**
   * Exports all Kiji metadata to an Avro file with the specified filename.
   *
   * @param outputFile the output filename. This file must not exist.
   * @param kiji the Kiji instance to backup.
   * @throws IOException when an error communicating with HBase or the filesystem occurs.
   */
  private static void exportMetadata(String outputFile, Kiji kiji) throws IOException {
    Preconditions.checkNotNull(outputFile);
    Preconditions.checkNotNull(kiji);

    final File file = new File(outputFile);
    if (file.exists()) {
      throw new IOException("Output file '" + outputFile + "' already exists. Won't overwrite.");
    }

    final MetadataBackup.Builder backup = MetadataBackup.newBuilder()
        .setLayoutVersion(kiji.getSystemTable().getDataVersion())
        .setMetaTable(new HashMap<String, TableBackup>());
    kiji.getMetaTable().writeToBackup(backup);

    final MetadataBackup backupRec = backup.build();

    // Store the schema table entries.
    // FIXME(taton): SchemaTable.writeToBackup() should use a builder.
    kiji.getSchemaTable().writeToBackup(backupRec);

    // Now write out the file itself.
    final DatumWriter<MetadataBackup> datumWriter =
        new SpecificDatumWriter<MetadataBackup>(MetadataBackup.class);
    final DataFileWriter<MetadataBackup> fileWriter =
        new DataFileWriter<MetadataBackup>(datumWriter);
    try {
      fileWriter.create(backupRec.getSchema(), file);
      fileWriter.append(backupRec);
    } finally {
      fileWriter.close();
    }
  }

  /**
   * Restores the specified table definition from the metadata backup
   * into the running Kiji instance.
   *
   * @param tableName the name of the table to restore
   * @param backup the deserialized metadata backup.
   * @param kiji the connected Kiji instance to restore to.
   * @throws IOException if there is an error communicating with HBase.
   * @throws RestoreException if there's a user error preventing restore from working.
   */
  private void restoreTable(String tableName, MetadataBackup backup, Kiji kiji)
      throws IOException, RestoreException {
    final KijiMetaTable metaTable = kiji.getMetaTable();
    final Set<String> existingTableNames = Sets.newHashSet(metaTable.listTables());

    final Map<String, TableBackup> metaTableBackup = backup.getMetaTable();
    final TableBackup tableBackup = metaTableBackup.get(tableName);
    if (null == tableBackup) {
      throw new RestoreException(String.format(
          "Cannot restore table '%s' from backup: no matching backup entry found.", tableName));
    }

    final HBaseAdmin hbaseAdmin = new HBaseAdmin(getConf());
    try {
      final KijiAdmin admin = new KijiAdmin(hbaseAdmin, kiji);
      restoreTable(tableName, tableBackup, admin, metaTable, kiji, existingTableNames);
    } finally {
      hbaseAdmin.close();
    }
  }

  /**
   * Restores the specified table definition from the metadata backup into the
   * running Kiji instance.
   *
   * @param tableName the name of the table to restore.
   * @param tableBackup the deserialized backup of the TableLayout to restore.
   * @param admin A KijiAdmin connected to the Kiji instance.
   * @param metaTable the MetaTable of the connected Kiji instance.
   * @param kiji the connected Kiji instance.
   * @param existingTableNames a list of existing tables in the Kiji instance.
   * @throws IOException if there is an error communicating with HBase
   */
  private void restoreTable(
      String tableName,
      TableBackup tableBackup,
      KijiAdmin admin,
      KijiMetaTable metaTable,
      Kiji kiji,
      Set<String> existingTableNames)
      throws IOException {
    Preconditions.checkNotNull(tableBackup);
    Preconditions.checkArgument(!tableBackup.getLayouts().isEmpty(),
        "Backup for table '%s' contains no layout.", tableName);

    if (!existingTableNames.contains(tableName)) {
      getPrintStream().println(String.format("Creating table '%s'.", tableName));
      long timestamp = Long.MAX_VALUE;
      TableLayoutBackupEntry initialEntry = null;
      for (TableLayoutBackupEntry entry : tableBackup.getLayouts()) {
        if (entry.getTimestamp() < timestamp) {
          timestamp = entry.getTimestamp();
          initialEntry = entry;
        }
      }
      final KijiTableLayout initialLayout = new KijiTableLayout(initialEntry.getLayout(), null);
      admin.createTable(tableName, initialLayout, true);
    }

    getPrintStream().println(String.format(
        "Restoring layout history for table '%s' (%d layouts).",
        tableName, tableBackup.getLayouts().size()));
    metaTable.restoreLayoutsFromBackup(tableBackup);
    metaTable.restoreKeyValuesFromBackup(tableBackup);
  }

  /**
   * Restores all tables from the metadata backup into the running Kiji instance.
   *
   * @param backup the deserialized backup of the metadata.
   * @param kiji the connected Kiji instance.
   * @throws IOException if there is an error communicating with HBase.
   */
  private void restoreTables(MetadataBackup backup, Kiji kiji) throws IOException {
    // Restore all tables in the file.
    // Create tables that do not exist, and set the layouts for tables that do.
    final KijiMetaTable metaTable = kiji.getMetaTable();
    final HBaseAdmin hbaseAdmin = new HBaseAdmin(getConf());

    try {
      final KijiAdmin admin = new KijiAdmin(hbaseAdmin, kiji);
      final Set<String> existingTableNames = Sets.newHashSet(metaTable.listTables());
      LOG.debug("Found " + existingTableNames.size() + " existing tables.");
      for (String existingTableName : existingTableNames) {
        LOG.debug("Existing table name: " + existingTableName);
      }

      for (Map.Entry<String, TableBackup> layoutEntry : backup.getMetaTable().entrySet()) {
        final String tableName = layoutEntry.getKey();
        LOG.debug("Found table backup entry for " + tableName);
        final TableBackup tableBackup = layoutEntry.getValue();
        restoreTable(tableName, tableBackup, admin, metaTable, kiji, existingTableNames);
      }
    } finally {
      hbaseAdmin.close();
    }
  }

  /**
   * Restores all SchemaTable entries from the metadata backup.
   *
   * @param backup the deserialized backup of the metadata.
   * @param kiji the connected Kiji instance.
   * @throws IOException if there is an error communicating with HBase.
   */
  private void restoreSchemas(MetadataBackup backup, Kiji kiji) throws IOException {
    // Restore all Schema table entries in the file.
    final KijiSchemaTable schemaTable = kiji.getSchemaTable();
    getPrintStream().println("Restoring schema table entries...");
    schemaTable.restoreFromBackup(backup);
    getPrintStream().println("Restored " + backup.getSchemaTable().size() + " entries.");
  }

  /**
   * Restores Kiji metadata from an Avro file with the specified filename.
   *
   * @param inputFile the input filename.
   * @param kiji the Kiji instance to restore to.
   * @throws IOException when an error communicating with HBase or the file system occurs.
   * @throws RestoreException if there's a user error preventing restore from working.
   */
  private void restoreMetadata(String inputFile, Kiji kiji)
      throws IOException, RestoreException {
    LOG.debug("Restoring metadata...");
    final File file = new File(inputFile);
    if (!file.exists()) {
      throw new IOException("No such metadata file: " + inputFile);
    }

    final DatumReader<MetadataBackup> datumReader =
        new SpecificDatumReader<MetadataBackup>(MetadataBackup.class);
    final DataFileReader<MetadataBackup> fileReader =
        new DataFileReader<MetadataBackup>(file, datumReader);

    try {
      if (!fileReader.hasNext())  {
        throw new IOException("Metadata file '" + inputFile + "' is empty");
      }

      LOG.debug("Reading metadata backup file entry...");
      final MetadataBackup backup = fileReader.next();
      LOG.debug("Read metadata backup file entry.");

      // Check that the dataVersion is compatible with this Kiji instance.
      final KijiSystemTable statusTable = kiji.getSystemTable();
      final String curDataVersion = statusTable.getDataVersion();
      LOG.debug("Metadata backup data version: " + backup.getLayoutVersion());
      LOG.debug("Current data version: " + curDataVersion);
      if (!curDataVersion.equals(backup.getLayoutVersion())) {
        throw new IOException(String.format(
            "Cannot restore: backup layout version '%s' does not match Kiji instance version '%s'.",
            backup.getLayoutVersion(), curDataVersion));
      }

      if (mAllTables) {
        LOG.debug("Restoring all tables...");
        restoreTables(backup, kiji);
      } else if (!mTableName.isEmpty()) {
        LOG.debug("Restoring single table " + mTableName + "...");
        restoreTable(mTableName, backup, kiji);
      }

      if (mSchemas) {
        LOG.debug("Restoring schemas...");
        restoreSchemas(backup, kiji);
      }
    } finally {
      fileReader.close();
    }

    // TODO(WIBI-236): Restore freshener table entries.

    getPrintStream().println("Restore complete.");
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    if (mOutFile.isEmpty() && mInFile.isEmpty()) {
      getPrintStream().println(
          "Must use one of --export or --restore. See --help for details.");
      return 1;
    }

    if (!mOutFile.isEmpty() && !mInFile.isEmpty()) {
      getPrintStream().println("Cannot do both --export and --restore. See --help for details.");
      return 1;
    }

    if (!mInFile.isEmpty() && !mConfirm) {
      getPrintStream().println("Would restore metadata from " + mInFile
          + ", overwriting current metadata.");
      getPrintStream().println("Use --confirm=true if you're sure.");
      getPrintStream().println("(No restore operation performed.)");
      return 1;
    }

    Kiji kiji = getKiji();
    if (!mOutFile.isEmpty()) {
      try {
        exportMetadata(mOutFile, kiji);
      } catch (IOException ioe) {
        getPrintStream().println("Error performing backup: " + ioe.getMessage());
        getPrintStream().println("(Backup failed.)");
        return 1;
      }
    } else {
      assert !mInFile.isEmpty();
      assert mConfirm;
      try {
        initRestoreOps();
        restoreMetadata(mInFile, kiji);
      } catch (IOException ioe) {
        getPrintStream().println("Error performing restore: " + ioe.getMessage());
        getPrintStream().println("(Restore failed.)");
        return 1;
      } catch (RestoreException re) {
        getPrintStream().println(re.getMessage());
        getPrintStream().println("(Restore failed.)");
        return 1;
      }
    }

    return 0;
  }


  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new MetadataTool(), args));
  }
}
