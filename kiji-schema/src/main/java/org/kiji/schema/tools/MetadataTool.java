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
import java.util.List;

import com.google.common.base.Joiner;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.common.flags.Flag;
import org.kiji.schema.KConstants;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.MetadataBackup;
import org.kiji.schema.impl.MetadataRestorer;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.schema.util.ResourceUtils;

/**
 * A tool to backup and restore Metadata.
 */
public class MetadataTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataTool.class);
  private MetadataRestorer mRestorer = new MetadataRestorer();

  @Flag(name="kiji", usage="The KijiURI of the instance to use.")
  private String mKijiURIString = String.format("kiji://.env/%s",
      KConstants.DEFAULT_INSTANCE_NAME);
  @Flag(name = "backup", usage = "Output filename for Kiji metadata")
  private String mOutFile = "";
  @Flag(name = "restore", usage = "Input filename to restore Kiji metadata from")
  private String mInFile = "";
  // The following flags specify whether only certain parts of the backup should be restored.
  @Flag(name = "tables", usage = "Restore table definitions")
  private boolean mAllTables;
  @Flag(name = "schemas", usage = "Restore schema definitions")
  private boolean mSchemas;

  /** Opened Kiji to use. */
  private Kiji mKiji;
  /** KijiURI of the target instance. */
  private KijiURI mURI;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "metadata";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Backup or restore kiji metadata.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Metadata";
  }

  /**
   * Sets flags to indicate which aspects of a backup to restore.
   */
  private void initRestoreOps() {
    if (!mAllTables && !mSchemas) {
      // User has selected no restore operations. Restore everything.
      mAllTables = true;
      mSchemas = true;
    }
  }

  /**
   * Restores Kiji metadata from an Avro file with the specified filename.
   *
   * @param inputFile the input filename.
   * @param kiji the Kiji instance to restore to.
   * @throws IOException when an error communicating with HBase or the file system occurs.
   */
  private void restoreMetadata(String inputFile, Kiji kiji)
    throws IOException {
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
      if (!fileReader.hasNext()) {
        throw new IOException("Metadata file '" + inputFile + "' is empty");
      }

      LOG.debug("Reading metadata backup file entry...");
      final MetadataBackup backup = fileReader.next();
      LOG.debug("Read metadata backup file entry.");

      // Check that the dataVersion is compatible with this Kiji instance.
      final KijiSystemTable statusTable = kiji.getSystemTable();
      final ProtocolVersion curDataVersion = statusTable.getDataVersion();
      LOG.debug("Metadata backup data version: " + backup.getLayoutVersion());
      LOG.debug("Current data version: " + curDataVersion);
      if (!curDataVersion.equals(ProtocolVersion.parse(backup.getLayoutVersion()))) {
        throw new IOException(String.format(
          "Cannot restore: backup layout version '%s' does not match Kiji instance version '%s'.",
          backup.getLayoutVersion(), curDataVersion));
      }

      if (mAllTables) {
        LOG.debug("Restoring all tables...");
        mRestorer.restoreTables(backup, kiji);
      }

      if (mSchemas) {
        LOG.debug("Restoring schemas...");
        mRestorer.restoreSchemas(backup, kiji);
      }
    } finally {
      fileReader.close();
    }

    getPrintStream().println("Restore complete.");
  }

  /**
   * Opens a kiji instance.
   *
   * @return The opened kiji.
   * @throws IOException if there is an error.
   */
  private Kiji openKiji() throws IOException {
    return Kiji.Factory.open(getURI(), getConf());
  }

  /**
   * Retrieves the kiji instance used by this tool. On the first call to this method,
   * the kiji instance will be opened and will remain open until {@link #cleanup()} is called.
   *
   * @return The kiji instance.
   * @throws IOException if there is an error loading the kiji.
   */
  protected synchronized Kiji getKiji() throws IOException {
    if (null == mKiji) {
      mKiji = openKiji();
    }
    return mKiji;
  }

  /**
   * Returns the kiji URI of the target this tool operates on.
   *
   * @return The kiji URI of the target this tool operates on.
   */
  protected KijiURI getURI() {
    if (null == mURI) {
      getPrintStream().println("No URI specified.");
    }
    return mURI;
  }

  /**
   * Sets the kiji URI of the target this tool operates on.
   *
   * @param uri The kiji URI of the target this tool should operate on.
   */
  protected void setURI(KijiURI uri) {
    if (null == mURI) {
      mURI = uri;
    } else {
      getPrintStream().printf("URI is already set to: %s", mURI.toString());
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() {
    setURI(parseURI(mKijiURIString));
    getConf().setInt(HConstants.ZOOKEEPER_CLIENT_PORT, mURI.getZookeeperClientPort());
    getConf().set(HConstants.ZOOKEEPER_QUORUM,
        Joiner.on(",").join(mURI.getZookeeperQuorumOrdered()));
    setConf(HBaseConfiguration.addHbaseResources(getConf()));
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup() {
    ResourceUtils.releaseOrLog(mKiji);
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    if (mOutFile.isEmpty() && mInFile.isEmpty()) {
      getPrintStream().println(
        "Must use one of --backup or --restore. See --help for details.");
      return 1;
    }

    if (!mOutFile.isEmpty() && !mInFile.isEmpty()) {
      getPrintStream().println("Cannot do both --backup and --restore. See --help for details.");
      return 1;
    }

    Kiji kiji = getKiji();
    if (!mOutFile.isEmpty()) {
      try {
        mRestorer.exportMetadata(mOutFile, kiji);
      } catch (IOException ioe) {
        getPrintStream().println("Error performing backup: " + ioe.getMessage());
        getPrintStream().println("(Backup failed.)");
        return 1;
      }
    } else {
      assert !mInFile.isEmpty();
        if (isInteractive()) {
          if (!yesNoPrompt("Are you sure you want to restore metadata from backup? \n"
            + "This will delete your current metatable. ")) {
            getPrintStream().println("No metadata restore operation performed.");
            return 1;
          }
        }
      try {
        getPrintStream().println("Restoring Metadata from backup.");
        initRestoreOps();
        restoreMetadata(mInFile, kiji);
      } catch (IOException ioe) {
        getPrintStream().println("Error performing restore: " + ioe.getMessage());
        getPrintStream().println("(Restore failed.)");
        return 1;
      } catch (Exception re) {
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
    System.exit(new KijiToolLauncher().run(new MetadataTool(), args));
  }
}
