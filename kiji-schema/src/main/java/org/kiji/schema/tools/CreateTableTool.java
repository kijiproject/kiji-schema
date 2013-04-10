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

import java.io.IOException;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.SplitKeyFile;

/**
 * Command-line tool for creating kiji tables in kiji instances.
 */
@ApiAudience.Private
public final class CreateTableTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(CreateTableTool.class);

  @Flag(name="table", usage="URI of the Kiji table to create,"
      + " eg. --table=kiji://hbase-address/kiji-instance/table.")
  private String mTableURIFlag = null;

  @Flag(name="layout", usage="Path to a file containing a JSON table layout description.")
  private String mLayout = null;

  @Flag(name="num-regions",
      usage="Number (>= 1) of initial regions to create in the table.\n"
          + "\tRegions are evenly sized across the HBase row key space.\n"
          + "\tDo not use if specifying regions explicitly with --split-key-file=...")
  private String mNumRegionsFlag = null;

  @Flag(name="split-key-file",
      usage="Path to a file of row keys to use as boundaries between regions.\n"
          + "\tSplit key files are formatted as one HBase encoded row per line:\n"
          + "\t\t- each HBase encoded row is formatted in ASCII form;\n"
          + "\t\t- non printable characters are escaped as '\\x??';\n"
          + "\t\t- backslash must be escaped as '\\\\'.\n"
          + "\tThe first and last HBase rows are omitted: a region split file with one\n"
          + "\tentry E designates 2 regions: [''..E) and [E..).\n"
          + "\tDo not use if specifying a number of regions with --num-regions=N.")
  private String mSplitKeyFilePath = null;

  /**
   * Initialized from --num-regions=N flag (N >= 1) if flag is provided.
   * Defaults to 1 if neither --num-regions nor --split-key-file is specified.
   */
  private int mNumRegions = -1;

  /** Opened Kiji to use. */
  private Kiji mKiji;

  /** KijiURI of the table to create. */
  private KijiURI mTableURI;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "create-table";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Create a kiji table in a kiji instance.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "DDL";
  }

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    super.validateFlags();
    Preconditions.checkArgument((mTableURIFlag != null) && !mTableURIFlag.isEmpty(),
        "Specify the table to create with --table=kiji://hbase-address/kiji-instance/table");
    mTableURI = KijiURI.newBuilder(mTableURIFlag).build();

    Preconditions.checkArgument((mLayout != null) && !mLayout.isEmpty(),
        "Specify the table layout with --layout=/path/to/table-layout.json");

    if ((mNumRegionsFlag != null) && !mNumRegionsFlag.isEmpty()) {
      mNumRegions = Integer.parseInt(mNumRegionsFlag);
      Preconditions.checkArgument(mNumRegions >= 1,
          "Invalid initial number of regions {}, must be >= 1.", mNumRegions);
    } else  if ((mSplitKeyFilePath == null) || mSplitKeyFilePath.isEmpty()) {
      // No region split specified, defaults to 1:
      mNumRegions = 1;
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() throws Exception {
    super.setup();
    mKiji = Kiji.Factory.open(mTableURI, getConf());
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup() throws IOException {
    mKiji.release();
    mKiji = null;
    super.cleanup();
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    getPrintStream().println("Parsing table layout: " + mLayout);
    final Path path = new Path(mLayout);
    final FileSystem fs =
        fileSystemSpecified(path) ? path.getFileSystem(getConf()) : FileSystem.getLocal(getConf());
    final FSDataInputStream inputStream = fs.open(path);
    final TableLayoutDesc tableLayout = KijiTableLayout.readTableLayoutDescFromJSON(inputStream);
    final String tableName = tableLayout.getName();
    Preconditions.checkArgument(
        (mTableURI.getTable() == null) || tableName.equals(mTableURI.getTable()),
        "Table name '%s' does not match URI %s", tableName, mTableURI);

    // For large numbers of initial regions, table creation may take a long time as we wait for
    // the new regions to come online. Increase the hbase RPC timeout to compensate.
    int hbaseTimeout = getConf().getInt("hbase.rpc.timeout", 60000);
    hbaseTimeout = hbaseTimeout * 10;
    getConf().setInt("hbase.rpc.timeout", hbaseTimeout);

    getPrintStream().println("Creating Kiji table " + mTableURI);
    if (mNumRegions >= 1) {
      // Create a table with an initial number of evenly split regions.
      mKiji.createTable(tableLayout, mNumRegions);

    } else if (!mSplitKeyFilePath.isEmpty()) {
      switch (KijiTableLayout.getEncoding(tableLayout.getKeysFormat())) {
      case HASH:
      case HASH_PREFIX:
        throw new IllegalArgumentException(
            "Row key hashing is enabled for the table. Use --num-regions=N instead.");
      case RAW:
        break;
      case FORMATTED:
        // TODO Support pre-splitting tables for FORMATTED RKF
        // (https://jira.kiji.org/browse/SCHEMA-172)
        throw new RuntimeException("CLI support for FORMATTED row keys is not yet available");
      default:
        throw new RuntimeException(
            "Unexpected row key encoding: "
                + KijiTableLayout.getEncoding(tableLayout.getKeysFormat()));
      }
      // Open the split key file.
      final Path splitKeyFilePath = new Path(mSplitKeyFilePath);
      final FileSystem splitKeyPathFs = fileSystemSpecified(splitKeyFilePath)
          ? splitKeyFilePath.getFileSystem(getConf())
          : FileSystem.getLocal(getConf());
      final FSDataInputStream splitKeyFileInputStream = splitKeyPathFs.open(splitKeyFilePath);

      // Read the split keys.
      final List<byte[]> splitKeys = SplitKeyFile.decodeRegionSplitList(splitKeyFileInputStream);
      LOG.debug("Read {} keys from split-key-file '{}':", splitKeys.size(), splitKeyFilePath);
      for (int i = 0; i < splitKeys.size(); ++i) {
        LOG.debug("Split key #{}: {}", i, Bytes.toStringBinary(splitKeys.get(i)));
      }

      // Create the table with the given split keys.
      mKiji.createTable(tableLayout, splitKeys.toArray(new byte[splitKeys.size()][]));

    } else {
      // Create a table with a single initial region:
      mKiji.createTable(tableLayout);
    }

    return SUCCESS;
  }

  /**
   * Determines whether a path has its filesystem explicitly specified.  Did it start
   * with "hdfs://" or "file://"?
   *
   * @param path The path to check.
   * @return Whether a file system was explicitly specified in the path.
   */
  private static boolean fileSystemSpecified(Path path) {
    return null != path.toUri().getScheme();
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new CreateTableTool(), args));
  }
}
