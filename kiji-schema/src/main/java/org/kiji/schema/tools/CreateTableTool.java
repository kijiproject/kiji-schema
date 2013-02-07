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

import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.SplitKeyFile;


/**
 * Command-line tool for creating kiji tables in kiji instances.
 */
@ApiAudience.Private
public final class CreateTableTool extends VersionValidatedTool {
  private static final Logger LOG = LoggerFactory.getLogger(CreateTableTool.class);

  @Flag(name="layout", usage="Path to a file containing a JSON table layout.")
  private String mLayout = "";

  // The following two flags are used to specify the initial number of regions in the table.
  // Only one of them may be specified -- if your layout has key format encoding HASH or
  // HASH_PREFIX, then you may only use --num-regions.  Otherwise, you must specify the split key
  // file with --split-key-file.
  @Flag(name="num-regions", usage="The number of initial regions to create in the table")
  private int mNumRegions = 1;

  @Flag(name="split-key-file",
      usage="Path to a file of row keys to use as boundaries between regions")
  private String mSplitKeyFilePath = "";

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
    if (mLayout.isEmpty()) {
      throw new RequiredFlagException("layout");
    }
    if (mNumRegions != 1 && !mSplitKeyFilePath.isEmpty()) {
      throw new RuntimeException("Only one of --num-regions and --split-key-file may be specified");
    }
    if (mNumRegions < 1) {
      throw new RuntimeException("--num-regions must be positive");
    }
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    getPrintStream().println("Parsing table layout: " + mLayout);
    final Path path = new Path(mLayout);
    final FileSystem fs =
        fileSystemSpecified(path) ? path.getFileSystem(getConf()) : FileSystem.getLocal(getConf());
    final FSDataInputStream inputStream = fs.open(path);
    final KijiTableLayout tableLayout = KijiTableLayout.createFromEffectiveJson(inputStream);
    final String tableName = tableLayout.getName();
    if ((getURI().getTable() != null) && !tableName.equals(getURI().getTable())) {
      throw new IllegalArgumentException(
          String.format("Table name '%s' does not match URI %s", tableName, getURI()));
    }

    // For large numbers of initial regions, table creation may take a long time as we wait for
    // the new regions to come online. Increase the hbase RPC timeout to compensate.
    int hbaseTimeout = getConf().getInt("hbase.rpc.timeout", 60000);
    hbaseTimeout = hbaseTimeout * 10;
    getConf().setInt("hbase.rpc.timeout", hbaseTimeout);

    setURI(KijiURI.newBuilder(getURI()).withTableName(tableName).build());
    getPrintStream().println("Creating kiji table: " + getURI().toString() + "...");
    if (mNumRegions > 1) {
      // Create a table with an initial number of evenly split regions.
      getKiji().createTable(tableName, tableLayout, mNumRegions);
    } else if (!mSplitKeyFilePath.isEmpty()) {
      switch (tableLayout.getDesc().getKeysFormat().getEncoding()) {
      case HASH:
      case HASH_PREFIX:
        throw new RuntimeException(
            "Row key hashing is enabled for the table.  Use --num-regions instead.");
      case RAW:
        break;
      default:
        throw new RuntimeException(
            "Unexpected row key encoding: " + tableLayout.getDesc().getKeysFormat().getEncoding());
      }
      // Open the split key file.
      Path splitKeyFilePath = new Path(mSplitKeyFilePath);
      FileSystem splitKeyPathFs = fileSystemSpecified(splitKeyFilePath)
          ? splitKeyFilePath.getFileSystem(getConf()) : FileSystem.getLocal(getConf());
      FSDataInputStream splitKeyFileInputStream = splitKeyPathFs.open(splitKeyFilePath);

      // Read the split keys.
      List<byte[]> splitKeys = SplitKeyFile.decodeRegionSplitList(splitKeyFileInputStream);
      for (byte[] splitKey : splitKeys) {
        LOG.debug("Read split key from file: " + Bytes.toString(splitKey));
      }

      // Create the table with the given split keys.
      getKiji().createTable(tableName, tableLayout,
          splitKeys.toArray(new byte[splitKeys.size()][]));
    } else {
      // Create a table with the default initial region.
      getKiji().createTable(tableName, tableLayout);
    }

    return 0;
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
