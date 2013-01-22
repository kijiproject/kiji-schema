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

import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.KijiAdmin;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.ToJson;

/**
 * Command-line tool for interacting with table layouts. Actions include reading a layout,
 * setting a table layout, and viewing a table's layout history.
 */
@ApiAudience.Private
public final class LayoutTool extends VersionValidatedTool {
  private static final Logger LOG = LoggerFactory.getLogger(LayoutTool.class);

  @Flag(name="do", usage="Action to perform: dump, set, or history.")
  private String mDo = "dump";

  @Flag(name="table", usage="The kiji table to use.")
  private String mTableName = "";

  @Flag(name="layout",
      usage="Path to the file containing the layout update, in JSON.")
  private String mLayout = "";

  @Flag(name="dry-run",
      usage="Prints what actions would be taken, but does not commit changes.")
  private boolean mDryRun = false;

  @Flag(name="max-versions",
      usage="Maximum number of layout versions to retrieve in the layout history.")
  private int mMaxVersions = 1;

  @Flag(name="write-to",
      usage="Write layout(s) to this file(s). "
          + "Empty means write to console output. "
          + "A '.json' extension is appended to this parameter.")
  private String mWriteTo = "";

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "layout";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "View or modify kiji table layouts.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "DDL";
  }

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    if (mTableName.isEmpty()) {
      throw new RequiredFlagException("table");
    }
    Preconditions.checkArgument(mMaxVersions >= 1, "--max-versions must be >= 1");
  }

  /**
   * Implements the --do=dump operation.
   *
   * @param admin kiji admin interface.
   * @throws Exception on error.
   */
  private void dumpLayout(KijiAdmin admin) throws Exception {
    final KijiTableLayout layout = getKiji().getMetaTable().getTableLayout(mTableName);
    final String json = ToJson.toJsonString(layout.getDesc());
    if (mWriteTo.isEmpty()) {
      System.out.println(json);
    } else {
      final String fileName = String.format("%s.json", mWriteTo);
      final FileOutputStream fos = new FileOutputStream(fileName);
      try {
        fos.write(Bytes.toBytes(json));
      } finally {
        IOUtils.closeQuietly(fos);
      }
    }
  }

  /**
   * Loads a table layout descriptor from a JSON-encoded file.
   *
   * @param filePath Path to a JSON-encoded table layout descriptor.
   * @return the table layout descriptor decoded from the file.
   * @throws Exception on error.
   */
  private TableLayoutDesc loadJsonTableLayoutDesc(String filePath) throws Exception {
    final Path path = new Path(filePath);
    final FileSystem fs = fileSystemSpecified(path)
        ? path.getFileSystem(getConf())
        : FileSystem.getLocal(getConf());
    final InputStream istream = fs.open(path);
    try {
      return KijiTableLayout.readTableLayoutDescFromJSON(istream);
    } finally {
      IOUtils.closeQuietly(istream);
    }
  }

  /**
   * Implements the --do=set operation.
   *
   * @param admin kiji admin interface.
   * @throws Exception on error.
   */
  private void setLayout(KijiAdmin admin) throws Exception {
    final TableLayoutDesc layoutDesc = loadJsonTableLayoutDesc(mLayout);
    admin.setTableLayout(mTableName, layoutDesc, mDryRun, getPrintStream());
  }

  /**
   * Dumps the history of layouts of a given table.
   *
   * @param admin kiji admin interface.
   * @throws Exception on error.
   */
  private void history(KijiAdmin admin) throws Exception {
    // Gather all of the layouts stored in the metaTable.
    final NavigableMap<Long, KijiTableLayout> timedLayouts =
        getKiji().getMetaTable().getTimedTableLayoutVersions(mTableName, mMaxVersions);
    if (timedLayouts.isEmpty()) {
        throw new RuntimeException("No such table: " + mTableName);
    }
    for (Map.Entry<Long, KijiTableLayout> entry: timedLayouts.entrySet()) {
      final long timestamp = entry.getKey();
      final KijiTableLayout layout = entry.getValue();
      final String json = ToJson.toJsonString(layout.getDesc());

      if (mWriteTo.isEmpty()) {
        System.out.printf("timestamp: %d:%n%s", timestamp, json);
      } else {
        final String fileName = String.format("%s-%d.json", mWriteTo, timestamp);
        final FileOutputStream fos = new FileOutputStream(fileName);
        try {
          fos.write(Bytes.toBytes(json));
        } finally {
          IOUtils.closeQuietly(fos);
        }
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    final KijiAdmin admin = getKiji().getAdmin();
    if (mDo.equals("dump")) {
      dumpLayout(admin);
    } else if (mDo.equals("set")) {
      Preconditions.checkArgument(!mLayout.isEmpty(),
          "Specify the layout with --layout=path/to/layout.json");
      setLayout(admin);
    } else if (mDo.equals("history")) {
      history(admin);
    } else {
      System.err.println("Unknown layout action: " + mDo);
      System.err.println("Specify the action to perform with --do=(dump|set|history)");
      return 1;
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
  protected static boolean fileSystemSpecified(Path path) {
    return null != path.toUri().getScheme();
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new LayoutTool(), args));
  }
}
