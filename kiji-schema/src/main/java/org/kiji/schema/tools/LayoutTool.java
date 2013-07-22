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
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.schema.util.ToJson;

/**
 * Command-line tool for interacting with table layouts. Actions include reading a layout,
 * setting a table layout, and viewing a table's layout history.
 *
 * <h2>Examples:</h2>
 * Write the recent layout history of a table to a file:
 * <pre>
 *   kiji layout --table=kiji://my-hbase/my-instance/my-table/ \
 *       --do=history --max-version=3 --write-to=my-output-file
 * </pre>
 * Set a new layout for a table:
 * <pre>
 *   kiji layout --table=kiji://my-hbase/my-instance/my-table/ \
 *       --do=set --layout=my-input-file
 * </pre>
 * Perform a dry run of setting a table layout:
 * <pre>
 *   kiji layout --table=kiji://my-hbase/my-instance/my-table/ \
 *       --do=set --layout=my-input-file --dry-run=true
 * </pre>
 */
@ApiAudience.Private
public final class LayoutTool extends BaseTool {
  @Flag(name="do", usage="Action to perform: dump, set, or history.")
  private String mDo = "dump";

  @Flag(name="table", usage="The KijiURI of the table to use.")
  private String mTableURIFlag = null;

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

  /** URI of the table to edit or print the layout of. */
  private KijiURI mTableURI;

  /** The Kiji to use to set layouts. */
  private Kiji mKiji;

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
    Preconditions.checkArgument((mTableURIFlag != null) && !mTableURIFlag.isEmpty(),
        "Specify a table with --table=kiji://hbase-address/kiji-instance/table");
    mTableURI = KijiURI.newBuilder(mTableURIFlag).build();
    Preconditions.checkArgument(mTableURI.getTable() != null,
        "Specify a table with --table=kiji://hbase-address/kiji-instance/table");

    Preconditions.checkArgument(mMaxVersions >= 1,
        "Invalid maximum number of versions: {}, --max-versions must be >= 1",
        mMaxVersions);
  }

  /**
   * Implements the --do=dump operation.
   *
   * @throws Exception on error.
   */
  private void dumpLayout() throws Exception {
    final KijiTableLayout layout = mKiji.getMetaTable().getTableLayout(mTableURI.getTable());
    final StringBuilder json = new StringBuilder();
    json.append("/*\n");
    json.append("   The raw JSON view of table layouts is intended for use by\n");
    json.append("   system administrators or for debugging purposes.\n");
    json.append("\n");
    json.append("   Most users should use the 'kiji-schema-shell' DDL tool to modify\n");
    json.append("   your table layouts instead.\n");
    json.append("*/\n");
    json.append(ToJson.toJsonString(layout.getDesc()));
    if (mWriteTo.isEmpty()) {
      System.out.println(json.toString());
    } else {
      final String fileName = String.format("%s.json", mWriteTo);
      final FileOutputStream fos = new FileOutputStream(fileName);
      try {
        fos.write(Bytes.toBytes(json.toString()));
      } finally {
        ResourceUtils.closeOrLog(fos);
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
      ResourceUtils.closeOrLog(istream);
      ResourceUtils.closeOrLog(fs);
    }
  }

  /**
   * Implements the --do=set operation.
   *
   * @param kiji Kiji instance.
   * @throws Exception on error.
   */
  private void setLayout(Kiji kiji) throws Exception {
    final TableLayoutDesc layoutDesc = loadJsonTableLayoutDesc(mLayout);
    Preconditions.checkArgument(mTableURI.getTable().equals(layoutDesc.getName()),
        "Descriptor table name '%s' does not match URI %s.",
        layoutDesc.getName(), mTableURI);
    kiji.modifyTableLayout(layoutDesc, mDryRun, getPrintStream());
  }

  /**
   * Dumps the history of layouts of a given table.
   *
   * @throws Exception on error.
   */
  private void history() throws Exception {
    // Gather all of the layouts stored in the metaTable.
    final NavigableMap<Long, KijiTableLayout> timedLayouts =
        mKiji.getMetaTable().getTimedTableLayoutVersions(mTableURI.getTable(), mMaxVersions);
    if (timedLayouts.isEmpty()) {
      throw new RuntimeException("No such table: " + mTableURI.getTable());
    }
    for (Map.Entry<Long, KijiTableLayout> entry: timedLayouts.entrySet()) {
      final long timestamp = entry.getKey();
      final KijiTableLayout layout = entry.getValue();
      final String json = ToJson.toJsonString(layout.getDesc());

      if (mWriteTo.isEmpty()) {
        // SCHEMA-14: Add a newline to separate the dumped JSON versions of the layout.
        System.out.printf("timestamp: %d:%n%s%n", timestamp, json);
      } else {
        final String fileName = String.format("%s-%d.json", mWriteTo, timestamp);
        final FileOutputStream fos = new FileOutputStream(fileName);
        try {
          fos.write(Bytes.toBytes(json));
        } finally {
          ResourceUtils.closeOrLog(fos);
        }
      }
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
    ResourceUtils.releaseOrLog(mKiji);
    mKiji = null;
    super.cleanup();
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    if (mDo.equals("dump")) {
      dumpLayout();
    } else if (mDo.equals("set")) {
      Preconditions.checkArgument(!mLayout.isEmpty(),
          "Specify the layout with --layout=path/to/layout.json");
      setLayout(mKiji);
    } else if (mDo.equals("history")) {
      history();
    } else {
      System.err.println("Unknown layout action: " + mDo);
      System.err.println("Specify the action to perform with --do=(dump|set|history)");
      return FAILURE;
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
