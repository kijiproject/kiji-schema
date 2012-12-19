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

package org.kiji.schema.testutil;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.tools.BaseTool;
import org.kiji.schema.tools.CreateTableTool;
import org.kiji.schema.tools.DeleteTableTool;
import org.kiji.schema.tools.InstallTool;
import org.kiji.schema.tools.KijiToolLauncher;
import org.kiji.schema.tools.UninstallTool;
import org.kiji.schema.util.ToJson;

/**
 * IntegrationHelper provides methods for installing and managing a Kiji instance during a test.
 *
 * <p>If you would like to install a new Kiji instance, it will randomly generate a new
 * Kiji instance name so you don't step on the toes of other tests.  If you do create Kiji
 * instances, you uninstall them when you are finished.</p>
 *
 * <p>Almost by definition, any test that uses this will be prefixed with
 * "IntegrationTest" and will thus be run in 'mvn integration-test' instead of a nice fast
 * 'mvn test'.</p>
 *
 * <p>If you only need one Kiji instance during your test, you can extend {@link
 * org.kiji.schema.testutil.AbstractKijiIntegrationTest}.</p>
 */
public class IntegrationHelper extends Configured {
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationHelper.class);

  /** Some test data that can be loaded into the "foo" table. */
  public static final String INPUT_FILE = "org/kiji/schema/test-data.csv";

  /** A bulk import input-format file describing the test-data file above. */
  private static final String INPUT_FORMAT_FILE
      = "org/kiji/schema/inputlayout/test-integration-helper-input.txt";

  /** A directory within the DFS where tests can create files. */
  private static final String DFS_TEST_DIR = "test-data";

  /**
   * Creates a new <code>IntegrationHelper</code> instance.
   *
   * @param conf A configuration specifying the HBase cluster.
   */
  public IntegrationHelper(Configuration conf) {
    super(conf);
  }

  /**
   * Copies a local file into the default filesystem (which is HDFS if you've started an
   * HBase cluster).
   *
   * @param localFile The file to copy.
   * @param destPath A relative destination path to be used within the shared tmp dir.
   * @return The path of the file in HDFS.
   */
  public Path copyToDfs(File localFile, String destPath) throws IOException {
    Path target = getDfsPath(destPath);
    FileSystem fs = FileSystem.get(getConf());
    if (!fs.exists(target)) {
      // Only copy if it doesn't already exist.
      FileUtil.copy(localFile, fs, target, false, getConf());
    }
    return target;
  }

  /**
   * Get the DFS path corresponding with a local file path.
   *
   * @param destPath A relative destination path to be used within the shared tmp dir.
   * @return The path to the file in the mini HDFS filesystem.
   * @throws java.io.IOException If there is an error.
   */
  public Path getDfsPath(String destPath) throws IOException {
    return new Path(DFS_TEST_DIR, destPath).makeQualified(FileSystem.get(getConf()));
  }

  /**
   * Installs Kiji metadata tables in the HBase instance.
   *
   * @param kijiURI The uri of the Kiji instance to install.
   * @throws Exception If there is an error.
   */
  public void installKiji(KijiURI kijiURI) throws Exception {
    // TODO: We should be using command line tools programmatically in tester set up.

    // Write resource to a temporary file.
    ToolResult result = runTool(getConf(), new InstallTool(), new String[] {
      "--kiji=" + kijiURI.toString(),
    });

    if (0 != result.getReturnCode()) {
      throw new Exception("Non-zero return from installer: " + result.getReturnCode()
          + ". Stdout: " + result.getStdoutUtf8());
    }
  }

  /**
   * Uninstall a Kiji instance.
   *
   * @throws Exception If there is an error.
   */
  public void uninstallKiji(KijiURI kijiURI) throws Exception {
    ToolResult result = runTool(getConf(), new UninstallTool(), new String[] {
      "--kiji=" + kijiURI.toString(),
      "--confirm",
    });
    if (0 != result.getReturnCode()) {
      throw new Exception("Non-zero return from uninstaller: " + result.getReturnCode()
          + ". Stdout: " + result.getStdoutUtf8());
    }
  }

  /**
   * Runs a tool and captures the console output.
   *
   * @param conf The configuration to run the tool with.
   * @param tool The tool to run.
   * @param args The command-line args to pass to the tool.
   * @return A result with the captured tool output.
   * @throws Exception If there is an error.
   */
  public ToolResult runTool(Configuration conf, BaseTool tool, String[] args) throws Exception {
    // Capture STDOUT to an in-memory output stream.
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    tool.setPrintStream(new PrintStream(output));

    // Run the tool.
    LOG.debug("Running tool " + tool.getClass().getName() + " with args " + Arrays.toString(args));
    KijiToolLauncher launcher = new KijiToolLauncher();
    launcher.setConf(conf);
    int exitCode = launcher.run(tool, args);
    return new ToolResult(exitCode, output);
  }

  /**
   * Writes the contents of the input stream to the temporary file provided.
   *
   * @param tempFile The temporary file.
   * @param inStream The input stream.
   */
  private static void writeTempFile(File tempFile, InputStream inStream) throws IOException {
    OutputStream outStream = new FileOutputStream(tempFile);
    try {
      IOUtils.copy(inStream, outStream);
    } finally {
      if (null != outStream) {
        outStream.close();
      }
    }
  }

  /**
   * Writes the files necessary to create the foo table.
   *
   * @param layoutFile A file to hold the table layout.
   * @param dataFile A file to hold the table data.
   * @param formatFile A file to hold the data format.
   * @throws java.io.IOException If there is an error while writing the files.
   */
  private void writeFooTableFiles(File layoutFile, File dataFile, File formatFile)
      throws IOException {
    InputStream layoutStream = null;
    InputStream dataStream = null;
    InputStream formatStream = null;

    try {
      layoutStream = new ByteArrayInputStream(Bytes.toBytes(ToJson.toJsonString(
          KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST))));
      dataStream = getClass().getClassLoader().getResourceAsStream(INPUT_FILE);
      formatStream = getClass().getClassLoader().getResourceAsStream(INPUT_FORMAT_FILE);

      writeTempFile(layoutFile, layoutStream);
      writeTempFile(dataFile, dataStream);
      writeTempFile(formatFile, formatStream);
    } finally {
      if (null != layoutStream) {
        layoutStream.close();
      }
      if (null != dataStream) {
        dataStream.close();
      }
      if (null != formatStream) {
        formatStream.close();
      }
    }
  }

  /**
   * Creates and populates a test table of users called 'foo'.
   *
   * @param kijiURI The KijiURI to create the table in.
   * @throws Exception If there is an error.
   */
  public void createAndPopulateFooTable(KijiURI kijiURI) throws Exception {
    // Create the temp files needed to create the foo table.
    final File layoutFile = File.createTempFile("layout", ".json");
    layoutFile.deleteOnExit();
    final File dataFile = File.createTempFile("data", ".csv");
    dataFile.deleteOnExit();
    final File dataFormatFile = File.createTempFile("data-format", ".csv");
    dataFormatFile.deleteOnExit();
    // Write the temp files needed for the test.
    writeFooTableFiles(layoutFile, dataFile, dataFormatFile);

    // Create a foo table.
    String layoutFilename = layoutFile.getPath();
    LOG.info("layout file path: " + layoutFilename);
    ToolResult createResult = runTool(getConf(), new CreateTableTool(), new String[] {
      "--kiji=" + kijiURI,
      "--table=foo",
      "--layout=" + layoutFilename,
    });
    assertEquals(0, createResult.getReturnCode());

    // Add data to foo table.
    final KijiConfiguration kijiConf = new KijiConfiguration(getConf(), kijiURI.getInstance());
    final Kiji kiji = Kiji.open(kijiConf);
    final KijiTable table = kiji.openTable("foo");
    final KijiTableWriter fooWriter = table.openTableWriter();
    try {
      long timestamp = System.currentTimeMillis();
      fooWriter.put(table.getEntityId("gwu@usermail.example.com"), "info", "email", timestamp,
          "gwu@usermail.example.com");
      fooWriter.put(table.getEntityId("gwu@usermail.example.com"), "info", "name", timestamp,
          "Garrett Wu");

      fooWriter.put(table.getEntityId("aaron@usermail.example.com"), "info", "email", timestamp,
          "aaron@usermail.example.com");
      fooWriter.put(table.getEntityId("aaron@usermail.example.com"), "info", "name", timestamp,
          "Aaron Kimball");

      fooWriter.put(table.getEntityId("christophe@usermail.example.com"), "info", "email",
          timestamp, "christophe@usermail.example.com");
      fooWriter.put(table.getEntityId("christophe@usermail.example.com"), "info", "name", timestamp,
          "Christophe Bisciglia");

      fooWriter.put(table.getEntityId("kiyan@usermail.example.com"), "info", "email", timestamp,
          "kiyan@usermail.example.com");
      fooWriter.put(table.getEntityId("kiyan@usermail.example.com"), "info", "name", timestamp,
          "Kiyan Ahmadizadeh");

      fooWriter.put(table.getEntityId("john.doe@gmail.com"), "info", "email", timestamp,
          "john.doe@gmail.com");
      fooWriter.put(table.getEntityId("john.doe@gmail.com"), "info", "name", timestamp,
          "John Doe");

      fooWriter.put(table.getEntityId("jane.doe@gmail.com"), "info", "email", timestamp,
          "jane.doe@gmail.com");
      fooWriter.put(table.getEntityId("jane.doe@gmail.com"), "info", "name", timestamp,
          "Jane Doe");
    } finally {
      IOUtils.closeQuietly(fooWriter);
      IOUtils.closeQuietly(table);
      IOUtils.closeQuietly(kiji);
      layoutFile.delete();
      dataFile.delete();
      dataFormatFile.delete();
    }
  }

  /**
   * Deletes the table created with createAndPopulateFooTable().
   *
   * @param kijiURI The KijiURI to delete the table from.
   * @throws Exception If there is an error.
   */
  public void deleteFooTable(KijiURI kijiURI) throws Exception {
    ToolResult result = runTool(getConf(), new DeleteTableTool(), new String[] {
      "--kiji=" + kijiURI,
      "--table=foo",
      "--confirm",
    });
    assertEquals(0, result.getReturnCode());
  }
}
