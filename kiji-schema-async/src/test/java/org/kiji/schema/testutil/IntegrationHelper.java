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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiInstaller;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.tools.BaseTool;
import org.kiji.schema.tools.KijiToolLauncher;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.schema.util.ResourceUtils;

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
    KijiInstaller.get().install(kijiURI, getConf());
  }

  /**
   * Uninstall a Kiji instance.
   *
   * @throws Exception If there is an error.
   */
  public void uninstallKiji(KijiURI kijiURI) throws Exception {
    KijiInstaller.get().uninstall(kijiURI, getConf());
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
        ResourceUtils.closeOrLog(outStream);
      }
    }
  }

  /**
   * Writes the files necessary to create the foo table.
   *
   * @param dataFile A file to hold the table data.
   * @param formatFile A file to hold the data format.
   * @throws IOException If there is an error while writing the files.
   */
  private void writeFooTableFiles(File dataFile, File formatFile) throws IOException {
    InputStream dataStream = null;
    InputStream formatStream = null;
    try {
      dataStream = getClass().getClassLoader().getResourceAsStream(INPUT_FILE);
      formatStream = getClass().getClassLoader().getResourceAsStream(INPUT_FORMAT_FILE);

      writeTempFile(dataFile, dataStream);
      writeTempFile(formatFile, formatStream);
    } finally {
      if (null != dataStream) {
        ResourceUtils.closeOrLog(dataStream);
      }
      if (null != formatStream) {
        ResourceUtils.closeOrLog(formatStream);
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
    final Kiji kiji = Kiji.Factory.open(kijiURI, getConf());
    try {
      final KijiTableLayout layout = KijiTableLayouts.getTableLayout(KijiTableLayouts.FOO_TEST);
      final long timestamp = System.currentTimeMillis();

      new InstanceBuilder(kiji)
          .withTable(layout.getName(), layout)
              .withRow("gwu@usermail.example.com")
                  .withFamily("info")
                      .withQualifier("email").withValue(timestamp, "gwu@usermail.example.com")
                      .withQualifier("name").withValue(timestamp, "Garrett Wu")
              .withRow("aaron@usermail.example.com")
                  .withFamily("info")
                      .withQualifier("email").withValue(timestamp, "aaron@usermail.example.com")
                      .withQualifier("name").withValue(timestamp, "Aaron Kimball")
              .withRow("christophe@usermail.example.com")
                  .withFamily("info")
                      .withQualifier("email")
                          .withValue(timestamp, "christophe@usermail.example.com")
                      .withQualifier("name").withValue(timestamp, "Christophe Bisciglia")
              .withRow("kiyan@usermail.example.com")
                  .withFamily("info")
                      .withQualifier("email").withValue(timestamp, "kiyan@usermail.example.com")
                      .withQualifier("name").withValue(timestamp, "Kiyan Ahmadizadeh")
              .withRow("john.doe@gmail.com")
                  .withFamily("info")
                      .withQualifier("email").withValue(timestamp, "john.doe@gmail.com")
                      .withQualifier("name").withValue(timestamp, "John Doe")
              .withRow("jane.doe@gmail.com")
                  .withFamily("info")
                      .withQualifier("email").withValue(timestamp, "jane.doe@gmail.com")
                      .withQualifier("name").withValue(timestamp, "Jane Doe")
          .build();

    } finally {
      kiji.release();
    }

    // Create the temp files needed to populate the foo table.
    final File dataFile = File.createTempFile("data", ".csv");
    dataFile.deleteOnExit();
    final File dataFormatFile = File.createTempFile("data-format", ".csv");
    dataFormatFile.deleteOnExit();
    // Write the temp files needed for the test.
    writeFooTableFiles(dataFile, dataFormatFile);
    dataFile.delete();
    dataFormatFile.delete();
  }

  /**
   * Deletes the table created with createAndPopulateFooTable().
   *
   * @param kijiURI The KijiURI to delete the table from.
   * @throws Exception If there is an error.
   */
  public void deleteFooTable(KijiURI kijiURI) throws Exception {
    final Kiji kiji = Kiji.Factory.open(kijiURI, getConf());
    try {
      kiji.deleteTable("foo");
    } finally {
      kiji.release();
    }
  }
}
