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

package org.kiji.schema.cassandra;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.impl.cassandra.CassandraKijiInstaller;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;
import org.kiji.schema.testutil.IntegrationHelper;
import org.kiji.schema.testutil.ToolResult;
import org.kiji.schema.tools.BaseTool;
import org.kiji.schema.util.Debug;
import org.kiji.schema.util.ResourceUtils;

/**
 * A base class for all Cassandra Kiji integration tests.
 *
 * This class sets up a Kiji instance before each test and tears it down afterwards.
 * It assumes there is a Cassandra cluster running already, and that there is a file
 * `cassandra-maven-plugin.properties` on the classpath.  `AbstractCassandraKijiIntegrationTest`
 * extracts from this properties file an IP address and native transport port for the Cassandra
 * cluster, which it uses to form a {@link org.kiji.schema.cassandra.CassandraKijiURI}.  The
 * properties are `cassandra.initialIp` and `cassandra.nativePort`.
 *
 * A user can specify the values for these properties manually to match whatever Cassandra cluster
 * is running in the background, or can assign them properties in a POM file and use the Maven
 * resources plugin to filter those properties into the properties value.  In such a situation,
 * the POM file might look something like:
 *
 * {@literal
 * <properties>
 *   <cassandra.initialIp>127.0.0.1</cassandra.initialIp>
 *   <cassandra.nativePort>9042</cassandra.nativePort>
 * </properties>
 * ...
 * <testResources>
 *   <testResource>
 *     <directory>src/test/resources</directory>
 *     <includes><include>cassandra-maven-plugin.properties</include></includes>
 *     <filtering>true</filtering>
 *   </testResource>
 * </testResources>
 *   <testResource>
 *     <directory>src/test/resources</directory>
 *     <excludes><exclude>cassandra-maven-plugin.properties</exclude></excludes>
 *     <filtering>false</filtering>
 *   </testResource>
 * </testResources>
 *<testResources>
 * }
 * The `src/test/resources/cassandra-maven-plugin.properties` file would then look like:
 * {@literal
 * cassandra.initialIp=${cassandra.initialIp}
 * cassandra.nativePort=${cassandra.nativePort}}
 *
 * After Maven running `mvn clean verify`, the file
 * `target/test-classes/cassandra-maven-plugin.properties` would then look like:
 * {@literal
 * cassandra.initialIp=127.0.0.1
 * cassandra.nativePort=9042}
 *
 * Instead of assigning these values directly in the POM, the user could also use the Maven
 * build helper plugin to find unused ports.
 *
 * To avoid stepping on other Kiji instances, the name of the instance created is
 * a random unique identifier.
 *
 * This class is abstract because it has a lot of boilerplate for setting up integration
 * tests but doesn't actually test anything.
 *
 * The STANDALONE variable controls whether the test creates an embedded HBase and M/R mini-cluster
 * for itself. This allows run a single test in a debugger without external setup.
 *
 */
public abstract class AbstractCassandraKijiIntegrationTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractCassandraKijiIntegrationTest.class);

  /**
   * Name of the property to specify an external HBase instance.
   *
   * From Maven, one may specify an external cluster with:
   *   mvn clean verify \
   *   -Dkiji.test.cassandra.cluster.uri=kiji-cassandra://localhost:2181/cassandrahost/9140
   */
  private static final String BASE_TEST_URI_PROPERTY = "kiji.test.cassandra.cluster.uri";

  /** An integration helper for installing and removing instances. */
  private IntegrationHelper mHelper;

  /** Base URI to use for creating all of the instances for different tests. */
  private static KijiURI mBaseUri;

  private static AtomicInteger mKijiCounter = new AtomicInteger();

  // -----------------------------------------------------------------------------------------------

  /** Test configuration. */
  private Configuration mConf;

  /** The randomly generated URI for the instance. */
  private KijiURI mKijiUri;

  private Kiji mKiji;

  // JUnit requires public, checkstyle disagrees:
  // CSOFF: VisibilityModifierCheck
  /** Test method name (eg. "testFeatureX"). */
  @Rule
  public final TestName mTestName = new TestName();

  /** A temporary directory for test data. */
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();
  // CSON: VisibilityModifierCheck

  /**
   * Creates a Configuration object for the HBase instance to use.
   *
   * @return an HBase configuration to work against.
   */
  protected Configuration createConfiguration() {
    final Configuration conf = HBaseConfiguration.create();

    // Set the mapred.output.dir to a unique temporary directory for each integration test:
    final String tempDir = conf.get("hadoop.tmp.dir");
    assertNotNull(
        "hadoop.tmp.dir must be set in the configuration for integration tests to use.",
        tempDir);
    conf.set("mapred.output.dir", new Path(tempDir, UUID.randomUUID().toString()).toString());

    return conf;
  }

  @BeforeClass
  public static void createBaseUri() {
    final Configuration conf = HBaseConfiguration.create();

    if (System.getProperty(BASE_TEST_URI_PROPERTY) != null) {
      mBaseUri = KijiURI.newBuilder(System.getProperty(BASE_TEST_URI_PROPERTY)).build();
    } else {
      Properties properties = new Properties();
      try {
        // Read the IP address and port from a filtered .properties file.
        InputStream input = AbstractKijiIntegrationTest.class
            .getClassLoader()
            .getResourceAsStream("cassandra-maven-plugin.properties");
        properties.load(input);
        input.close();
        LOG.info(
            "Successfully loaded Cassandra Maven plugin properties from file: ",
            properties.toString()
        );
      } catch (IOException ioe) {
        throw new KijiIOException(
            "Problem loading cassandra-maven-plugin.properties file from the classpath.");
      }

      // Create a Kiji instance.
      mBaseUri = KijiURI.newBuilder(String.format(
          // Note that the .10 matches what is in the Cassandra maven plugin in the POM.
          "kiji-cassandra://%s:%s/%s:%s/",
          conf.get(HConstants.ZOOKEEPER_QUORUM),
          conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT),
          properties.getProperty("cassandra.initialIp"),
          properties.getProperty("cassandra.nativePort")
      )).build();
      LOG.info("Base URI for Cassandra integration tests = ", mBaseUri.toString());
    }
  }

  @Before
  public final void setupKijiIntegrationTest() throws Exception {
    mConf = createConfiguration();

    String instanceName = "it" + mKijiCounter.getAndIncrement();

    mKijiUri = KijiURI.newBuilder(mBaseUri).withInstanceName(instanceName).build();

    LOG.info("Installing to URI " + mKijiUri);

    try {
      CassandraKijiInstaller.get().install(mKijiUri, null);
      LOG.info("Created Kiji instance at " + mKijiUri);
    } catch (IOException ioe) {
      LOG.warn("Could not create Kiji instance.");
      assertTrue("Did not start.", false);
    }

    mKiji = Kiji.Factory.open(mKijiUri);
    Preconditions.checkNotNull(mKiji);

    LOG.info("Setup summary for {}", getClass().getName());
    Debug.logConfiguration(mConf);

    // Get a new Kiji instance, with a randomly-generated name.
    mHelper = new IntegrationHelper(mConf);
  }

  public Kiji getKiji() {
    assertNotNull(mKiji);
    return mKiji;
  }

  @After
  public final void teardownKijiIntegrationTest() throws Exception {
    if (null != mKiji) {
      mKiji.release();
    }
    mKiji = null;
    mHelper = null;
    mKijiUri = null;
    mConf = null;
  }

  /** @return The KijiURI for this test instance. */
  protected KijiURI getKijiURI() {
    return mKijiUri;
  }

  /** @return The name of the instance installed for this test. */
  protected String getInstanceName() {
    return mKijiUri.getInstance();
  }

  /** @return The temporary directory to use for test data. */
  protected File getTempDir() {
    return mTempDir.getRoot();
  }

  /** @return a test Configuration, with a MapReduce and HDFS cluster. */
  public Configuration getConf() {
    return mConf;
  }

  /**
   * Gets the path to a file in the mini HDFS cluster.
   *
   * <p>If the file does not yet exist, it will be copied into the cluster first.</p>
   *
   * @param localResource A local file resource.
   * @return The path to the file in the mini HDFS filesystem.
   * @throws java.io.IOException If there is an error.
   */
  protected Path getDfsPath(URL localResource) throws IOException {
    return getDfsPath(localResource.getPath());
  }


  /**
   * Gets the path to a file in the mini HDFS cluster.
   *
   * <p>If the file does not yet exist, it will be copied into the cluster first.</p>
   *
   * @param localPath A local file path (doesn't have to exist, but if it does it will be copied).
   * @return The path to the file in the mini HDFS filesystem.
   * @throws java.io.IOException If there is an error.
   */
  protected Path getDfsPath(String localPath) throws IOException {
    return getDfsPath(new File(localPath));
  }

  /**
   * Gets the path to a file in the mini HDFS cluster.
   *
   * <p>If the file does not yet exist, it will be copied into the cluster first.</p>
   *
   * @param localFile A local file.
   * @return The path to the file in the mini HDFS filesystem.
   * @throws java.io.IOException If there is an error.
   */
  protected Path getDfsPath(File localFile) throws IOException {
    String uniquePath
        = new File(getClass().getName().replaceAll("\\.\\$", "/"), localFile.getPath()).getPath();
    if (localFile.exists()) {
      return mHelper.copyToDfs(localFile, uniquePath);
    }
    return mHelper.getDfsPath(uniquePath);
  }

  /**
   * Runs a tool within the instance for this test and captures the console output.
   *
   * @param tool The tool to run.
   * @param args The command-line args to pass to the tool.  The --instance flag will be added.
   * @return A result with the captured tool output.
   * @throws Exception If there is an error.
   */
  protected ToolResult runTool(BaseTool tool, String[] args) throws Exception {
    // Append the --instance=<instance-name> flag on the end of the args.
    final String[] argsWithKiji = Arrays.copyOf(args, args.length + 1);
    argsWithKiji[args.length] = "--debug=true";
    return mHelper.runTool(mHelper.getConf(), tool, argsWithKiji);
  }

  /**
   * Creates and populates a test table of users called 'foo'.
   *
   * @throws Exception If there is an error.
   */
  protected void createAndPopulateFooTable() throws Exception {
    mHelper.createAndPopulateFooTable(mKijiUri);
  }

  /**
   * Deletes the table created with createAndPopulateFooTable().
   *
   * @throws Exception If there is an error.
   */
  public void deleteFooTable() throws Exception {
    mHelper.deleteFooTable(mKijiUri);
  }

  /**
   * Formats an exception stack trace into a string.
   *
   * @param exn Exception to format.
   * @return the exception stack trace, as a string.
   */
  protected static String formatException(Exception exn) {
    final StringWriter writer = new StringWriter();
    final PrintWriter printWriter = new PrintWriter(writer);
    exn.printStackTrace(printWriter);
    ResourceUtils.closeOrLog(printWriter);
    return writer.toString();
  }
}
