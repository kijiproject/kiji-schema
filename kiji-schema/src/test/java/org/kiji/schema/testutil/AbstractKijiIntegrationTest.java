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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiURI.KijiURIBuilder;
import org.kiji.schema.KijiURIException;
import org.kiji.schema.tools.BaseTool;

/**
 * A base class for all Kiji integration tests.
 *
 * <p>
 *   This class sets up a Kiji instance before each test and tears it down afterwards.
 *   It assumes there is an HBase cluster running already, and its configuration is on the
 *   classpath.
 * </p>
 *
 * <p>
 *   To avoid stepping on other Kiji instances, the name of the instance created is
 *   a random unique identifier.
 * </p>
 *
 * This class is abstract because it has a lot of boilerplate for setting up integration
 * tests but doesn't actually test anything.
 *
 * The STANDALONE variable controls whether the test creates an embedded HBase and M/R mini-cluster
 * for itself. This allows run a single test in a debugger without external setup.
 */
public abstract class AbstractKijiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractKijiIntegrationTest.class);

  /** Whether to start an embedded mini HBase and M/R cluster. */
  private static final boolean STANDALONE =
      Boolean.parseBoolean(System.getProperty("kiji.test.standalone", "false"));

  /**
   * Name of the property to specify an external HBase instance.
   *
   * From Maven, one may specify an external HBase cluster with:
   *   mvn clean verify -Dkiji.test.cluster.uri=kiji://localhost:2181
   */
  private static final String BASE_TEST_URI_PROPERTY = "kiji.test.cluster.uri";

  /* Semaphore for tracking how many tests are currently running. */
  private static final Semaphore RUNNING_TEST_SEMAPHORE = new Semaphore(0);

  /** An integration helper for installing and removing instances. */
  private IntegrationHelper mHelper;

  /** The URI to be used for running tests on the hbase-maven-plugin. */
  private static final String HBASE_MAVEN_PLUGIN_URI = "kiji://.env/";

  private static KijiURI mHBaseURI;

  // -----------------------------------------------------------------------------------------------
  // Stand-alone integration test setup

  private static HBaseTestingUtility mHBaseUtil;

  /** Configuration created by the standalone setup, if enabled. */
  private static Configuration mStandaloneConf = null;

  /** Mini HBase cluster instance. */
  private static MiniHBaseCluster mCluster;

  /** Mini ZooKeeper cluster instance. */
  private static MiniZooKeeperCluster mZooKeeperCluster;

  private static void startHBaseMiniCluster() throws Exception {
    LOG.info("Starting MiniCluster");

    /** Mini HBase utility helper. */
    mHBaseUtil = new HBaseTestingUtility();

    /** HBase configuration. */
    mStandaloneConf = mHBaseUtil.getConfiguration();

    mZooKeeperCluster = mHBaseUtil.startMiniZKCluster();
    int zkClientPort = mHBaseUtil.getConfiguration().getInt(
        "hbase.zookeeper.property.clientPort", 0);
    LOG.info(String.format("Mini ZooKeeper cluster quorum: localhost:%d",
        zkClientPort));

    // Randomize HBase master info port:
    mStandaloneConf.set("hbase.master.info.port", "0");

    // Disable HBase region server info port:
    mStandaloneConf.set("hbase.regionserver.info.port", "-1");

    mCluster = mHBaseUtil.startMiniCluster();
    mCluster.waitForActiveAndReadyMaster();
    LOG.info("Mini HBase cluster is ready");

    LOG.info("Mini Kiji instance is ready");

    LOG.info("Starting mini map/reduce cluster");
    mStandaloneConf.set("hadoop.log.dir", "/tmp/test_hadoop_log_dir");
    mHBaseUtil.startMiniMapReduceCluster();

    LOG.info(String.format("Job tracker on %s", mStandaloneConf.get("mapred.job.tracker")));
  }

  // -----------------------------------------------------------------------------------------------

  /** The randomly generated URI for the instance. */
  private KijiURI mKijiURI;

  /** The Kiji configuration for this test's private instance. */
  private KijiConfiguration mKijiConf;

  // Disable checkstyle since mTempDir must be a public to work as a JUnit @Rule.
  // CSOFF: VisibilityModifierCheck
  /** A temporary directory for test data. */
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();
  // CSON: VisibilityModifierCheck

  // mCreationThread and mDeletionThread are lazily initialized in a @BeforeClass
  // handler. This ensures that only one such @Before method does the work.
  private static final Object THREAD_MANAGEMENT_LOCK = new Object();

  /** A thread that creates Kiji instances for use by tests. */
  private static KijiCreationThread mCreationThread;
  /** A thread that removes Kiji instances no longer in use by tests. */
  private static KijiDeletionThread mDeletionThread;

  /**
   * Creates a Configuration object for the HBase instance to use.
   *
   * @return an HBase configuration to work against.
   */
  protected Configuration createConfiguration() {
    if (null != mStandaloneConf) {
      return HBaseConfiguration.create(mStandaloneConf);
    }
    return HBaseConfiguration.create();
  }

  /**
   * Determine the URI for the HBase instance to use.
   *
   * @return the URI for the HBase instance to use.
   */
  protected static KijiURI getHBaseURI() throws KijiURIException {
   if (STANDALONE) {
     // We are running an embedded HBase and M/R mini-cluster in-process:
      final Configuration conf = HBaseConfiguration.create(mStandaloneConf);
      final String quorum = conf.get(HConstants.ZOOKEEPER_QUORUM);
      final int clientPort =
          conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT);
      return KijiURI.parse(String.format("kiji://%s:%d", quorum, clientPort));
    }
    if (System.getProperty(BASE_TEST_URI_PROPERTY) != null) {
      return KijiURI.parse(System.getProperty(BASE_TEST_URI_PROPERTY));
    } else {
      return KijiURI.parse(HBASE_MAVEN_PLUGIN_URI);
    }
  }

  @BeforeClass
  public static void setupManagementThreads() throws Exception {
    if (STANDALONE) {
      startHBaseMiniCluster();
    }
    mHBaseURI = getHBaseURI();

    synchronized (THREAD_MANAGEMENT_LOCK) {
      // Create background worker threads if they're not already created.
      if (null == mCreationThread) {
        mCreationThread = new KijiCreationThread(mHBaseURI);
        mDeletionThread = new KijiDeletionThread();
        mCreationThread.start();
        mDeletionThread.start();
      }
      RUNNING_TEST_SEMAPHORE.release();
    }
  }

  @AfterClass
  public static void teardownManagementThreads() {
    synchronized (THREAD_MANAGEMENT_LOCK) {
      // Should always have a permit available since each thread offers one
      RUNNING_TEST_SEMAPHORE.tryAcquire();

      // Last one out shuts off the lights.
      if (RUNNING_TEST_SEMAPHORE.availablePermits() == 0) {
        // Put the remaining created instances into the deletion queue
        mCreationThread.stopKijiCreation();
        KijiURI unusedKijiURI = mCreationThread.getKijiForCleanup();
        while (null != unusedKijiURI) {
          try {
            mDeletionThread.destroyKiji(unusedKijiURI);
          } catch (InterruptedException e) {
            LOG.error("Failed to put Kiji instance [" + unusedKijiURI.toString()
                + "] into the deletion queue: " + e.getMessage());
            continue;
          }
          unusedKijiURI = mCreationThread.getKijiForCleanup();
        }

        // Finish deleting and clear the threads
        mDeletionThread.waitForCompletion();
        mCreationThread = null;
        mDeletionThread = null;

        // Force garbage compaction to find any remaining references to
        // opened tables, etc.
        System.gc();
        System.runFinalization();
      }
    }
  }

  @Before
  public void setupKiji() throws Exception {

    // Get a new Kiji instance, with a randomly-generated name.
    mKijiURI = mCreationThread.getFreshKiji();
    mHelper = new IntegrationHelper(createConfiguration());

    // Construct a Kiji configuration.
    mKijiConf = new KijiConfiguration(mHelper.getConf(), mKijiURI.getInstance());
  }

  @After
  public void teardownKiji() throws Exception {
    // Schedule the Kiji instance for asynchronous deletion.
    if (null != mKijiURI) {
      mDeletionThread.destroyKiji(mKijiURI);
    }
  }

  /** @return The integration helper. */
  protected IntegrationHelper getIntegrationHelper() {
    return mHelper;
  }

  /** @return The Kiji configuration for this test instance. */
  protected KijiConfiguration getKijiConfiguration() {
    return mKijiConf;
  }

  /** @return The KijiURI for this test instance. */
  protected KijiURI getKijiURI() {
    return mKijiURI;
  }

  /** @return The name of the instance installed for this test. */
  protected String getInstanceName() {
    return mKijiURI.getInstance();
  }

  /** @return The temporary directory to use for test data. */
  protected File getTempDir() {
    return mTempDir.getRoot();
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
    String[] argsWithKiji = Arrays.copyOf(args, args.length + 2);
    argsWithKiji[args.length] = "--kiji=" + mKijiURI;
    argsWithKiji[args.length + 1] = "--debug=true";

    return mHelper.runTool(mHelper.getConf(), tool, argsWithKiji);
  }

  /**
   * Creates and populates a test table of users called 'foo'.
   *
   * @throws Exception If there is an error.
   */
  protected void createAndPopulateFooTable() throws Exception {
    mHelper.createAndPopulateFooTable(mKijiURI);
  }

  /**
   * Deletes the table created with createAndPopulateFooTable().
   *
   * @throws Exception If there is an error.
   */
  public void deleteFooTable() throws Exception {
    mHelper.deleteFooTable(mKijiURI);
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
    printWriter.close();
    return writer.toString();
  }

  /**
   * Thread that creates Kiji instances for use by integration tests.
   */
  private static final class KijiCreationThread extends Thread {
    /** Maintain a pool of this many fresh Kiji instances ready to go. */
    private static final int INSTANCE_POOL_SIZE = 4;

    /** Ensures that this thread is either in running mode or cleanup mode. */
    private static final Object THREAD_CREATION_ACTIVE_LOCK = new Object();

    /** Bounded producer/consumer queue that holds the names of new Kiji instances. */
    private final LinkedBlockingQueue<KijiURI> mKijiQueue =
        new LinkedBlockingQueue<KijiURI>(INSTANCE_POOL_SIZE);

    private final KijiURI mHBaseURI;

    /** Denotes whether this thread should continue to create Kiji instances. */
    private boolean mActive = true;


    private KijiCreationThread(KijiURI hbaseURI) {
      setName("KijiCreationThread");
      setDaemon(true);
      mHBaseURI = hbaseURI;
    }

    /**
     * Get a new Kiji instance from the pool. This may block if we're using Kiji
     * instances in tests too fast.
     *
     * @return a KijiURI identifying an unused Kiji instance.
     * @throws InterruptedException if the blocking call is interrupted.
     */
    public KijiURI getFreshKiji() throws InterruptedException {
      return mKijiQueue.take();
    }

    /**
     * Get a remaining Kiji instance from the pool for cleaning up. This may block if we're out
     * of Kiji instances and there are still instances being created
     *
     * Package private as this should only be use for cleanup
     *
     * @return a KijiURI identifying an unused Kiji instance for cleaning up
     */
    KijiURI getKijiForCleanup() {
      if (!mKijiQueue.isEmpty()) {
        return mKijiQueue.poll();
      }
      synchronized (THREAD_CREATION_ACTIVE_LOCK) {
        return mKijiQueue.poll();
      }
    }

    /**
     * Stops the creation of new Kiji instances.  The creation thread will finish creating any
     * instances that are still in progress.
     */
    public void stopKijiCreation() {
      mActive = false;
    }

    /** {@inheritDoc} */
    @Override
    public void run() {
      synchronized (THREAD_CREATION_ACTIVE_LOCK) {
        while (mActive) {
          // Create a new Kiji instance.
          final String instanceName = UUID.randomUUID().toString().replaceAll("-", "_");
          final IntegrationHelper intHelper = new IntegrationHelper(HBaseConfiguration.create());
          try {
            final KijiURI kijiURI =
                KijiURIBuilder.createFromKijiURI(mHBaseURI).withInstanceName(instanceName).build();
            intHelper.installKiji(kijiURI);

            // This blocks if the queue is full:
            mKijiQueue.put(kijiURI);

          } catch (Exception exn) {
            LOG.error(String.format("Exception while installing Kiji instance [%s]: %s\n%s",
                instanceName, exn.toString(), formatException(exn)));
          }
        }
      } // THREAD_CREATION_ACTIVE_LOCK
    }
  }

  /**
   * Thread that destroys Kiji instances used by integration tests.
   */
  private static final class KijiDeletionThread extends Thread {
    /** Unbounded producer/consumer queue that holds Kiji instances to destroy. */
    private LinkedBlockingQueue<KijiURI> mKijiQueue;

    private boolean mActive;

    /** Ensures that this thread is either in running mode or cleanup mode. */
    private static final Object THREAD_DELETION_ACTIVE_LOCK = new Object();

    private KijiDeletionThread() {
      setName("KijiDeletionThread");
      setDaemon(true);
      mKijiQueue = new LinkedBlockingQueue<KijiURI>();
      mActive = true;
    }

    /**
     * Add a Kiji instance to the list of Kiji instances to be destroyed.
     * The instance will be destroyed asynchronously.
     *
     * @param kijiURI a String identifying a Kiji instance we're done with.
     * @throws InterruptedException if the blocking call is interrupted.
     */
    public void destroyKiji(KijiURI kijiURI) throws InterruptedException {
      mKijiQueue.put(kijiURI);
    }

    /**
     * Tells the deletion thread to stop running and waits until all remaining instances have
     * been uninstalled.
     */
    public void waitForCompletion() {
      mActive = false;
      synchronized (THREAD_DELETION_ACTIVE_LOCK) {
        return;
      }
    }

    /** {@inheritDoc} */
    @Override
    public void run() {
      IntegrationHelper intHelper = new IntegrationHelper(HBaseConfiguration.create());
      synchronized (THREAD_DELETION_ACTIVE_LOCK) {
        while (!mKijiQueue.isEmpty() || mActive) {
          KijiURI instanceURI = null;

          try {
            instanceURI = mKijiQueue.take();
          } catch (InterruptedException ie) {
            // Interruption in here is expected; as long as the queue is non-empty,
            // keep trying to remove one.
            continue;
          }

          if (null == instanceURI) {
            LOG.warn("Unexpected null Kiji instance after take() in destroy thread");
            continue;
          }

          try {
            intHelper.uninstallKiji(instanceURI);
          } catch (Exception e) {
            LOG.error("Could not destroy Kiji instance [" + instanceURI.toString() + "]: "
                + e.getMessage());
          }
        }
      }
    }
  }

}
