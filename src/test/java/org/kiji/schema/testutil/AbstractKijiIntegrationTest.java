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
import org.kiji.schema.KijiURIException;
import org.kiji.schema.tools.BaseTool;

/**
 * A base class for all Kiji integration tests.
 *
 * <p>This class sets up a Kiji instance before each test and tears it down afterwards.
 * It assumes there is an HBase cluster running already, and its configuration is on the
 * classpath.</p>
 *
 * <p>To avoid stepping on other Kiji instances, the name of the instance created is
 * a random unique identifier.</p>
 *
 * This class is abstract because it doesn't know where to get a license from.
 * Implement the getLicense() method to specify.
 */
public abstract class AbstractKijiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractKijiIntegrationTest.class);

  private static final String BASE_TEST_URI_PROPERTY = "kiji.test.cluster.uri";

  /* Semaphore for tracking how many tests are currently running. */
  private static final Semaphore RUNNING_TEST_SEMAPHORE = new Semaphore(0);

  /** An integration helper for installing and removing instances. */
  private IntegrationHelper mHelper;

  /** The URI to be used for running tests on the hbase-maven-plugin. */
  private static final String HBASE_MAVEN_PLUGIN_URI = "kiji://.env/";

  /** Base KijiURI for creating Kiji instances to run integration tests in. */
  private static KijiURI mBaseTestURI;

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
  private static final Object THREAD_MANAGEMENT_LOCK;
  static {
    THREAD_MANAGEMENT_LOCK = new Object();
  }

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
    return HBaseConfiguration.create();
  }

  @BeforeClass
  public static void setupManagementThreads() throws KijiURIException {
    // Determine the base URI for Kiji instances
    if (System.getProperty(BASE_TEST_URI_PROPERTY) != null) {
      mBaseTestURI = KijiURI.parse(System.getProperty(BASE_TEST_URI_PROPERTY));
    } else {
      mBaseTestURI = KijiURI.parse(HBASE_MAVEN_PLUGIN_URI);
    }

    synchronized (THREAD_MANAGEMENT_LOCK) {
      // Create background worker threads if they're not already created.
      if (null == mCreationThread) {
        mCreationThread = new KijiCreationThread(mBaseTestURI);
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
    String[] argsWithKiji = Arrays.copyOf(args, args.length + 1);
    argsWithKiji[args.length] = "--kiji=" + mKijiURI;

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
   * Thread that creates Kiji instances for use by integration tests.
   */
  private static final class KijiCreationThread extends Thread {
    /** Maintain a pool of this many fresh Kiji instances ready to go. */
    private static final int INSTANCE_POOL_SIZE = 4;

    private final KijiURI mBaseTestURI;

    private final IntegrationHelper mIntegrationHelper;
    /** Bounded producer/consumer queue that holds the names of new Kiji instances. */
    private LinkedBlockingQueue<KijiURI> mKijiQueue;

    /** Denotes whether this thread should continue to create Kiji instances. */
    private boolean mActive;

    /** Ensures that this thread is either in running mode or cleanup mode. */
    private static final Object THREAD_CREATION_ACTIVE_LOCK;
    static {
      THREAD_CREATION_ACTIVE_LOCK = new Object();
    }

    private KijiCreationThread(KijiURI baseTestURI) {
      setName("KijiCreationThread");
      setDaemon(true);

      mBaseTestURI = baseTestURI;
      mIntegrationHelper = new IntegrationHelper(HBaseConfiguration.create());
      mKijiQueue = new LinkedBlockingQueue<KijiURI>(INSTANCE_POOL_SIZE);
      mActive = true;
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
        KijiURI kijiURI = null;
        while (mActive) {
          if (null == kijiURI) {
            // Create a new Kiji instance.
            String instanceName = UUID.randomUUID().toString().replaceAll("-", "_");
            final IntegrationHelper intHelper =
                new IntegrationHelper(HBaseConfiguration.create());
            try {
              kijiURI = mBaseTestURI.setInstanceName(instanceName);
              intHelper.installKiji(kijiURI);
            } catch (Exception exn) {
              final StringWriter writer = new StringWriter();
              final PrintWriter printWriter = new PrintWriter(writer);
              exn.printStackTrace(printWriter);
              printWriter.close();
              LOG.error(String.format("Exception while installing Kiji instance [%s]: %s\n%s",
                  instanceName, exn.toString(), writer.toString()));
              instanceName = null;
              continue;
            }
          }

          if (null == kijiURI) {
            LOG.error("Unexpected null Kiji instance in mid-loop creation thread");
            continue;
          }

          try {
            // If the queue is full, block til it's not. Then put this one in,
            // and start making the next instance.
            mKijiQueue.put(kijiURI);
            kijiURI = null; // Forget this instance after we successfully put() it.
          } catch (InterruptedException ie) {
            // Ok, interrupted! Check if we're done or not, and try again.
            continue;
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
    private static final Object THREAD_DELETION_ACTIVE_LOCK;
    static {
      THREAD_DELETION_ACTIVE_LOCK = new Object();
    }

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
