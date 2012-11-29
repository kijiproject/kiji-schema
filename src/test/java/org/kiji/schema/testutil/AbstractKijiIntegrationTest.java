// (c) Copyright 2011 WibiData, Inc.

package com.wibidata.core;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.kiji.schema.KijiConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wibidata.core.license.SignedLicense;
import com.wibidata.core.testutil.IntegrationHelper;
import com.wibidata.core.testutil.ToolResult;
import com.wibidata.core.tools.WibiTool;

/**
 * A base class for all Wibi integration tests.
 *
 * <p>This class sets up a wibi instance before each test and tears it down afterwards.
 * It assumes there is an HBase cluster running already, and its configuration is on the
 * classpath.</p>
 *
 * <p>To avoid stepping on other wibi instances, the name of the instance created is
 * a random unique identifier.</p>
 *
 * This class is abstract because it doesn't know where to get a license from.
 * Implement the getLicense() method to specify.
 */
public abstract class AbstractWibiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractWibiIntegrationTest.class);

  /** An integration helper for installing and removing instances. */
  private IntegrationHelper mHelper;

  /** The randomly generated name for the instance. */
  private String mInstanceName;

  /** The wibi configuration for this test's private instance. */
  private KijiConfiguration mKijiConf;

  // Disable checkstyle since mTempDir must be a public to work as a JUnit @Rule.
  // CHECKSTYLE:OFF
  /** A temporary directory for test data. */
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();
  // CHECKSTYLE:ON


  // mCreationThread and mDeletionThread are lazily initialized in a @Before
  // handler. This ensures that only one such @Before method does the work.
  private static final Object THREAD_CREATION_LOCK;
  static {
    THREAD_CREATION_LOCK = new Object();
  }

  /** A thread that creates Wibi instances for use by tests. */
  private static WibiCreationThread mCreationThread;
  /** A thread that removes Wibi instances no longer in use by tests. */
  private static WibiDeletionThread mDeletionThread;

  /**
   * Creates a Configuration object for the HBase instance to use.
   *
   * @return an HBase configuration to work against.
   */
  protected Configuration createConfiguration() {
    return HBaseConfiguration.create();
  }

  @Before
  public void setupWibi() throws Exception {
    synchronized (THREAD_CREATION_LOCK) {
      // Create background worker threads if they're not already created.
      if (null == mCreationThread) {
        mCreationThread = new WibiCreationThread();
        mDeletionThread = new WibiDeletionThread();
        mCreationThread.start();
        mDeletionThread.start();
      }
    }

    // Get a new Wibi instance, with a randomly-generated name.
    mInstanceName = mCreationThread.getFreshWibi();
    mHelper = new IntegrationHelper(createConfiguration());

    // Construct a wibi configuration.
    mKijiConf = new KijiConfiguration(mHelper.getConf(), mInstanceName);
  }

  @After
  public void teardownWibi() throws Exception {
    // Schedule the Wibi instance for asynchronous deletion.
    if (null != mInstanceName) {
      mDeletionThread.destroyWibi(mInstanceName);
    }
  }

  /** @return The integration helper. */
  protected IntegrationHelper getIntegrationHelper() {
    return mHelper;
  }

  /** @return The wibi configuration for this test instance. */
  protected KijiConfiguration getKijiConfiguration() {
    return mKijiConf;
  }

  /** @return The name of the instance installed for this test. */
  protected String getInstanceName() {
    return mInstanceName;
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
   * @throws IOException If there is an error.
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
   * @throws IOException If there is an error.
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
   * @throws IOException If there is an error.
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
  protected ToolResult runTool(WibiTool tool, String[] args) throws Exception {
    // Append the --instance=<instance-name> flag on the end of the args.
    String[] argsWithWibi = Arrays.copyOf(args, args.length + 1);
    argsWithWibi[args.length] = "--instance=" + mInstanceName;

    return mHelper.runTool(mHelper.getConf(), tool, argsWithWibi);
  }

  /**
   * Creates and populates a test table of users called 'foo'.
   *
   * @throws Exception If there is an error.
   */
  protected void createAndPopulateFooTable() throws Exception {
    mHelper.createAndPopulateFooTable(mInstanceName);
  }

  /**
   * Deletes the table created with createAndPopulateFooTable().
   *
   * @throws Exception If there is an error.
   */
  public void deleteFooTable() throws Exception {
    mHelper.deleteFooTable(mInstanceName);
  }

  /**
   * Thread that creates Wibi instances for use by integration tests.
   */
  private final class WibiCreationThread extends Thread {
    /** Maintain a pool of this many fresh Wibi instances ready to go. */
    private static final int INSTANCE_POOL_SIZE = 4;

    /** Bounded producer/consumer queue that holds the names of new Wibi instances. */
    private LinkedBlockingQueue<String> mWibiQueue;

    private WibiCreationThread() {
      setName("WibiCreationThread");
      setDaemon(true);
      mWibiQueue = new LinkedBlockingQueue<String>(INSTANCE_POOL_SIZE);
    }

    /**
     * Get a new Wibi instance from the pool. This may block if we're using Wibi
     * instances in tests too fast.
     *
     * @return a String identifying an unused Wibi instance.
     * @throws InterruptedException if the blocking call is interrupted.
     */
    public String getFreshWibi() throws InterruptedException {
      return mWibiQueue.take();
    }

    /** {@inheritDoc} */
    @Override
    public void run() {
      String instanceName = null;

      while (true) {
        if (null == instanceName) {
          // Create a new Wibi instance.
          instanceName = UUID.randomUUID().toString().replaceAll("-", "_");
          final IntegrationHelper intHelper =
              new IntegrationHelper(AbstractWibiIntegrationTest.this.createConfiguration());
          try {
            intHelper.installWibi(instanceName, getLicense());
          } catch (Exception exn) {
            final StringWriter writer = new StringWriter();
            final PrintWriter printWriter = new PrintWriter(writer);
            exn.printStackTrace(printWriter);
            printWriter.close();
            LOG.error(String.format("Exception while installing Wibi instance [%s]: %s\n%s",
                instanceName, exn.toString(), writer.toString()));
            instanceName = null;
            continue;
          }
        }

        if (null == instanceName) {
          LOG.error("Unexpected null wibi instance in mid-loop creation thread");
          continue;
        }

        try {
          // If the queue is full, block til it's not. Then put this one in,
          // and start making the next instance.
          mWibiQueue.put(instanceName);
          instanceName = null; // Forget this instance after we successfully put() it.
        } catch (InterruptedException ie) {
          // Ok, interrupted! Check if we're done or not, and try again.
          continue;
        }
      }
    }
  }

  /**
   * Thread that destroys Wibi instances used by integration tests.
   */
  private final class WibiDeletionThread extends Thread {
    /** Unbounded producer/consumer queue that holds Wibi instances to destroy. */
    private LinkedBlockingQueue<String> mWibiQueue;

    private WibiDeletionThread() {
      setName("WibiDeletionThread");
      setDaemon(true);
      mWibiQueue = new LinkedBlockingQueue<String>();
    }

    /**
     * Add a Wibi instance to the list of Wibi instances to be destroyed.
     * The instance will be destroyed asynchronously.
     *
     * @param wibiName a String identifying a Wibi instance we're done with.
     * @throws InterruptedException if the blocking call is interrupted.
     */
    public void destroyWibi(String wibiName) throws InterruptedException {
      mWibiQueue.put(wibiName);
    }

    /** {@inheritDoc} */
    @Override
    public void run() {
      while (true) {
        String instanceName = null;

        try {
          instanceName = mWibiQueue.take();
        } catch (InterruptedException ie) {
          // Interruption in here is expected; as long as the queue is non-empty,
          // keep trying to remove one.
          continue;
        }

        if (null == instanceName) {
          LOG.warn("Unexpected null wibi instance after take() in destroy thread");
          continue;
        }

        final IntegrationHelper intHelper =
            new IntegrationHelper(AbstractWibiIntegrationTest.this.createConfiguration());
        try {
          intHelper.uninstallWibi(instanceName);
        } catch (Exception e) {
          LOG.error("Could not destroy Wibi instance [" + instanceName + "]: "
              + e.getMessage());
        }
      }
    }
  }

  /**
   * Gets the license to use when installing Wibi instances.
   *
   * @return The license.
   */
  protected abstract SignedLicense getLicense() throws IOException;
}
