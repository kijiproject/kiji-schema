/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.schema;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.checkin.CheckinUtils;
import org.kiji.schema.impl.async.AsyncKiji;
import org.kiji.schema.impl.async.AsyncKijiFactory;
import org.kiji.schema.platform.SchemaPlatformBridge;
import org.kiji.schema.util.TestingFileUtils;

/**
 * Base class for tests that interact with kiji as a client.
 *
 * <p> Provides MetaTable and KijiSchemaTable access. </p>
 * <p>
 *   By default, this base class generate fake HBase instances, for testing.
 *   By setting a JVM system property, this class may be configured to use a real HBase instance.
 *   For example, to use an HBase mini-cluster running on <code>localhost:2181</code>, you may use:
 *   <pre>
 *     mvn clean test \
 *         -DargLine="-Dorg.kiji.schema.KijiClientTest.HBASE_ADDRESS=localhost:2181"
 *   </pre>
 * </p>
 */
public class KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(KijiClientTest.class);

  static {
    SchemaPlatformBridge.get().initializeHadoopResources();
  }

  /**
   * Externally configured address of an HBase cluster to use for testing.
   * Null when unspecified, which means use fake HBase instances.
   */
  private static final String HBASE_ADDRESS =
      System.getProperty("org.kiji.schema.KijiClientTest.HBASE_ADDRESS", null);

  // JUnit requires public, checkstyle disagrees:
  // CSOFF: VisibilityModifierCheck
  /** Test method name (eg. "testFeatureX"). */
  @Rule
  public final TestName mTestName = new TestName();
  // CSON: VisibilityModifierCheck

  /** Counter for fake HBase instances. */
  private final AtomicLong mFakeHBaseInstanceCounter = new AtomicLong();

  /** Counter for test Kiji instances. */
  private final AtomicLong mKijiInstanceCounter = new AtomicLong();

  /** Test identifier, eg. "org_package_ClassName_testMethodName". */
  private String mTestId;

  /** Kiji instances opened during test, and that must be released and cleaned up after. */
  private List<Kiji> mKijis = Lists.newArrayList();

  /** Local temporary directory, automatically cleaned up after. */
  private File mLocalTempDir = null;

  /** Default test Kiji instance. */
  private Kiji mKiji = null;

  /** Default test AsyncKiji instance. */
  private Kiji mAsyncKiji = null;

  /** The configuration object for this kiji instance. */
  private Configuration mConf;

  /**
   * Initializes the in-memory kiji for testing.
   *
   * @throws Exception on error.
   */
  @Before
  public final void setupKijiTest() throws Exception {
    try {
      doSetupKijiTest();
    } catch (Exception exn) {
      // Make exceptions from setup method visible:
      exn.printStackTrace();
      throw exn;
    }
  }

  private void doSetupKijiTest() throws Exception {
    mTestId =
        String.format("%s_%s", getClass().getName().replace('.', '_'), mTestName.getMethodName());
    mLocalTempDir = TestingFileUtils.createTempDir(mTestId, "temp-dir");
    mConf = HBaseConfiguration.create();
    mConf.set("fs.defaultFS", "file://" + mLocalTempDir);
    mConf.set("mapred.job.tracker", "local");
    mKiji = null;  // lazily initialized
    // Disable logging of commands to the upgrade server by accident.
    System.setProperty(CheckinUtils.DISABLE_CHECKIN_PROP, "true");
  }

  /**
   * Creates a test HBase URI.
   *
   * <p>
   *   This HBase instance is ideally made unique for each test, but there is no hard guarantee.
   *   In particular, the HBase instance is shared with other tests when running against an
   *   external HBase cluster.
   *   Thus, you must clean after yourself by removing tables you create in your tests.
   * </p>
   *
   * @return the KijiURI of a test HBase instance.
   */
  public KijiURI createTestHBaseURI() {
    final long fakeHBaseCounter = mFakeHBaseInstanceCounter.getAndIncrement();
    final String testName =
        String.format("%s_%s", getClass().getSimpleName(), mTestName.getMethodName());
    final String hbaseAddress =
        (HBASE_ADDRESS != null)
        ? HBASE_ADDRESS
        : String.format(".fake.%s-%d", testName, fakeHBaseCounter);
    return KijiURI.newBuilder(String.format("kiji://%s", hbaseAddress)).build();
  }

  /**
   * Creates and opens a new unique test Kiji instance in a new fake HBase cluster.  All generated
   * Kiji instances are automatically cleaned up by KijiClientTest.
   *
   * @return a fresh new Kiji instance in a new fake HBase cluster.
   * @throws Exception on error.
   */
  public Kiji createTestKiji() throws Exception {
    return createTestKiji(createTestHBaseURI());
  }

  /**
   * Creates and opens a new unique test Kiji instance in the specified cluster.  All generated
   * Kiji instances are automatically cleaned up by KijiClientTest.
   *
   * @param clusterURI of cluster create new instance in.
   * @return a fresh new Kiji instance in the specified cluster.
   * @throws Exception on error.
   */
  public Kiji createTestKiji(KijiURI clusterURI) throws Exception {
    Preconditions.checkNotNull(mConf);
    final String instanceName = String.format("%s_%s_%d",
        getClass().getSimpleName(),
        mTestName.getMethodName(),
        mKijiInstanceCounter.getAndIncrement());
    final KijiURI uri = KijiURI.newBuilder(clusterURI).withInstanceName(instanceName).build();
    KijiInstaller.get().install(uri, mConf);
    final Kiji kiji = Kiji.Factory.open(uri, mConf);

    mKijis.add(kiji);
    return kiji;
  }

  /**
   * Creates and opens a new unique test AsyncKiji instance in a new fake HBase cluster.  All generated
   * AsyncKiji instances are automatically cleaned up by KijiClientTest.
   *
   * @return a fresh new AsyncKiji instance in a new fake HBase cluster.
   * @throws Exception on error.
   */
  public Kiji createTestAsyncKiji() throws Exception {
    return createTestAsyncKiji(createTestHBaseURI());
  }

  /**
   * Creates and opens a new unique test AsyncKiji instance in the specified cluster.  All generated
   * AsyncKiji instances are automatically cleaned up by KijiClientTest.
   *
   * @param clusterURI of cluster create new instance in.
   * @return a fresh new AsyncKiji instance in the specified cluster.
   * @throws Exception on error.
   */
  public Kiji createTestAsyncKiji(KijiURI clusterURI) throws Exception {
    Preconditions.checkNotNull(mConf);
    final String instanceName = String.format("%s_%s_%d",
        getClass().getSimpleName(),
        mTestName.getMethodName(),
        mKijiInstanceCounter.getAndIncrement());
    final KijiURI uri = KijiURI.newBuilder(clusterURI).withInstanceName(instanceName).build();
    KijiInstaller.get().install(uri, mConf);
    final Kiji kiji = new AsyncKijiFactory().open(uri);

    mKijis.add(kiji);
    return kiji;
  }

  /**
   * Deletes a test Kiji instance. The <code>Kiji</code> reference provided to this method will no
   * longer be valid after it returns (it will be closed).  Calling this method on Kiji instances
   * created through KijiClientTest is not necessary, it is provided for testing situations in which
   * a Kiji is explicitly closed.
   *
   * @param kiji instance to be closed and deleted.
   * @throws Exception on error.
   */
  public void deleteTestKiji(Kiji kiji) throws Exception {
    Preconditions.checkState(mKijis.contains(kiji));
    kiji.release();
    KijiInstaller.get().uninstall(kiji.getURI(), mConf);
    mKijis.remove(kiji);
  }

  /**
   * Closes the in-memory kiji instance.
   * @throws Exception If there is an error.
   */
  @After
  public final void teardownKijiTest() throws Exception {
    LOG.debug("Tearing down {}", mTestId);
    for (Kiji kiji : mKijis) {
      kiji.release();
      KijiInstaller.get().uninstall(kiji.getURI(), mConf);
    }
    mKijis = null;
    mKiji = null;
    mConf = null;
    FileUtils.deleteDirectory(mLocalTempDir);
    mLocalTempDir = null;
    mTestId = null;

    // Force a garbage collection, to trigger finalization of resources and spot
    // resources that were not released or closed.
    System.gc();
    System.runFinalization();
  }

  /**
   * Gets the default Kiji instance to use for testing.
   *
   * @return the default Kiji instance to use for testing.
   *     Automatically released by KijiClientTest.
   * @throws IOException on I/O error.  Should be Exception, but breaks too many tests for now.
   */
  public synchronized Kiji getKiji() throws IOException {
    if (null == mKiji) {
      try {
        mKiji = createTestKiji();
      } catch (IOException ioe) {
        throw ioe;
      } catch (Exception exn) {
        // TODO: Remove wrapping:
        throw new IOException(exn);
      }
    }
    return mKiji;
  }

  /**
   * Gets the default AsyncKiji instance to use for testing.
   *
   * @return the default AsyncKiji instance to use for testing.
   *     Automatically released by KijiClientTest.
   * @throws IOException on I/O error.  Should be Exception, but breaks too many tests for now.
   */
  public synchronized Kiji getAsyncKiji() throws IOException {
    if (null == mAsyncKiji) {
      try {
        mAsyncKiji = createTestAsyncKiji();
      } catch (IOException ioe) {
        throw ioe;
      } catch (Exception exn) {
        // TODO: Remove wrapping:
        throw new IOException(exn);
      }
    }
    return mAsyncKiji;
  }

  /** @return a valid identifier for the current test. */
  public String getTestId() {
    return mTestId;
  }

  /** @return a local temporary directory. */
  public File getLocalTempDir() {
    return mLocalTempDir;
  }

  /**
   * @return a test Hadoop configuration, with:
   *     <li> a default FS
   *     <li> a job tracker
   */
  public Configuration getConf() {
    return mConf;
  }
}
