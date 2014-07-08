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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.checkin.CheckinUtils;
import org.kiji.schema.Kiji;
import org.kiji.schema.impl.cassandra.CassandraKijiFactory;
import org.kiji.schema.impl.cassandra.CassandraKijiInstaller;
import org.kiji.schema.util.TestingFileUtils;

/**
 * Base class for Cassandra tests that interact with kiji as a client.
 *
 * <p> Provides MetaTable and KijiSchemaTable access. </p>
 */
public class CassandraKijiClientTest {
  /*
   * <p>
   *   // TODO: Implement ability to connect to a real Cassandra service in tests.
   *   By default, this base class connects to an EmbeddedCassandraService. By setting a JVM
   *   system property, this class may be configured to use a real Cassandra instance. For example,
   *   to use an C* node running on <code>localhost:2181</code>, you may use:
   *   <pre>
   *     mvn clean test \
   *         -DargLine="-Dorg.kiji.schema.CassandraKijiClientTest.CASSANDRA_ADDRESS=localhost:2181"
   *   </pre>
   * </p>
   */
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiClientTest.class);

  //static { SchemaPlatformBridge.get().initializeHadoopResources(); }

  /**
   * Externally configured address of a C* cluster to use for testing.
   * Null when unspecified, which means use EmbeddedCassandraService.
   */
  private static final String CASSANDRA_ADDRESS =
      System.getProperty("org.kiji.schema.CassandraKijiClientTest.CASSANDRA_ADDRESS", null);

  // JUnit requires public, checkstyle disagrees:
  // CSOFF: VisibilityModifierCheck
  /** Test method name (eg. "testFeatureX"). */
  @Rule
  public final TestName mTestName = new TestName();
  // CSON: VisibilityModifierCheck

  /** Counter for fake C* instances. */
  private static final AtomicLong FAKE_CASSANDRA_INSTANCE_COUNTER = new AtomicLong();

  /** Counter for test Kiji instances. */
  private static final AtomicLong KIJI_INSTANCE_COUNTER = new AtomicLong();

  /** Test identifier, eg. "org_package_ClassName_testMethodName". */
  private String mTestId;

  /** Kiji instances opened during test, and that must be released and cleaned up after. */
  private List<Kiji> mAllKijis = Lists.newArrayList();

  /** Local temporary directory, automatically cleaned up after. */
  private File mLocalTempDir = null;

  /** Default test Kiji instance. */
  private Kiji mKiji = null;

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
    LOG.info("Setting up Cassandra Kiji client tests...");
    mTestId =
        String.format("%s_%s", getClass().getName().replace('.', '_'), mTestName.getMethodName());
    mLocalTempDir = TestingFileUtils.createTempDir(mTestId, "temp-dir");
    mKiji = null;  // lazily initialized
    // Disable logging of commands to the upgrade server by accident.
    System.setProperty(CheckinUtils.DISABLE_CHECKIN_PROP, "true");
  }

  /**
   * Creates a test C* URI.
   *
   * @return the KijiURI of a test HBase instance.
   */
  public CassandraKijiURI createTestCassandraURI() {
    final long fakeCassandraCounter = FAKE_CASSANDRA_INSTANCE_COUNTER.getAndIncrement();
    final String testName = String.format(
        "%s_%s",
        getClass().getSimpleName(),
        mTestName.getMethodName()
    );

    // Goes into the ZooKeeper section of the URI.
    final String cassandraAddress =
        (CASSANDRA_ADDRESS != null)
        ? CASSANDRA_ADDRESS
        : String.format(".fake.%s-%d", testName, fakeCassandraCounter);

    CassandraKijiURI uri = CassandraKijiURI
        .newBuilder(String.format("kiji-cassandra://%s/localhost/9042", cassandraAddress))
        .build();
    LOG.info("Created test Cassandra URI: " + uri);
    return uri;
  }

  /**
   * Opens a new unique test Kiji instance, creating it if necessary.
   *
   * Each call to this method returns a fresh new Kiji instance.
   * All generated Kiji instances are automatically cleaned up by CassandraKijiClientTest.
   *
   * @return a fresh new Kiji instance.
   * @throws Exception on error.
   */
  public Kiji createTestKiji() throws Exception {
    // Note: The C* keyspace for the instance has to be less than 48 characters long. Every C*
    // Kiji keyspace starts with "kiji_", so we have a total of 43 characters to work with - yikes!
    // Hopefully dropping off the class name is good enough to make this short enough.

    final String instanceName =
        String.format("%s_%d", mTestName.getMethodName(), KIJI_INSTANCE_COUNTER.getAndIncrement());

    LOG.info("Creating a test Kiji instance.  Calling Kiji instance " + instanceName);

    final CassandraKijiURI kijiURI = createTestCassandraURI();
    final CassandraKijiURI instanceURI =
        CassandraKijiURI.newBuilder(kijiURI).withInstanceName(instanceName).build();
    LOG.info("Installing fake C* instance " + instanceURI);
    CassandraKijiInstaller.get().install(instanceURI, null);
    final Kiji kiji = CassandraKijiFactory.get().open(instanceURI);

    mAllKijis.add(kiji);
    return kiji;
  }

  /**
   * Closes the in-memory kiji instance.
   * @throws Exception If there is an error.
   */
  @After
  public final void tearDownKijiTest() throws Exception {
    LOG.debug("Tearing down {}", mTestId);
    for (Kiji kiji : mAllKijis) {
      kiji.release();
      CassandraKijiInstaller.get().uninstall(kiji.getURI(), null);
    }
    mAllKijis = null;
    mKiji = null;
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
   * @throws java.io.IOException on I/O error.  Should be Exception, but breaks too many tests for
   *     now.
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

  /** @return a valid identifier for the current test. */
  public String getTestId() {
    return mTestId;
  }

  /** @return a local temporary directory. */
  public File getLocalTempDir() {
    return mLocalTempDir;
  }
}
