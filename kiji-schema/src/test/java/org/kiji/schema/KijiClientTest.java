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

package org.kiji.schema;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for tests that interact with kiji as a client.
 * Provides MetaTable and KijiSchemaTable access.
 */
public class KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(KijiClientTest.class);
  private static final AtomicLong INSTANCE_COUNTER = new AtomicLong();

  /** An in-memory kiji instance. */
  private Kiji mKiji;

  /** An in-memory kiji admin instance. */
  private KijiAdmin mKijiAdmin;

  /** The URI of the in-memory kiji instance. */
  private KijiURI mURI;

  /** The configuration object for this kiji instance. */
  private Configuration mConf;

  /**
   * Initializes the in-memory kiji for testing.
   *
   * @throws Exception on error.
   */
  @Before
  public void setUp() throws Exception {
    try {
      doSetUp();
    } catch (Exception exn) {
      // Make exceptions in setUp() visible:
      exn.printStackTrace();
      throw exn;
    }
  }

  private void doSetUp() throws Exception {
    mConf = HBaseConfiguration.create();
    final long id = INSTANCE_COUNTER.getAndIncrement();
    String instanceName = getClass().getSimpleName() + "_test_instance";
    mURI = KijiURI.parse(String.format("kiji://.fake.%d/" + instanceName, id));
    KijiInstaller.install(mURI, mConf);

    mKiji = Kiji.Factory.open(mURI, mConf);
    mKijiAdmin = getKiji().getAdmin();
  }

  /**
   * Closes the in-memory kiji instance.
   * @throws Exception If there is an error.
   */
  @After
  public void tearDown() throws Exception {
    LOG.debug("Closing mock kiji instance");
    mKiji.release();
    mKiji = null;
    mKijiAdmin = null;
    mURI = null;
    mConf = null;
  }

  /**
   * Gets the kiji instance for testing.
   *
   * @return the test kiji instance. No need to release.
   */
  protected Kiji getKiji() {
    return mKiji;
  }

  /**
   * Gets the kiji admin instance for testing.
   *
   * @return The test kiji admin instance.
   */
  protected KijiAdmin getKijiAdmin() {
    return mKijiAdmin;
  }

  /**
   * Gets the uri of the kiji instance used for testing.
   *
   * @return The uri of the test kiji instance.
   */
  protected KijiURI getKijiURI() {
    return mURI;
  }

  protected Configuration getConfiguration() {
    return mConf;
  }
}
