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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.delegation.Priority;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.impl.cassandra.CassandraAdminFactory;
import org.kiji.schema.impl.cassandra.DefaultCassandraFactory;
import org.kiji.schema.impl.cassandra.TestingCassandraAdminFactory;

/** Factory for Cassandra instances based on URIs. */
public final class TestingCassandraFactory implements CassandraFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TestingCassandraFactory.class);

  /** Factory to delegate to. */
  private static final CassandraFactory DELEGATE = new DefaultCassandraFactory();

  /**
   * Singleton Cassandra mSession for testing.
   *
   * Lazily instantiated when the first test requests a C* mSession for a .fake Kiji instance.
   *
   * Once started, will remain alive until the JVM shuts down.
   */
  private static Session mCassandraSession = null;

  /**
   * Public constructor. This should not be directly invoked by users; you should
   * use CassandraFactory.get(), which retains a singleton instance.
   *
   * This constructor needs to be public because the Java service loader must be able to
   * instantiate it.
   */
  public TestingCassandraFactory() {
  }

  /** URIs for fake HBase instances are "kiji://.fake.[fake-id]/instance/table". */
  private static final String FAKE_CASSANDRA_ID_PREFIX = ".fake.";

  //------------------------------------------------------------------------------------------------
  // URI stuff

  /** {@inheritDoc} */
  @Override
  public CassandraAdminFactory getCassandraAdminFactory(KijiURI uri) {
    if (isFakeCassandraURI(uri)) {
      LOG.debug("URI is a fake C* URI -> Creating FakeCassandraAdminFactory...");
      // Make sure that the EmbeddedCassandraService is started
      try {
        startEmbeddedCassandraServiceIfNotRunningAndOpenSession();
      } catch (Exception e) {
        throw new KijiIOException("Problem with embedded Cassandra Session! " + e);
      }
      // Get an admin factory that will work with the embedded service
      return createFakeCassandraAdminFactory();
    } else {
      LOG.debug("URI is not a fake Cassandra URI.");
      return DELEGATE.getCassandraAdminFactory(uri);
    }
  }

  /**
   * Check whether this is the URI for a fake Cassandra instance.
   *
   * @param uri The URI in question.
   * @return Whether the URI is for a fake instance or not.
   */
  private static boolean isFakeCassandraURI(KijiURI uri) {
    if (uri.getZookeeperQuorum().size() != 1) {
      return false;
    }
    final String zkHost = uri.getZookeeperQuorum().get(0);
    if (!zkHost.startsWith(FAKE_CASSANDRA_ID_PREFIX)) {
      return false;
    }
    return true;
  }

  //------------------------------------------------------------------------------------------------
  // Stuff for starting up C*

  /**
   * Return a fake C* admin factory for testing.
   * @return A C* admin factory that will produce C* admins that will all use the shared
   *     EmbeddedCassandraService.
   */
  private CassandraAdminFactory createFakeCassandraAdminFactory() {
    Preconditions.checkNotNull(mCassandraSession);
    return TestingCassandraAdminFactory.get(mCassandraSession);
  }

  /**
   * Ensure that the EmbeddedCassandraService for unit tests is running.  If it is not, then start
   * it.
   */
  private void startEmbeddedCassandraServiceIfNotRunningAndOpenSession() throws Exception {
    LOG.debug("Ready to start a C* service if necessary...");
    if (null != mCassandraSession) {
      LOG.debug("C* is already running, no need to start the service.");
      //Preconditions.checkNotNull(mCassandraSession);
      return;
    }

    LOG.debug("Starting EmbeddedCassandra!");
    try {
      LOG.info("Starting EmbeddedCassandraService...");
      // Use a custom YAML file that specifies different ports from normal for RPC and thrift.
      InputStream yamlStream = getClass().getResourceAsStream("/cassandra.yaml");
      LOG.info("Checking that we can load cassandra.yaml as a stream...");
      Preconditions.checkNotNull(yamlStream);
      LOG.info("Looks good to load it as a stream!");

      // Write out the YAML contents to a temp file.
      File temp = File.createTempFile("cassandra", ".yaml");
      LOG.info("Writing cassandra.yaml to " + temp);
      BufferedWriter bw = new BufferedWriter(new FileWriter(temp));
      bw.write(IOUtils.toString(yamlStream));
      bw.close();

      File yamlFile = temp;

      Preconditions.checkArgument(yamlFile.exists());
      System.setProperty("cassandra.config", "file:" + yamlFile.getAbsolutePath());
      System.setProperty("cassandra-foreground", "true");

      // Make sure that all of the directories for the commit log, data, and caches are empty.
      // Thank goodness there are methods to get this information (versus parsing the YAML
      // directly).
      ArrayList<String> directoriesToDelete = new ArrayList<String>(Arrays.asList(
          DatabaseDescriptor.getAllDataFileLocations()
      ));
      directoriesToDelete.add(DatabaseDescriptor.getCommitLogLocation());
      directoriesToDelete.add(DatabaseDescriptor.getSavedCachesLocation());
      for (String dirName : directoriesToDelete) {
        FileUtils.deleteDirectory(new File(dirName));
      }
      EmbeddedCassandraService embeddedCassandraService = new EmbeddedCassandraService();
      embeddedCassandraService.start();

    } catch (IOException ioe) {
      throw new KijiIOException("Cannot start embedded C* service!");
    }

    try {
      // Use different port from normal here to avoid conflicts with any locally-running C* cluster.
      // Port settings are controlled in "cassandra.yaml" in test resources.
      String hostIp = "127.0.0.1";
      int port = 9043;
      Cluster cluster = Cluster.builder().addContactPoints(hostIp).withPort(port).build();
      mCassandraSession = cluster.connect();
    } catch (Exception exc) {
      throw new KijiIOException(
          "Started embedded C* service, but cannot connect to cluster. " + exc);
    }
  }

  /** {@inheritDoc} */
  @Override
  public int getPriority(Map<String, String> runtimeHints) {
    // Higher priority than default factory.
    return Priority.HIGH;
  }
}
