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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestKijiConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(TestKijiConfiguration.class);

  // Constants used to build the paths to test configuration files.
  private static final String TEST_CONF_FILE = "kiji.conf";
  private static final String CORE_SITE_FILE = "org/kiji/schema/conf/core-site.xml";
  private static final String HBASE_SITE_FILE = "org/kiji/schema/conf/hbase-site.xml";
  private static final String HDFS_SITE_FILE = "org/kiji/schema/conf/hdfs-site.xml";
  private static final String MAPRED_SITE_FILE = "org/kiji/schema/conf/mapred-site.xml";

  // Some known configuration keys and the values they have in the test configuration files.
  private static final String HBASE_QUORUM_KEY = "hbase.zookeeper.quorum";
  private static final String HBASE_QUORUM_VALUE = "localhost";
  private static final String HBASE_CLIENT_PORT_KEY = "hbase.zookeeper.property.clientPort";
  private static final String HBASE_CLIENT_PORT_VALUE = "2181";
  private static final String MAPRED_FS_DEFAULT_KEY = "fs.default.name";
  private static final String MAPRED_FS_DEFAULT_VALUE = "hdfs://localhost:8020";
  private static final String HDFS_NAME_DIR_KEY = "dfs.name.dir";
  private static final String HDFS_NAME_DIR_VALUE = "/var/lib/hadoop-0.20/cache/hadoop/dfs/name";

  // Disable checkstyle for this variable.  It must be public to work with JUnit @Rule.
  // CSOFF: VisibilityModifierCheck
  /** A temporary directory to hold a configuration file. */
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();
  // CSON: VisibilityModifierCheck

  private File mKijiConfFile;

  /**
   * Tests the static methods used to create a new kiji configuration using cluster resources
   * specified in a configuration file.
   */
  @Test
  public void testGetKijiConfiguration() throws IOException {
    String confFilePath = mKijiConfFile.getPath();
    LOG.info("Kiji conf file path: " + confFilePath);
    KijiConfiguration kijiConf = KijiConfiguration.create(confFilePath,
        KijiConfiguration.DEFAULT_INSTANCE_NAME);
    // Test that known values of some configuration variables were loaded properly.
    Configuration conf = kijiConf.getConf();
    assert conf.get(HBASE_QUORUM_KEY).equals(HBASE_QUORUM_VALUE);
    assert conf.get(HBASE_CLIENT_PORT_KEY).equals(HBASE_CLIENT_PORT_VALUE);
    assert conf.get(MAPRED_FS_DEFAULT_KEY).equals(MAPRED_FS_DEFAULT_VALUE);
    assert conf.get(HDFS_NAME_DIR_KEY).equals(HDFS_NAME_DIR_VALUE);
  }

  /**
   * Writes a sample Kiji configuration file into a temporary directory.
   */
  @Before
  public void setup() throws IOException {
    mKijiConfFile = mTempDir.newFile(TEST_CONF_FILE);
    PrintWriter confWriter = null;
    try {
      confWriter = new PrintWriter(mKijiConfFile);
      confWriter.println(getClass().getClassLoader().getResource(CORE_SITE_FILE).getPath());
      confWriter.println(getClass().getClassLoader().getResource(HBASE_SITE_FILE).getPath());
      confWriter.println(getClass().getClassLoader().getResource(HDFS_SITE_FILE).getPath());
      confWriter.println(getClass().getClassLoader().getResource(MAPRED_SITE_FILE).getPath());
    } finally {
      if (null != confWriter) {
        confWriter.close();
      }
    }
  }
}
