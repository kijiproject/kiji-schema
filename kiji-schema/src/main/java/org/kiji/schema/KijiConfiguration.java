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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;

/**
 * <p>The configuration for a single instance of Kiji, as a cluster could
 * contain multiple instances.</p>
 *
 * <p>An installed Kiji instance contains several tables:
 *   <ul>
 *     <li>System table: Kiji system and version information.</li>
 *     <li>Meta table: Metadata about the existing Kiji tables.</li>
 *     <li>Schema table: Avro Schemas of the cells in Kiji tables.</li>
 *     <li>User tables: The user-space Kiji tables that you create.</li>
 *   </ul>
 * </p>
 *
 * <p>The default Kiji instance name is <em>default</em>.</p>
 */
@ApiAudience.Public
@Deprecated
public final class KijiConfiguration extends Configured {
  private static final Logger LOG = LoggerFactory.getLogger(KijiConfiguration.class);

  /** The default Kiji configuration file. */
  public static final String DEFAULT_CONF_FILE = "/etc/kiji/kiji.conf";

  /** The default kiji instance name. */
  public static final String DEFAULT_INSTANCE_NAME = "default";

  /** The Configuration variable to store the kiji instance name. */
  public static final String CONF_KIJI_INSTANCE_NAME = "kiji.instance.name";

  /**
   * Constructs a handle to a Kiji instance.
   *
   * @param conf A Configuration object that specifies the HBase cluster.
   * @param instanceName The name of the Kiji instance.
   */
  public KijiConfiguration(Configuration conf, String instanceName) {
    super(conf);
    conf.set(CONF_KIJI_INSTANCE_NAME, instanceName);
  }

  /**
   * Initializes a Kiji configuration from a Hadoop configuration and a Kiji URI.
   *
   * @param conf Hadoop configuration.
   * @param uri Kiji URI of the instance to initialize.
   */
  public KijiConfiguration(Configuration conf, KijiURI uri) {
    super(conf);
    conf.set(CONF_KIJI_INSTANCE_NAME, Preconditions.checkNotNull(uri.getInstance()));
    conf.set(
        HConstants.ZOOKEEPER_QUORUM,
        Joiner.on(",").join(uri.getZookeeperQuorumOrdered()));
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());
  }

  /**
   * Constructs a deep copy of an existing Kiji configuration.
   *
   * @param kijiConf The Kiji configuration to copy.
   */
  public KijiConfiguration(KijiConfiguration kijiConf) {
    this(new Configuration(kijiConf.getConf()), kijiConf.getName());
  }

  /**
   * Gets a handle to a Kiji instance.
   * This method is useful if you cannot add the HBase configuration resources to the classpath of
   * your application (in an HTTP Servlet, for example).
   *
   * @param confFile The path to a configuration file that specifies the location of HBase cluster
   *    resources.
   * @param name The name of the Kiji instance.
   * @return A handle to a Kiji instance.
   * @throws IOException If there is an error reading the configuration file.
   */
  public static KijiConfiguration create(String confFile, String name)
      throws IOException {
    LOG.info("Kiji conf file: " + confFile);
    Configuration conf = new Configuration();
    // Load resources specified in confFile into the configuration.
    BufferedReader confReader = null;
    try {
      confReader = new BufferedReader(new InputStreamReader(new FileInputStream(
          confFile), "UTF-8"));
      String nextLine = null;
      while ((nextLine = confReader.readLine()) != null) {
        nextLine = nextLine.trim();
        LOG.debug("Adding default resource: " + nextLine);
        conf.addResource(new URL("file://" + nextLine));
      }
      return new KijiConfiguration(conf, name);
    } finally {
      if (null != confReader) {
        confReader.close();
      }
    }
  }

  /**
   * Gets a handle to a Kiji instance using the default Kiji configuration file.
   * This method is useful if you cannot add the HBase configuration resources to the classpath of
   * your application (in an HTTP Servlet, for example).
   *
   * @param name The name of the Kiji instance.
   * @return A handle to a Kiji instance.
   * @throws IOException If there is an error reading the configuration file.
   */
  public static KijiConfiguration create(String name) throws IOException {
    return create(DEFAULT_CONF_FILE, name);
  }

  /**
   * Gets the name of this Kiji instance.
   *
   * @return The name of the Kiji instance.
   */
  public String getName() {
    if (null == getConf().get(CONF_KIJI_INSTANCE_NAME)) {
      LOG.warn(CONF_KIJI_INSTANCE_NAME + " is not set in this Configuration.  "
          + "Returning the default name of \"" + DEFAULT_INSTANCE_NAME + "\".");
    }
    return getConf().get(CONF_KIJI_INSTANCE_NAME, DEFAULT_INSTANCE_NAME);
  }

  /**
   * Determines whether this named Kiji instance exists.
   *
   * @return Whether this Kiji instance exists in the HBase cluster.
   * @throws IOException If there is an error.
   */
  public boolean exists() throws IOException {
    // A Kiji instance is installed in an HBase cluster if all of the
    // meta tables exist.  It's kind of a waste to check for all of
    // them -- that sort of thing could happen in a fsck-like tool for
    // Kiji to be written later.  We will assume the Kiji instance is
    // valid if the Kiji system table for this Kiji exists.
    HBaseAdmin hbaseAdmin = new HBaseAdmin(getConf());
    try {
      return exists(hbaseAdmin);
    } finally {
      hbaseAdmin.close();
    }
  }

  /**
   * Determines whether the Kiji instance exists according to the
   * given HBase admin interface.
   *
   * @param hbaseAdmin The HBase interface to use.
   * @return Whether the Kiji instance exists.
   * @throws IOException If there is an error.
   */
  protected boolean exists(HBaseAdmin hbaseAdmin) throws IOException {
    return hbaseAdmin.tableExists(
        KijiManagedHBaseTableName.getSystemTableName(getName()).toString());
  }
}
