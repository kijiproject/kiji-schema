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

package org.kiji.schema.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.TableNotFoundException;

import org.kiji.schema.IncompatibleKijiVersionException;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiNotInstalledException;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.impl.HBaseSystemTable;

/**
 * Reads the version info from the jar manifest for this version of Kiji.
 */
public final class VersionInfo {
  /** No constructor since this is a utility class. */
  private VersionInfo() {}

  /** Fallback software version ID, in case the properties file is not generated/reachable. */
  public static final String DEFAULT_DEVELOPMENT_VERSION = "development";

  private static final String KIJI_SCHEMA_PROPERTIES_RESOURCE =
      "org/kiji/schema/kiji-schema.properties";

  private static final String KIJI_SCHEMA_VERSION_PROP_NAME = "kiji-schema-version";

  /**
   * Loads kiji schema properties.
   *
   * @return the Kiji schema properties.
   * @throws IOException on I/O error.
   */
  private static Properties loadKijiSchemaProperties() throws IOException {
    final InputStream istream =
        VersionInfo.class.getClassLoader().getResourceAsStream(KIJI_SCHEMA_PROPERTIES_RESOURCE);
    try {
      final Properties properties = new Properties();
      properties.load(istream);
      return properties;
    } finally {
      IOUtils.closeQuietly(istream);
    }
  }

  /**
   * Gets the version of the Kiji client software.
   *
   * @return The version string.
   * @throws IOException on I/O error.
   */
  public static String getSoftwareVersion() throws IOException {
    final String version = VersionInfo.class.getPackage().getImplementationVersion();
    if (version != null) {
      // Proper release: use the value of 'Implementation-Version' in META-INF/MANIFEST.MF:
      return version;
    }

    // Most likely a development version:
    final Properties kijiProps = loadKijiSchemaProperties();
    return kijiProps.getProperty(KIJI_SCHEMA_VERSION_PROP_NAME, DEFAULT_DEVELOPMENT_VERSION);
  }

  /**
   * Gets the version of the Kiji data format assumed by the client.
   *
   * @return The version string.
   */
  public static String getClientDataVersion() {
    final Properties defaults = new Properties();
    try {
      InputStream defaultsFileStream = VersionInfo.class.getClassLoader()
          .getResourceAsStream(HBaseSystemTable.DEFAULTS_PROPERTIES_FILE);
      if (null == defaultsFileStream) {
        throw new IOException(
            "Unable to load system table defaults: " + HBaseSystemTable.DEFAULTS_PROPERTIES_FILE);
      }
      defaults.load(defaultsFileStream);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    return defaults.get("data-version").toString();
  }

  /**
   * Gets the version of the Kiji data format installed in the instance of the HBase cluster.
   *
   * @param kiji The kiji instance.
   * @return The version string.
   * @throws IOException on I/O error.
   */
  public static String getClusterDataVersion(Kiji kiji) throws IOException {
    try {
      final KijiSystemTable systemTable = kiji.getSystemTable();
      final String dataVersion = systemTable.getDataVersion();
      return dataVersion;
    } catch (TableNotFoundException e) {
      throw new KijiNotInstalledException(
          String.format("Kiji instance '%s' is not installed.", kiji.getName()),
          kiji.getName());
    }
  }

  /**
   * Validates that the client data version matches the data version installed on a Kiji instance.
   *
   * @param kiji The kiji instance.
   * @throws IOException on I/O error, and in particular IncompatibleKijiVersionException
   *     if the versions are incompatible.
   */
  public static void validateVersion(Kiji kiji) throws IOException {
    final String clientVersion = VersionInfo.getClientDataVersion();
    final String clusterVersion = VersionInfo.getClusterDataVersion(kiji);

    final String[] clientSplit = clientVersion.split("\\.");
    final String[] clusterSplit = clusterVersion.split("\\.");

    if (!clusterSplit[0].equals(clientSplit[0])) {
      throw new IncompatibleKijiVersionException(String.format(
          "Data format of Kiji instance (%s) does not match client (%s)",
          clusterVersion, clientVersion));
    }
  }
}
