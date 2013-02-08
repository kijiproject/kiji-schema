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

import org.apache.hadoop.hbase.TableNotFoundException;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.IncompatibleKijiVersionException;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiNotInstalledException;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.impl.HBaseSystemTable;

/**
 * Reports on the version numbers associated with this software bundle
 * as well as the installed format versions in used in a Kiji instance.
 */
@ApiAudience.Public
public final class VersionInfo {
  /** No constructor since this is a utility class. */
  private VersionInfo() {}

  /** Fallback software version ID, in case the properties file is not generated/reachable. */
  public static final String DEFAULT_DEVELOPMENT_VERSION = "development";

  private static final String KIJI_SCHEMA_PROPERTIES_RESOURCE =
      "org/kiji/schema/kiji-schema.properties";

  private static final String KIJI_SCHEMA_VERSION_PROP_NAME = "kiji-schema-version";

  /**
   * Deprecated version string. Old 1.0.0-rc releases used 'kiji-1.0' as the instance format
   * version; this is now the same as 'system-1.0'.
   */
  private static final ProtocolVersion DEPRECATED_INSTANCE_VERSION =
      ProtocolVersion.parse("kiji-1.0");

  /** Version string that represents the first version of the instance format. */
  private static final ProtocolVersion MIN_INSTANCE_VERSION =
      ProtocolVersion.parse("system-1.0");

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
      ResourceUtils.closeOrLog(istream);
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
   * Gets the version of the Kiji instance format assumed by the client.
   *
   * <p>The instance format describes the layout of the global metadata state of
   * a Kiji instance. This version number specifies which Kiji instances it would
   * be compatible with. See {@link #isKijiVersionCompatible} to determine whether
   * a deployment is compatible with this version.
   *
   * @return A parsed version of the instance format protocol version string.
   */
  public static ProtocolVersion getClientDataVersion() {
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
    return ProtocolVersion.parse(defaults.get("data-version").toString());
  }

  /**
   * Gets the version of the Kiji instance format installed on the HBase cluster.
   *
   * <p>The instance format describes the layout of the global metadata state of
   * a Kiji instance.</p>
   *
   * @param kiji The kiji instance.
   * @return A parsed version of the storage format protocol version string.
   * @throws IOException on I/O error.
   */
  public static ProtocolVersion getClusterDataVersion(Kiji kiji) throws IOException {
    try {
      final KijiSystemTable systemTable = kiji.getSystemTable();
      final ProtocolVersion dataVersion = systemTable.getDataVersion();
      return dataVersion;
    } catch (TableNotFoundException e) {
      final String instance = kiji.getURI().getInstance();
      throw new KijiNotInstalledException(
          String.format("Kiji instance '%s' is not installed.", instance),
          instance);
    }
  }

  /**
   * Validates that the client instance format version is compatible with the instance
   * format version installed on a Kiji instance.
   * Throws IncompatibleKijiVersionException if not.
   *
   * <p>For the definition of compatibility used in this method, see {@link
   * #isKijiVersionCompatible}</p>
   *
   * @param kiji The kiji instance.
   * @throws IOException on I/O error reading the data version from the cluster,
   *     or throws IncompatibleKijiVersionException if the installed instance format version
   *     is incompatible with the version supported by the client.
   */
  public static void validateVersion(Kiji kiji) throws IOException {
    if (isKijiVersionCompatible(kiji)) {
      return; // valid.
    } else {
      final ProtocolVersion clientVersion = VersionInfo.getClientDataVersion();
      final ProtocolVersion clusterVersion = VersionInfo.getClusterDataVersion(kiji);
      throw new IncompatibleKijiVersionException(String.format(
          "Data format of Kiji instance (%s) cannot operate with client (%s)",
          clusterVersion, clientVersion));
    }
  }

  /**
   * Validates that the client instance format version is compatible with the instance
   * format version installed on a Kiji instance.
   * Returns true if they are compatible, false otherwise.
   * "Compatible" versions have the same major version digit (e.g., <tt>system-1.1</tt>
   * and <tt>system-1.0</tt> are compatible; <tt>system-2.5</tt> and <tt>system-1.0</tt> are not).
   *
   * <p>Older instances (installed with KijiSchema 1.0.0-rc3 and prior) will use an instance
   * format version of <tt>kiji-1.0</tt>. This is treated as an alias for <tt>system-1.0</tt>.
   * No other versions associated with the <tt>"kiji"</tt> protocol are supported.</p>
   *
   * @param kiji the kiji instance.
   * @throws IOException on I/O error reading the Kiji version from the system.
   * @return true if the installed instance format
   *     version is compatible with this client, false otherwise.
   */
  public static boolean isKijiVersionCompatible(Kiji kiji) throws IOException {
    final ProtocolVersion clientVersion = VersionInfo.getClientDataVersion();
    final ProtocolVersion clusterVersion = VersionInfo.getClusterDataVersion(kiji);
    return areInstanceVersionsCompatible(clientVersion, clusterVersion);
  }

  /**
   * Actual comparison logic that validates client/cluster data compatibility according to
   * the rules defined in {@link #isKijiVersionCompatible(Kiji)}.
   *
   * <p>package-level visibility for unit testing.</p>
   *
   * @param clientVersion the client software's instance version.
   * @param clusterVersion the cluster's installed instance version.
   * @return true if the installed instance format
   *     version is compatible with this client, false otherwise.
   */
  static boolean areInstanceVersionsCompatible(
      ProtocolVersion clientVersion, ProtocolVersion clusterVersion) {

    if (clusterVersion.equals(DEPRECATED_INSTANCE_VERSION)) {
      // The "kiji-1.0" version is equivalent to "system-1.0" in compatibility tests.
      clusterVersion = MIN_INSTANCE_VERSION;
    }

    return clientVersion.getProtocolName().equals(clusterVersion.getProtocolName())
        && clientVersion.getMajorVersion() == clusterVersion.getMajorVersion();
  }
}
