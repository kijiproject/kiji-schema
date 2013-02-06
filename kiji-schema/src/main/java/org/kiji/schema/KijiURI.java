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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.util.KijiNameValidator;

/**
 * URI that fully qualifies a Kiji instance/table/column.
 * Use "default" for the default Kiji instance.
 *
 * Valid URI forms look like:
 * <li> "kiji://zkHost"
 * <li> "kiji://zkHost/instance"
 * <li> "kiji://zkHost/instance/table"
 * <li> "kiji://zkHost:zkPort/instance/table"
 * <li> "kiji://zkHost1,zkHost2/instance/table"
 * <li> "kiji://(zkHost1,zkHost2):zkPort/instance/table"
 * <li> "kiji://zkHost/instance/table/col"
 * <li> "kiji://zkHost/instance/table/col1,col2"
 * <li> "kiji://.env/instance/table"
 * <li> "kiji://.unset/instance/table"
 */
@ApiAudience.Public
public final class KijiURI {

  /** URI/URL scheme used to fully qualify a Kiji table. */
  public static final String KIJI_SCHEME = "kiji";

  /** String to specify an unset KijiURI field. */
  public static final String UNSET_URI_STRING = ".unset";

  /** String to specify a value through the local environment. */
  public static final String ENV_URI_STRING = ".env";

  /** Default Zookeeper port. */
  public static final int DEFAULT_ZOOKEEPER_CLIENT_PORT = 2181;

  /** Pattern matching "(host1,host2,host3):port". */
  public static final Pattern RE_AUTHORITY_GROUP = Pattern.compile("\\(([^)]+)\\):(\\d+)");

  /**
   * Zookeeper quorum: comma-separated list of Zookeeper host names or IP addresses.
   * Preserves user ordering.
   */
  private final ImmutableList<String> mZookeeperQuorum;

  /** Normalized version of mZookeeperQuorum. */
  private final ImmutableList<String> mZookeeperQuorumNormalized;

  /** Zookeeper client port number. */
  private final int mZookeeperClientPort;

  /** Kiji instance name. Null means unset. */
  private final String mInstanceName;

  /** Kiji table name. Null means unset. */
  private final String mTableName;

  /** Kiji column names. Never null. Empty means unset. Preserves user ordering. */
  private final ImmutableList<KijiColumnName> mColumnNames;

  /** Normalized version of mColumnNames. Never null. */
  private final ImmutableList<KijiColumnName> mColumnNamesNormalized;

  /**
   * Constructs a new KijiURI with the given parameters.
   *
   * @param zookeeperQuorum Zookeeper quorum.
   * @param zookeeperClientPort Zookeeper client port.
   * @param instanceName Instance name.
   * @param tableName Table name.
   * @param columnNames Column names.
   * @throws KijiURIException If the parameters are invalid.
   */
  private KijiURI(
      Iterable<String> zookeeperQuorum,
      int zookeeperClientPort,
      String instanceName,
      String tableName,
      Iterable<KijiColumnName> columnNames) throws KijiURIException {
    mZookeeperQuorum = ImmutableList.copyOf(zookeeperQuorum);
    mZookeeperQuorumNormalized = ImmutableSortedSet.copyOf(mZookeeperQuorum).asList();
    mZookeeperClientPort = zookeeperClientPort;
    mInstanceName =
        ((null == instanceName) || !instanceName.equals(UNSET_URI_STRING)) ? instanceName : null;
    mTableName = ((null == tableName) || !tableName.equals(UNSET_URI_STRING)) ? tableName : null;
    mColumnNames = ImmutableList.copyOf(columnNames);
    mColumnNamesNormalized = ImmutableSortedSet.copyOf(mColumnNames).asList();
    validateNames();
  }

  /**
   * Constructs a URI that fully qualifies a Kiji table.
   *
   * @param uri Kiji URI
   * @throws KijiURIException if the URI is invalid.
   */
  private KijiURI(URI uri) throws KijiURIException {
    if (!uri.getScheme().equals(KIJI_SCHEME)) {
      throw new KijiURIException(uri.toString(), "URI scheme must be '" + KIJI_SCHEME + "'");
    }

    final AuthorityParser parser = new AuthorityParser(uri);
    mZookeeperQuorum = parser.getZookeeperQuorum();
    mZookeeperQuorumNormalized = ImmutableSortedSet.copyOf(mZookeeperQuorum).asList();
    mZookeeperClientPort = parser.getZookeeperClientPort();

    final String[] path = new File(uri.getPath()).toString().split("/");
    if (path.length > 4) {
      throw new KijiURIException(uri.toString(),
          "Invalid path, expecting '/kiji-instance/table-name/(column1, column2, ...)'");
    }
    Preconditions.checkState((path.length == 0) || path[0].isEmpty());

    // Instance name:
    if (path.length >= 2) {
      mInstanceName = (path[1].equals(UNSET_URI_STRING)) ? null: path[1];
    } else {
      mInstanceName = null;
    }

    // Table name:
    if (path.length >= 3) {
      mTableName = (path[2].equals(UNSET_URI_STRING)) ? null : path[2];
    } else {
      mTableName = null;
    }

    // Columns:
    final ImmutableList.Builder<KijiColumnName> builder = ImmutableList.builder();
    if (path.length >= 4) {
      if (!path[3].equals(UNSET_URI_STRING)) {
        String[] split = path[3].split(",");
        for (String name : split) {
          builder.add(new KijiColumnName(name));
        }
      }
    }
    mColumnNames = builder.build();
    mColumnNamesNormalized = ImmutableSortedSet.copyOf(mColumnNames).asList();

    validateNames();
  }

  /**
   * Builder class for constructing KijiURIs.
   */
  public static final class KijiURIBuilder {
    /**
     * Zookeeper quorum: comma-separated list of Zookeeper host names or IP addresses.
     * Preserves user ordering.
     */
    private ImmutableList<String> mZookeeperQuorum;

    /** Zookeeper client port number. */
    private int mZookeeperClientPort;

    /** Kiji instance name. Null means unset. */
    private String mInstanceName;

    /** Kiji table name. Null means unset. */
    private String mTableName;

    /** Kiji column names. Never null. Empty means unset. Preserves user ordering. */
    private ImmutableList<KijiColumnName> mColumnNames;

    /**
     * Constructs a new builder for KijiURIs.
     *
     * @param zookeeperQuorum The initial zookeeper quorum.
     * @param zookeeperClientPort The initial zookeeper client port.
     * @param instanceName The initial instance name.
     * @param tableName The initial table name.
     * @param columnNames The initial column names.
     */
    private KijiURIBuilder(
        Iterable<String> zookeeperQuorum,
        int zookeeperClientPort,
        String instanceName,
        String tableName,
        Iterable<KijiColumnName> columnNames) {
      mZookeeperQuorum = ImmutableList.copyOf(zookeeperQuorum);
      mZookeeperClientPort = zookeeperClientPort;
      mInstanceName =
          ((null == instanceName) || !instanceName.equals(UNSET_URI_STRING)) ? instanceName : null;
      mTableName = ((null == tableName) || !tableName.equals(UNSET_URI_STRING)) ? tableName : null;
      mColumnNames = ImmutableList.copyOf(columnNames);
    }

    /**
     * Constructs a new builder for KijiURIs with default values.
     */
    private KijiURIBuilder() {
      ImmutableList.Builder<String> quorumBuilder = ImmutableList.builder();
      mZookeeperQuorum = quorumBuilder.build();
      mZookeeperClientPort = DEFAULT_ZOOKEEPER_CLIENT_PORT;
      mInstanceName = UNSET_URI_STRING;
      mTableName = UNSET_URI_STRING;
      ImmutableList.Builder<KijiColumnName> columnBuilder = ImmutableList.builder();
      mColumnNames = columnBuilder.build();
    }

    /**
     * Configures the KijiURI with Zookeeper Quorum.
     *
     * @param zookeeperQuorum The zookeeper quorum.
     * @return This builder instance so you may chain configuration method calls.
     */
    public KijiURIBuilder withZookeeperQuorum(String[] zookeeperQuorum) {
      mZookeeperQuorum = ImmutableList.copyOf(zookeeperQuorum);
      return this;
    }

    /**
     * Configures the KijiURI with the Zookeeper client port.
     *
     * @param zookeeperClientPort The port.
     * @return This builder instance so you may chain configuration method calls.
     */
    public KijiURIBuilder withZookeeperClientPort(int zookeeperClientPort) {
      mZookeeperClientPort = zookeeperClientPort;
      return this;
    }

    /**
     * Configures the KijiURI with the Kiji instance name.
     *
     * @param instanceName The Kiji instance name.
     * @return This builder instance so you may chain configuration method calls.
     */
    public KijiURIBuilder withInstanceName(String instanceName) {
      mInstanceName = instanceName;
      return this;
    }

    /**
     * Configures the KijiURI with the Kiji table name.
     *
     * @param tableName The Kiji table name.
     * @return This builder instance so you may chain configuration method calls.
     */
    public KijiURIBuilder withTableName(String tableName) {
      mTableName = tableName;
      return this;
    }

    /**
     * Configures the KijiURI with the Kiji column names.
     *
     * @param columnNames The Kiji column names to configure.
     * @return This builder instance so you may chain configuration method calls.
     */
    public KijiURIBuilder withColumnNames(Collection<String> columnNames) {
      ImmutableList.Builder<KijiColumnName> builder = ImmutableList.builder();
      for (String column : columnNames) {
        builder.add(new KijiColumnName(column));
      }
      mColumnNames = builder.build();
      return this;
    }

    /**
     * Adds the column names to the Kiji URI column names.
     *
     * @param columnNames The Kiji column names to add.
     * @return This builder instance so you may chain configuration method calls.
     */
    public KijiURIBuilder addColumnNames(Collection<KijiColumnName> columnNames) {
      ImmutableList.Builder<KijiColumnName> builder = ImmutableList.builder();
      builder.addAll(mColumnNames).addAll(columnNames);
      mColumnNames = builder.build();
      return this;
    }

    /**
     * Adds the column name to the Kiji URI column names.
     *
     * @param columnName The Kiji column name to add.
     * @return This builder instance so you may chain configuration method calls.
     */
    public KijiURIBuilder addColumnName(KijiColumnName columnName) {
      ImmutableList.Builder<KijiColumnName> builder = ImmutableList.builder();
      builder.addAll(mColumnNames).add(columnName);
      mColumnNames = builder.build();
      return this;
    }

    /**
     * Configures the KijiURI with the Kiji column names.
     *
     * @param columnNames The Kiji column names.
     * @return This builder instance so you may chain configuration method calls.
     */
    public KijiURIBuilder withColumnNames(Iterable<KijiColumnName> columnNames) {
      mColumnNames = ImmutableList.copyOf(columnNames);
      return this;
    }

    /**
     * Builds the configured KijiURI.
     *
     * @return A KijiURI.
     * @throws KijiURIException If the KijiURI was configured improperly.
     */
    public KijiURI build() throws KijiURIException {
      return new KijiURI(
          mZookeeperQuorum,
          mZookeeperClientPort,
          mInstanceName,
          mTableName,
          mColumnNames);
    }
  }

  /**
   * Private class for parsing the authority portion of a KijiURI.
   */
  private static class AuthorityParser {
    private final ImmutableList<String> mZookeeperQuorum;
    private final int mZookeeperClientPort;

    /**
     * Constructs an AuthorityParser.
     *
     * @param uri The uri whose authority is to be parsed.
     * @throws KijiURIException If the authority is invalid.
     */
    public AuthorityParser(URI uri) throws KijiURIException {
      String authority = uri.getAuthority();
      if (null == authority) {
        throw new KijiURIException(uri.toString(), "HBase address missing.");
      }

      if (authority.equals(ENV_URI_STRING)) {
        final Configuration conf = HBaseConfiguration.create();
        mZookeeperQuorum = ImmutableList.copyOf(conf.get(HConstants.ZOOKEEPER_QUORUM).split(","));
        mZookeeperClientPort =
            conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, DEFAULT_ZOOKEEPER_CLIENT_PORT);
        return;
      }

      final Matcher zkMatcher = RE_AUTHORITY_GROUP.matcher(authority);
      if (zkMatcher.matches()) {
        mZookeeperQuorum = ImmutableList.copyOf(zkMatcher.group(1).split(","));
        mZookeeperClientPort = Integer.parseInt(zkMatcher.group(2));
      } else {
        final String[] splits = authority.split(":");
        switch (splits.length) {
          case 1:
            mZookeeperQuorum = ImmutableList.copyOf(authority.split(","));
            mZookeeperClientPort = DEFAULT_ZOOKEEPER_CLIENT_PORT;
            break;
          case 2:
            if (splits[0].contains(",")) {
              throw new KijiURIException(uri.toString(),
                  "Multiple zookeeper hosts must be parenthesized.");
            } else {
              mZookeeperQuorum = ImmutableList.of(splits[0]);
            }
            mZookeeperClientPort = Integer.parseInt(splits[1]);
            break;
          default:
            throw new KijiURIException(uri.toString(),
                "Invalid address, expecting 'zookeeper-quorum[:zookeeper-client-port]'");
        }
      }
    }

    /**
     * Gets the zookeeper quorum.
     *
     * @return The zookeeper quorum.
     */
    public ImmutableList<String> getZookeeperQuorum() {
      return mZookeeperQuorum;
    }

    /**
     * Gets the zookeeper client port.
     *
     * @return The zookeeper client port.
     */
    public int getZookeeperClientPort() {
      return mZookeeperClientPort;
    }
  }

  /**
   * Gets a builder configured with default Kiji URI fields.
   *
   * @return A builder configured with this Kiji URI.
   */
  public static KijiURIBuilder newBuilder() {
    return new KijiURIBuilder();
  }

  /**
   * Gets a builder configured with a Kiji URI.
   *
   * @param uri The Kiji URI to configure the builder from.
   * @return A builder configured with uri.
   */
  public static KijiURIBuilder newBuilder(KijiURI uri) {
    return new KijiURIBuilder(uri.getZookeeperQuorumOrdered(),
        uri.getZookeeperClientPort(),
        uri.getInstance(),
        uri.getTable(),
        uri.getColumnsOrdered());
  }

  /**
   * Gets a builder configured with the Kiji URI.
   *
   * @param uri String specification of a Kiji URI.
   * @return A builder configured with uri.
   * @throws KijiURIException If the uri is invalid.
   */
  public static KijiURIBuilder newBuilder(String uri) throws KijiURIException {
    try {
      return newBuilder(new KijiURI(new URI(uri)));
    } catch (URISyntaxException exn) {
      throw new KijiURIException(uri, exn.getMessage());
    }
  }

  /**
   * Resolve the path relative to this KijiURI. Returns a new instance.
   *
   * @param path The path to resolve.
   * @return The resolved KijiURI.
   * @throws KijiURIException If this KijiURI is malformed.
   */
  public KijiURI resolve(String path) throws KijiURIException {
    try {
      // Without the "./", URI will assume a path containing a colon
      // is a new URI, for example "family:column".
      URI uri = new URI(toString()).resolve(String.format("./%s", path));
      return new KijiURI(uri);
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          String.format("KijiURI was incorrectly constructed (should never happen): %s",
              this.toString()));
    } catch (IllegalArgumentException e) {
      throw new KijiURIException(this.toString(),
          String.format("Path can not be resolved: %s", path));
    }
  }

  /** @return Zookeeper quorum (comma-separated list of host names or IPs), normalized. */
  public ImmutableList<String> getZookeeperQuorum() {
    return mZookeeperQuorumNormalized;
  }

  /** @return Zookeeper quorum (comma-separated list of host names or IPs), ordered. */
  public ImmutableList<String> getZookeeperQuorumOrdered() {
    return mZookeeperQuorum;
  }

  /** @return Zookeeper client port. */
  public int getZookeeperClientPort() {
    return mZookeeperClientPort;
  }

  /** @return Kiji instance name. */
  public String getInstance() {
    return mInstanceName;
  }

  /** @return Kiji table name. */
  public String getTable() {
    return mTableName;
  }

  /** @return Kiji columns (comma-separated list of Kiji column names), normalized. Never null. */
  public ImmutableList<KijiColumnName> getColumns() {
    return mColumnNamesNormalized;
  }

  /** @return Kiji columns (comma-separated list of Kiji column names), ordered. Never null. */
  public Collection<KijiColumnName> getColumnsOrdered() {
    return mColumnNames;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return toString(false);
  }

  /**
   * Returns a string representation of this URI that preserves ordering of lists in fields,
   * such as the Zookeeper quorum and Kiji columns.
   *
   * @return An order-preserving string representation of this URI.
   */
  public String toOrderedString() {
    return toString(true);
  }

  /**
   * Returns a string representation of this URI.
   *
   * @param preserveOrdering Whether to preserve ordering of lsits in fields.
   * @return A string reprresentation of this URI.
   */
  private String toString(boolean preserveOrdering) {
    // Remove trailing unset fields.
    if (!mColumnNames.isEmpty()) {
      return toStringCol(preserveOrdering);
    } else if (mTableName != null) {
      return toStringTable(preserveOrdering);
    } else if (mInstanceName != null) {
      return toStringInstance(preserveOrdering);
    } else {
      return toStringAuthority(preserveOrdering);
    }
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object object) {
    if (object == null) {
      return false;
    }
    return object.getClass() == this.getClass() && object.toString().equals(this.toString());
  }

  /**
   * Validates the names used in the URI.
   *
   * @throws KijiURIException if there is an invalid name in this URI.
   */
  private void validateNames() throws KijiURIException {
    if ((mInstanceName != null) && !KijiNameValidator.isValidKijiName(mInstanceName)) {
      throw new KijiURIException(String.format(
          "Invalid Kiji URI: '%s' is not a valid Kiji instance name.", mInstanceName));
    }
    if ((mTableName != null) && !KijiNameValidator.isValidLayoutName(mTableName)) {
      throw new KijiURIException(String.format(
          "Invalid Kiji URI: '%s' is not a valid Kiji table name.", mTableName));
    }
  }

  /**
   * Formats the full KijiURI up to the authority, preserving order.
   *
   * @param preserveOrdering Whether to preserve ordering.
   * @return Representation of this KijiURI up to the authority.
   */
  private String toStringAuthority(boolean preserveOrdering) {
    String zkQuorum;
    ImmutableList<String> zookeeperQuorum =
        preserveOrdering ? mZookeeperQuorum : mZookeeperQuorumNormalized;
    if (null == zookeeperQuorum) {
      zkQuorum = UNSET_URI_STRING;
    } else {
      if (zookeeperQuorum.size() == 1) {
        zkQuorum = zookeeperQuorum.get(0);
      } else {
        zkQuorum = String.format("(%s)", Joiner.on(",").join(zookeeperQuorum));
      }
    }
    return String.format("%s://%s:%s/",
        KIJI_SCHEME,
        zkQuorum,
        mZookeeperClientPort);
  }

  /**
   * Formats the full KijiURI up to the instance.
   *
   * @param preserveOrdering Whether to preserve ordering.
   * @return Representation of this KijiURI up to the instance.
   */
  private String toStringInstance(boolean preserveOrdering) {
    return String.format("%s%s/",
        toStringAuthority(preserveOrdering),
        (null == mInstanceName) ? UNSET_URI_STRING : mInstanceName);
  }

  /**
   * Formats the full KijiURI up to the table.
   *
   * @param preserveOrdering Whether to preserve ordering.
   * @return Representation of this KijiURI up to the table.
   */
  private String toStringTable(boolean preserveOrdering) {
    return String.format("%s%s/",
        toStringInstance(preserveOrdering),
        (null == mTableName) ? UNSET_URI_STRING : mTableName);
  }

  /**
   * Formats the full KijiURI up to the column.
   *
   * @param preserveOrdering Whether to preserve ordering.
   * @return Representation of this KijiURI up to the table.
   */
  private String toStringCol(boolean preserveOrdering) {
    String columnField;
    ImmutableList<KijiColumnName> columns =
        preserveOrdering ? mColumnNames : mColumnNamesNormalized;
    if (columns.isEmpty()) {
      columnField = UNSET_URI_STRING;
    } else {
      ImmutableList.Builder<String> builder = ImmutableList.builder();
      for (KijiColumnName column : columns) {
        builder.add(column.getName());
      }
      ImmutableList<String> strColumns = builder.build();
      if (strColumns.size() == 1) {
        columnField = strColumns.get(0);
      } else {
        columnField = Joiner.on(",").join(strColumns);
      }
    }
    return String.format("%s%s/", toStringTable(preserveOrdering), columnField);
  }
}
