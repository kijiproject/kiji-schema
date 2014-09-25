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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.hbase.HBaseKijiURI.HBaseKijiURIBuilder;
import org.kiji.schema.impl.KijiURIParser;
import org.kiji.schema.impl.KijiURIParser.ZooKeeperAuthorityParser;
import org.kiji.schema.util.KijiNameValidator;
import org.kiji.schema.zookeeper.ZooKeeperFactory;

/**
 * a {@code URI} that uniquely identifies a Kiji instance, table, and column-set.
 * Use {@value org.kiji.schema.KConstants#DEFAULT_HBASE_URI} for the default Kiji instance URI.
 *
 * <p>
 *
 * {@code KijiURI} objects can be constructed directly from parsing a URI string:
 * <pre><code>
 * final KijiURI uri = KijiURI.newBuilder("kiji://.env/default/mytable/col").build();
 * </code></pre>
 *
 * <p>
 *
 * Alternatively, {@code KijiURI} objects can be constructed from components by using a builder:
 * <pre><code>
 * final KijiURI uri = KijiURI.newBuilder()
 *   .withInstanceName("default")
 *   .withTableName("mytable")
 *   .addColumnName(KijiColumnName.create(col))
 *   .build();
 * </code></pre>
 *
 * <H2>Syntax</H2>
 *
 * A KijiURI is composed of multiple components: a {@code scheme}, a {@code cluster-identifier},
 * and optionally, an {@code instance-name}, a {@code table-name}, and {@code column-names}.
 * The text format of a {@code KijiURI} must be of the form:
 * <pre><code>
 * scheme://cluster-identifier[/instance-name[/table-name[/column-names]]]
 * </code></pre>
 * where square brackets indicate optional components.
 *
 * <H3>Scheme</H3>
 *
 * The scheme of all {@code KijiURI}s is a identifier prefixed with the string "{@code kiji}", and
 * followed by any combination of letters, digits, plus ("+"), period ("."), or hyphen ("-").
 * <p>
 * The scheme is specific to the type of cluster identified, and determines how the remaining
 * components will be parsed.
 * <p>
 * The default {@code KijiURI} scheme is "{@value #KIJI_SCHEME}". When this scheme is parsed an
 * {@link org.kiji.schema.hbase.HBaseKijiURI} will be created.
 *
 * <H3>Cluster Identifier</H3>
 *
 * The cluster identifier contains the information necessary for Kiji to identify the host cluster.
 * The exact form of the cluster identifier is specific to the cluster type (which is identified
 * by the scheme).
 *
 * At a minimum, the cluster identifier includes ZooKeeper ensemble information for connecting to
 * the ZooKeeper service hosting Kiji. The ZooKeeper Ensemble address is located in the 'authority'
 * position as defined in RFC3986, and may take one of the following host/port forms, depending on
 * whether one or more hosts is specified, and whether a port is specified:
 *
 * <li> {@code .env}
 * <li> {@code host}
 * <li> {@code host:port}
 * <li> {@code host1,host2}
 * <li> {@code (host1,host2):port}
 *
 * The {@value #ENV_URI_STRING} value will resolve at runtime to a ZooKeeper ensemble address
 * taken from the environment. Specifics of how the address is resolved is scheme-specific.
 * <p>
 * Note that, depending on the scheme, the cluster identifier may contain path segments, and thus
 * is potentially larger than just the URI authority.
 *
 * <H3>Instance Name</H3>
 *
 * The instance name component is optional. Identifies a Kiji instance hosted on the cluster.
 * Only valid Kiji instance names may be used.
 *
 * <H3>Table Name</H3>
 *
 * The table name component is optional, and may only be used if the instance name is defined.
 * The table name identifies a Kiji table in the identified instance. Only a valid Kiji table name
 * may be used.
 *
 * <H3>Column Names</H3>
 *
 * The column names component is optional, and may only be used if the table name is defined.
 * The column names identify a set of Kiji column names in the specified table. The column names
 * are comma separated with no spaces, and may contain only valid Kiji column names.
 *
 * <H2>Examples</H2>
 *
 * The following are valid {@code KijiURI}s:
 *
 * <li> {@code kiji://zkHost}
 * <li> {@code kiji://zkHost/instance}
 * <li> {@code kiji://zkHost/instance/table}
 * <li> {@code kiji://zkHost:zkPort/instance/table}
 * <li> {@code kiji://zkHost1,zkHost2/instance/table}
 * <li> {@code kiji://(zkHost1,zkHost2):zkPort/instance/table}
 * <li> {@code kiji://zkHost/instance/table/col}
 * <li> {@code kiji://zkHost/instance/table/col1,col2}
 * <li> {@code kiji://.env/instance/table}
 *
 * <H2>Usage</H2>
 *
 * The {@link KijiURI} class is not directly instantiable (it is effectively {@code abstract}).
 * The builder will instead return a concrete subclass based on the scheme of the provided URI or
 * URI string. If no URI or URI string is provided to create the builder, then the default
 * {@link org.kiji.schema.hbase.HBaseKijiURI} will be assumed.
 *
 * All {@link KijiURI} implementations are immutable and thread-safe.
 */
@ApiAudience.Public
@ApiStability.Stable
public class KijiURI {

  /**
   * Default {@code KijiURI} scheme. When a {@code KijiURI} with this scheme is parsed, the default
   * {@code KijiURI} type ({@link org.kiji.schema.hbase.HBaseKijiURI}) is used.
   */
  public static final String KIJI_SCHEME = "kiji";

  /** String to specify an unset KijiURI field. */
  public static final String UNSET_URI_STRING = ".unset";

  /** String to specify a value through the local environment. */
  public static final String ENV_URI_STRING = ".env";

  /** Default Zookeeper port. */
  public static final int DEFAULT_ZOOKEEPER_CLIENT_PORT = 2181;


  /** Pattern matching "(host1,host2,host3):port". */
  public static final Pattern RE_AUTHORITY_GROUP = Pattern.compile("\\(([^)]+)\\):(\\d+)");

  /** Pattern matching valid Kiji schemes. */
  private static final Pattern RE_SCHEME = Pattern.compile("(?i)kiji[a-z+.-0-9]*://.*");

  /**
   * The scheme of this KijiURI.
   */
  private final String mScheme;

  /**
   * Ordered list of Zookeeper quorum host names or IP addresses.
   * Preserves user ordering. Never null.
   */
  private final ImmutableList<String> mZookeeperQuorum;

  /** Normalized (sorted) version of mZookeeperQuorum. Never null. */
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
   * @param scheme of the KijiURI.
   * @param zookeeperQuorum Zookeeper quorum.
   * @param zookeeperClientPort Zookeeper client port.
   * @param instanceName Instance name.
   * @param tableName Table name.
   * @param columnNames Column names.
   * @throws KijiURIException If the parameters are invalid.
   */
  protected KijiURI(
      final String scheme,
      final Iterable<String> zookeeperQuorum,
      final int zookeeperClientPort,
      final String instanceName,
      final String tableName,
      final Iterable<KijiColumnName> columnNames
  ) {
    mScheme = scheme;
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
   * Builder class for constructing KijiURIs.
   */
  public static class KijiURIBuilder {

    /**
     * The scheme of the KijiURI being built.
     */
    protected String mScheme;

    /**
     * Zookeeper quorum: comma-separated list of Zookeeper host names or IP addresses.
     * Preserves user ordering.
     */
    protected ImmutableList<String> mZookeeperQuorum;

    /** Zookeeper client port number. */
    protected int mZookeeperClientPort;

    /** Kiji instance name. Null means unset. */
    protected String mInstanceName;

    /** Kiji table name. Null means unset. */
    protected String mTableName;

    /** Kiji column names. Never null. Empty means unset. Preserves user ordering. */
    protected ImmutableList<KijiColumnName> mColumnNames;

    /**
     * Constructs a new builder for KijiURIs.
     *
     * @param scheme of the URI.
     * @param zookeeperQuorum The initial zookeeper quorum.
     * @param zookeeperClientPort The initial zookeeper client port.
     * @param instanceName The initial instance name.
     * @param tableName The initial table name.
     * @param columnNames The initial column names.
     */
    protected KijiURIBuilder(
        final String scheme,
        final Iterable<String> zookeeperQuorum,
        final int zookeeperClientPort,
        final String instanceName,
        final String tableName,
        final Iterable<KijiColumnName> columnNames) {
      mScheme = scheme;
      mZookeeperQuorum = ImmutableList.copyOf(zookeeperQuorum);
      mZookeeperClientPort = zookeeperClientPort;
      mInstanceName =
          ((null == instanceName) || !instanceName.equals(UNSET_URI_STRING)) ? instanceName : null;
      mTableName = ((null == tableName) || !tableName.equals(UNSET_URI_STRING)) ? tableName : null;
      mColumnNames = ImmutableList.copyOf(columnNames);
    }

    /**
     * Constructs a new builder for KijiURIs with default values.
     * See {@link KijiURI#newBuilder()} for specific values.
     */
    protected KijiURIBuilder() {
      mScheme = KIJI_SCHEME;
      mZookeeperQuorum = ZooKeeperAuthorityParser.ENV_ZOOKEEPER_QUORUM;
      mZookeeperClientPort = ZooKeeperAuthorityParser.ENV_ZOOKEEPER_CLIENT_PORT;
      mInstanceName = KConstants.DEFAULT_INSTANCE_NAME;
      mTableName = UNSET_URI_STRING;
      mColumnNames = ImmutableList.of();
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
     * Configures the KijiURI with Zookeeper Quorum.
     *
     * @param zookeeperQuorum The zookeeper quorum.
     * @return This builder instance so you may chain configuration method calls.
     */
    public KijiURIBuilder withZookeeperQuorum(Iterable<String> zookeeperQuorum) {
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
        builder.add(KijiColumnName.create(column));
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
    public KijiURI build() {
      throw new UnsupportedOperationException("Abstract method.");
    }
  }

  /**
   * Gets a builder configured with default Kiji URI fields for an HBase cluster.
   *
   * More precisely, the following defaults are initialized:
   * <ul>
   *   <li>The Zookeeper quorum and client port is taken from the Hadoop <tt>Configuration</tt></li>
   *   <li>The Kiji instance name is set to <tt>KConstants.DEFAULT_INSTANCE_NAME</tt>
   *       (<tt>"default"</tt>).</li>
   *   <li>The table name and column names are explicitly left unset.</li>
   * </ul>
   *
   * @return A builder configured with defaults for an HBase cluster.
   */
  public static KijiURIBuilder newBuilder() {
    return new HBaseKijiURIBuilder();
  }

  /**
   * Gets a builder configured with a Kiji URI.
   *
   * @param uri The Kiji URI to configure the builder from.
   * @return A builder configured with uri.
   */
  public static KijiURIBuilder newBuilder(KijiURI uri) {
    return uri.getBuilder();
  }

  /**
   * Gets a builder configured with the Kiji URI.
   *
   * <p> The String parameter can be a relative URI (with a specified instance), in which
   *     case it is automatically normalized relative to DEFAULT_HBASE_URI.
   *
   * @param uri String specification of a Kiji URI.
   * @return A builder configured with uri.
   * @throws KijiURIException If the uri is invalid.
   */
  public static KijiURIBuilder newBuilder(final String uri) {
    final String uriWithScheme;
    if (RE_SCHEME.matcher(uri).matches()) {
      uriWithScheme = uri;
    } else {
      uriWithScheme = String.format("%s/%s/", KConstants.DEFAULT_HBASE_URI, uri);
    }
    try {
      return KijiURIParser.Factory.get(new URI(uriWithScheme));
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
  public KijiURI resolve(String path) {
    try {
      // Without the "./", URI will assume a path containing a colon
      // is a new URI, for example "family:column".
      URI uri = new URI(toString()).resolve(String.format("./%s", path));
      return KijiURIParser.Factory.get(uri).build();
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          String.format("KijiURI was incorrectly constructed (should never happen): %s",
              this.toString()));
    } catch (IllegalArgumentException e) {
      throw new KijiURIException(this.toString(),
          String.format("Path can not be resolved: %s", path));
    }
  }

  /**
   * Returns the address of each member of the ZooKeeper ensemble associated with this KijiURI
   * in comma-separated host:port  (standard ZooKeeper) format. This method will always return the
   * correct addresses of the ZooKeeper ensemble which hosts the metadata for this KijiURI's
   * instance.
   *
   * @return the addresses of the ZooKeeper ensemble members of the Kiji cluster.
   */
  public String getZooKeeperEnsemble() {
    return ZooKeeperFactory.Provider.get().getZooKeeperEnsemble(this);
  }

  /**
   * Returns the set of Zookeeper quorum hosts (names or IPs).
   *
   * <p> This method is not always guaranteed to return valid ZooKeeper hostnames, instead use
   *    {@link org.kiji.schema.KijiURI#getZooKeeperEnsemble()}. </p>
   *
   * <p> Host names or IP addresses are de-duplicated and sorted. </p>
   *
   * @return the set of Zookeeper quorum hosts (names or IPs).
   *     Never null.
   */
  public ImmutableList<String> getZookeeperQuorum() {
    return mZookeeperQuorumNormalized;
  }

  /**
   * Returns the original user-specified list of Zookeeper quorum hosts.
   *
   * <p> This method is not always guaranteed to return valid ZooKeeper hostnames, instead use
   *    {@link org.kiji.schema.KijiURI#getZooKeeperEnsemble()}. </p>
   *
   * <p> Host names are exactly as specified by the user. </p>
   *
   * @return the original user-specified list of Zookeeper quorum hosts.
   *     Never null.
   */
  public ImmutableList<String> getZookeeperQuorumOrdered() {
    return mZookeeperQuorum;
  }

  /** @return Zookeeper client port. */
  public int getZookeeperClientPort() {
    return mZookeeperClientPort;
  }

  /**
   * Returns the scheme of this KijiURI.
   *
   * @return the scheme of this KijiURI.
   */
  public String getScheme() {
    return mScheme;
  }

  /**
   * Returns the name of the Kiji instance specified by this URI, if any.
   *
   * @return the name of the Kiji instance specified by this URI.
   *     Null means unspecified (ie. this URI does not target a Kiji instance).
   */
  public String getInstance() {
    return mInstanceName;
  }

  /**
   * Returns the name of the Kiji table specified by this URI, if any.
   *
   * @return the name of the Kiji table specified by this URI.
   *     Null means unspecified (ie. this URI does not target a Kiji table).
   */
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
   * @return A string representation of this URI.
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

  /**
   * {@inheritDoc}
   *
   * *note* {@code KijiURI}'s are compared based on the string representation of the {@code URI}.
   * If two {@code KijiURI}s point to the same cluster with a different scheme, then they will
   * compare differently, for example {@code kiji://localhost} and {@code kiji-hbase://localhost}.
   */
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
  private void validateNames() {
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
   * Appends the cluster identifier for this {@code KijiURI} to the passed in {@link StringBuilder}.
   *
   * This is a default implementation which will append the ZooKeeper ensemble to the string
   * builder.  May be overridden by subclasses if more than the ZooKeeper ensemble is necessary for
   * the cluster identifier.
   *
   * @param sb a StringBuilder to append the cluster identifier to.
   * @param preserveOrdering whether to preserve ordering in the cluster identifier components.
   * @return the StringBuilder with the cluster identifier appended.
   */
  protected StringBuilder appendClusterIdentifier(
      final StringBuilder sb,
      final boolean preserveOrdering
  ) {
    ImmutableList<String> zookeeperQuorum =
        preserveOrdering ? mZookeeperQuorum : mZookeeperQuorumNormalized;
    if (zookeeperQuorum == null) {
      sb.append(UNSET_URI_STRING);
    } else if (zookeeperQuorum.size() == 1) {
      sb.append(zookeeperQuorum.get(0));
    } else {
      sb.append('(');
      Joiner.on(',').appendTo(sb, zookeeperQuorum);
      sb.append(')');
    }
    sb
      .append(':')
      .append(mZookeeperClientPort)
      .append('/');

    return sb;
  }

  /**
   * Formats the full KijiURI up to the authority, preserving order.
   *
   * @param preserveOrdering Whether to preserve ordering.
   * @return Representation of this KijiURI up to the authority.
   */
  private String toStringAuthority(boolean preserveOrdering) {
    StringBuilder sb = new StringBuilder();
    sb.append(mScheme)
      .append("://");

    appendClusterIdentifier(sb, preserveOrdering);
    return sb.toString();
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

    try {
      // SCHEMA-6. URI Encode column names using RFC-2396.
      final URI columnsEncoded = new URI(KIJI_SCHEME, columnField, null);
      return String.format("%s%s/",
          toStringTable(preserveOrdering),
          columnsEncoded.getRawSchemeSpecificPart());
    } catch (URISyntaxException e) {
      throw new KijiURIException(e.getMessage());
    }
  }

  /**
   * Creates a builder with fields from this {@code KijiURI}.
   *
   * @return a builder with fields from this {@code KijiURI}.
   */
  protected KijiURIBuilder getBuilder() {
    throw new UnsupportedOperationException("Abstract method.");
  }

  /**
   * Returns a {@link KijiFactory} suitable for installing an instance of this {@code KijiURI}.
   *
   * {@code KijiURI} implementations are required to override this method to return a concrete
   * implementation of {@code KijiFactory} appropriate for the cluster type.
   *
   * @return a {@code KijiFactory} for this {@code KijiURI}.
   */
  protected KijiFactory getKijiFactory() {
    throw new UnsupportedOperationException("Abstract method.");
  }

  /**
   * Returns a {@link KijiFactory} suitable for installing an instance of this {@code KijiURI}.
   *
   * {@code KijiURI} implementations are required to override this method. Furthermore, the
   * overridden implementation is required to return a subclass of {@code KijiInstaller} which
   * has overridden implementations of
   * {@link KijiInstaller#install(KijiURI, org.kiji.schema.hbase.HBaseFactory, java.util.Map,
   * org.apache.hadoop.conf.Configuration)} and
   * {@link KijiInstaller#uninstall(KijiURI, org.kiji.schema.hbase.HBaseFactory,
   * org.apache.hadoop.conf.Configuration)}.
   *
   * @return a {@code KijiFactory} for this {@code KijiURI}.
   */
  protected KijiInstaller getKijiInstaller() {
    throw new UnsupportedOperationException("Abstract method.");
  }
}
