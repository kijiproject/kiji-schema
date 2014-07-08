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

package org.kiji.schema.cassandra;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiURIException;
import org.kiji.schema.impl.KijiURIParser;
import org.kiji.schema.impl.cassandra.CassandraKijiFactory;
import org.kiji.schema.impl.cassandra.CassandraKijiInstaller;

/**
 * a {@link KijiURI} that uniquely identifies a Cassandra Kiji instance, table, and column set.
 *
 * <h2>{@code CassandraKijiURI} Scheme</h2>
 *
 * The scheme for {@code CassandraKijiURI}s is {@code kiji-cassandra}. When a parsing a KijiURI with
 * this scheme, the resulting {@code URI} or builder will be a Cassandra specific type.
 *
 * <h2>{@code CassandraKijiURI} Cluster Identifier</h2>
 *
 * Cassandra Kiji needs a valid ZooKeeper ensemble, as well as a set of contact points in the
 * Cassandra cluster. The ZooKeeper ensemble is specified in the same way as the default
 * {@code KijiURI}. The contact points follow the ZooKeeper ensemble separated with a slash.  The
 * contact points take one of the following forms, depending on whether one or more hosts is
 * specified, and whether a port is specified:
 *
 * <li> {@code host}
 * <li> {@code host:port}
 * <li> {@code host1,host2}
 * <li> {@code (host1,host2):port}
 *
 * <H2>{@code CassandraKijiURI} Examples</H2>
 *
 * The following are valid example {@code CassandraKijiURI}s:
 *
 * <li> {@code kiji-cassandra://zkEnsemble/contactPoint}
 * <li> {@code kiji-cassandra://zkEnsemble/contactPoint,contactPoint}
 * <li> {@code kiji-cassandra://zkEnsemble/contactPoint:9042/instance}
 * <li> {@code kiji-cassandra://zkEnsemble/contactPoint:9042/instance/table}
 * <li> {@code kiji-cassandra://zkEnsemble/contactPoint:9042/instance/table/col1,col2}
 * <li> {@code kiji-cassandra://zkEnsemble/(contactPoint,contactPoint):9042}
 * <li> {@code kiji-cassandra://zkEnsemble/(contactPoint,contactPoint):9042/instance/table}
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class CassandraKijiURI extends KijiURI {

  /** URI scheme used to fully qualify a Cassandra Kiji instance. */
  public static final String CASSANDRA_SCHEME = "kiji-cassandra";

  /** Default Cassandra contact host. */
  private static final String DEFAULT_CONTACT_POINT = "127.0.0.1";

  /** Default Cassandra contact port. */
  private static final int DEFAULT_CONTACT_PORT = 9042;

  /**
   * Cassandra contact point host names or IP addresses. preserves user specified ordering.
   * Not null.
   */
  private final ImmutableList<String> mContactPoints;

  /** Normalized (sorted and de-duplicated) contact point host names or IP addresses. Not null. */
  private final ImmutableList<String> mContactPointsNormalized;

  /** Cassandra contact points port number. */
  private final int mContactPort;

  // CSOFF: ParameterNumberCheck
  /**
   * Constructs a new CassandraKijiURI with the given parameters.
   *
   * @param scheme of the URI.
   * @param zookeeperQuorum Zookeeper quorum.
   * @param zookeeperClientPort Zookeeper client port.
   * @param contactPoints The host names of Cassandra contact points.
   * @param contactPort The port of Cassandra contact points.
   * @param instanceName Instance name.
   * @param tableName Table name.
   * @param columnNames Column names.
   * @throws KijiURIException If the parameters are invalid.
   */
  private CassandraKijiURI(
      final String scheme,
      final Iterable<String> zookeeperQuorum,
      final int zookeeperClientPort,
      final ImmutableList<String> contactPoints,
      final int contactPort,
      final String instanceName,
      final String tableName,
      final Iterable<KijiColumnName> columnNames) {
    super(scheme, zookeeperQuorum, zookeeperClientPort, instanceName, tableName, columnNames);
    mContactPoints = contactPoints;
    mContactPointsNormalized = ImmutableSortedSet.copyOf(mContactPoints).asList();
    mContactPort = contactPort;
  }
  // CSON

  /**
   * Returns the normalized (de-duplicated and sorted) Cassandra contact point hosts.
   *
   * @return the normalized Cassandra contact point hosts.
   */
  public ImmutableList<String> getContactPoints() {
    return mContactPointsNormalized;
  }

  /**
   * Returns the original user-specified list of Cassandra contact point hosts.
   *
   * @return the user-specified Cassandra contact point hosts.
   */
  public ImmutableList<String> getContactPointsOrdered() {
    return mContactPoints;
  }

  /**
   * Returns the Cassandra contact point port.
   *
   * @return the Cassandra contact point port.
   */
  public int getContactPort() {
    return mContactPort;
  }

  /**
   * Builder class for constructing CassandraKijiURIs.
   */
  public static final class CassandraKijiURIBuilder extends KijiURIBuilder {

    private ImmutableList<String> mContactPoints;
    private int mContactPort;

    // CSOFF: ParameterNumberCheck
    /**
     * Constructs a new builder for CassandraKijiURIs.
     *
     * @param scheme of the URI.
     * @param zookeeperQuorum The initial zookeeper quorum.
     * @param zookeeperClientPort The initial zookeeper client port.
     * @param contactPoints The host names of Cassandra contact points.
     * @param contactPort The port of Cassandra contact points.
     * @param instanceName The initial instance name.
     * @param tableName The initial table name.
     * @param columnNames The initial column names.
     */
    private CassandraKijiURIBuilder(
        final String scheme,
        final Iterable<String> zookeeperQuorum,
        final int zookeeperClientPort,
        final Iterable<String> contactPoints,
        final int contactPort,
        final String instanceName,
        final String tableName,
        final Iterable<KijiColumnName> columnNames
    ) {
      super(scheme, zookeeperQuorum, zookeeperClientPort, instanceName, tableName, columnNames);
      mContactPoints = ImmutableList.copyOf(contactPoints);
      mContactPort = contactPort;
    }
    // CSOFF

    /**
     * Constructs a new builder for CassandraKijiURIs.
     */
    public CassandraKijiURIBuilder() {
      super();
      mContactPoints = ImmutableList.of(DEFAULT_CONTACT_POINT);
      mContactPort = DEFAULT_CONTACT_PORT;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public CassandraKijiURIBuilder withZookeeperQuorum(String[] zookeeperQuorum) {
      super.withZookeeperQuorum(zookeeperQuorum);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public CassandraKijiURIBuilder withZookeeperQuorum(Iterable<String> zookeeperQuorum) {
      super.withZookeeperQuorum(zookeeperQuorum);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public CassandraKijiURIBuilder withZookeeperClientPort(int zookeeperClientPort) {
      super.withZookeeperClientPort(zookeeperClientPort);
      return this;
    }

    /**
     * Provide a list of Cassandra contact points. These contact points will be used to initially
     * connect to Cassandra.
     *
     * @param contactPoints used create initial connection to Cassandra cluster.
     * @return this.
     */
    public CassandraKijiURIBuilder withContactPoints(final Iterable<String> contactPoints) {
      mContactPoints = ImmutableList.copyOf(contactPoints);
      return this;
    }

    /**
     * Provide a port to connect to Cassandra contact points.
     *
     * @param contactPort to use when connecting to Cassandra contact points.
     * @return this.
     */
    public CassandraKijiURIBuilder withContactPort(final int contactPort) {
      mContactPort = contactPort;
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public CassandraKijiURIBuilder withInstanceName(String instanceName) {
      super.withInstanceName(instanceName);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public CassandraKijiURIBuilder withTableName(String tableName) {
      super.withTableName(tableName);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public CassandraKijiURIBuilder withColumnNames(Collection<String> columnNames) {
      super.withColumnNames(columnNames);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public CassandraKijiURIBuilder addColumnNames(Collection<KijiColumnName> columnNames) {
      super.addColumnNames(columnNames);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public CassandraKijiURIBuilder addColumnName(KijiColumnName columnName) {
      super.addColumnName(columnName);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public CassandraKijiURIBuilder withColumnNames(Iterable<KijiColumnName> columnNames) {
      super.withColumnNames(columnNames);
      return this;
    }

    /**
     * Builds the configured CassandraKijiURI.
     *
     * @return A CassandraKijiURI.
     * @throws KijiURIException If the CassandraKijiURI was configured improperly.
     */
    @Override
    public CassandraKijiURI build() {
      return new CassandraKijiURI(
          mScheme,
          mZookeeperQuorum,
          mZookeeperClientPort,
          mContactPoints,
          mContactPort,
          mInstanceName,
          mTableName,
          mColumnNames);
    }
  }

  /**
   * A {@link KijiURIParser} for {@link CassandraKijiURI}s.
   */
  public static final class CassandraKijiURIParser implements KijiURIParser {
    /** {@inheritDoc} */
    @Override
    public CassandraKijiURIBuilder parse(final URI uri) {
      final AuthorityParser authorityParser = AuthorityParser.getAuthorityParser(uri);
      final List<String> segments = Splitter.on('/').omitEmptyStrings().splitToList(uri.getPath());
      if (segments.size() < 1) {
        throw new KijiURIException(uri.toString(), "Cassandra contact points must be specified.");
      }

      final ImmutableList<String> contactPoints = AuthorityParser.parseHosts(uri, segments.get(0));
      final Integer contactPort = AuthorityParser.parsePort(uri, segments.get(0));

      final PathParser segmentParser = new PathParser(uri, 1);

      return new CassandraKijiURIBuilder(
          uri.getScheme(),
          authorityParser.getZookeeperQuorum(),
          authorityParser.getZookeeperClientPort(),
          contactPoints,
          contactPort == null ? DEFAULT_CONTACT_PORT : contactPort,
          segmentParser.getInstance(),
          segmentParser.getTable(),
          segmentParser.getColumns());
    }

    @Override
    public String getName() {
      return CASSANDRA_SCHEME;
    }
  }

  /** {@inheritDoc} */
  @Override
  protected StringBuilder appendClusterIdentifier(
      final StringBuilder sb,
      final boolean preserveOrdering
  ) {
    super.appendClusterIdentifier(sb, preserveOrdering);
    ImmutableList<String> contactPoints =
        preserveOrdering ? mContactPoints : mContactPointsNormalized;
    if (contactPoints.size() == 1) {
      sb.append(contactPoints.get(0));
    } else {
      sb.append('(');
      Joiner.on(',').appendTo(sb, contactPoints);
      sb.append(')');
    }
    sb
        .append(':')
        .append(mContactPort)
        .append('/');

    return sb;
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
   * Gets a builder configured with default Kiji URI fields.
   *
   * More precisely, the following defaults are initialized:
   * <ul>
   *   <li>The Zookeeper quorum and client port is taken from the Hadoop <tt>Configuration</tt></li>
   *   <li>The Kiji instance name is set to <tt>KConstants.DEFAULT_INSTANCE_NAME</tt>
   *       (<tt>"default"</tt>).</li>
   *   <li>The table name and column names are explicitly left unset.</li>
   * </ul>
   *
   * @return A builder configured with this Kiji URI.
   */
  public static CassandraKijiURIBuilder newBuilder() {
    return new CassandraKijiURIBuilder();
  }

  /**
   * Gets a builder configured with a Kiji URI.
   *
   * @param uri The Kiji URI to configure the builder from.
   * @return A builder configured with uri.
   */
  public static CassandraKijiURIBuilder newBuilder(KijiURI uri) {
    final ImmutableList<String> contactPoints;
    final int contactPort;
    if (uri instanceof CassandraKijiURI) {
      final CassandraKijiURI cassandraKijiURI = (CassandraKijiURI) uri;
      contactPoints = cassandraKijiURI.mContactPoints;
      contactPort = cassandraKijiURI.mContactPort;
    } else {
      contactPoints = ImmutableList.of(DEFAULT_CONTACT_POINT);
      contactPort = DEFAULT_CONTACT_PORT;
    }

    return new CassandraKijiURIBuilder(
        uri.getScheme(),
        uri.getZookeeperQuorumOrdered(),
        uri.getZookeeperClientPort(),
        contactPoints,
        contactPort,
        uri.getInstance(),
        uri.getTable(),
        uri.getColumnsOrdered());
  }

  /**
   * Gets a builder configured with the Kiji URI.
   *
   * <p> The String parameter can be a relative URI (with a specified instance), in which
   *     case it is automatically normalized relative to the default Cassandra URI.
   *
   * @param uri String specification of a Kiji URI.
   * @return A builder configured with uri.
   * @throws KijiURIException If the uri is invalid.
   */
  public static CassandraKijiURIBuilder newBuilder(String uri) {
    if (!(uri.startsWith(CASSANDRA_SCHEME))) {
      uri = String.format("kiji-cassandra://%s/%s:%s/%s",
          KijiURI.ENV_URI_STRING, DEFAULT_CONTACT_POINT, DEFAULT_CONTACT_PORT, uri);
    }
    try {
      return new CassandraKijiURIParser().parse(new URI(uri));
    } catch (URISyntaxException exn) {
      throw new KijiURIException(uri, exn.getMessage());
    }
  }

  /**
   * Overridden to provide specific return type.
   *
   * {@inheritDoc}
   */
  @Override
  public CassandraKijiURI resolve(String path) {
    try {
      // Without the "./", URI will assume a path containing a colon
      // is a new URI, for example "family:column".
      URI uri = new URI(toString()).resolve(String.format("./%s", path));
      return newBuilder(uri.toString()).build();
    } catch (URISyntaxException e) {
      // This should never happen
      throw new InternalKijiError(String.format("KijiURI was incorrectly constructed: %s.", this));
    } catch (IllegalArgumentException e) {
      throw new KijiURIException(this.toString(),
          String.format("Path can not be resolved: %s", path));
    }
  }

  /** {@inheritDoc} */
  @Override
  protected CassandraKijiURIBuilder getBuilder() {
    return newBuilder(this);
  }

  /** {@inheritDoc} */
  @Override
  protected CassandraKijiFactory getKijiFactory() {
    return new CassandraKijiFactory();
  }

  /** {@inheritDoc} */
  @Override
  protected CassandraKijiInstaller getKijiInstaller() {
    return CassandraKijiInstaller.get();
  }
}
