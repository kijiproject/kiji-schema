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

package org.kiji.schema.hbase;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KConstants;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiURIException;
import org.kiji.schema.impl.KijiURIParser;
import org.kiji.schema.impl.hbase.HBaseKijiFactory;
import org.kiji.schema.impl.hbase.HBaseKijiInstaller;

/**
 * a {@link KijiURI} that uniquely identifies an HBase Kiji instance, table, and column-set.
 *
 * <h2>{@code HBaseKijiURI} Scheme</h2>
 *
 * The scheme for {@code HBaseKijiURI}s is either {@code kiji} or {@code kiji-hbase}. When either
 * of these schemes is used while parsing a {@code KijiURI}, the resulting URI or builder will
 * be an HBase specific type.
 *
 * <h2>{@code HBaseKijiURI} Cluster Identifier</h2>
 *
 * HBase Kiji needs only a valid ZooKeeper ensemble in order to identify the host HBase cluster.
 * The cluster identifier is identical to the default KijiURI cluster identifier.
 * <p>
 * The {@value #ENV_URI_STRING} value will resolve at runtime to the ZooKeeper ensemble address
 * in the HBase configuration on the classpath.
 *
 * <H2>{@code HBaseKijiURI} Examples</H2>
 *
 * The following are valid example {@code HBaseKijiURI}s:
 *
 * <li> {@code kiji-hbase://zkHost}
 * <li> {@code kiji-hbase://zkHost/instance}
 * <li> {@code kiji-hbase://zkHost/instance/table}
 * <li> {@code kiji-hbase://zkHost:zkPort/instance/table}
 * <li> {@code kiji-hbase://zkHost1,zkHost2/instance/table}
 * <li> {@code kiji-hbase://(zkHost1,zkHost2):zkPort/instance/table}
 * <li> {@code kiji-hbase://zkHost/instance/table/col}
 * <li> {@code kiji-hbase://zkHost/instance/table/col1,col2}
 * <li> {@code kiji-hbase://.env/instance/table}
 * <li> {@code kiji-hbase://.unset/instance/table}
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class HBaseKijiURI extends KijiURI {

  /** URI scheme used to fully qualify an HBase Kiji instance. */
  public static final String HBASE_SCHEME = "kiji-hbase";

  /**
   * Constructs a new HBaseKijiURI with the given parameters.
   *
   * @param scheme of the URI.
   * @param zookeeperQuorum Zookeeper quorum.
   * @param zookeeperClientPort Zookeeper client port.
   * @param instanceName Instance name.
   * @param tableName Table name.
   * @param columnNames Column names.
   * @throws KijiURIException If the parameters are invalid.
   */
  private HBaseKijiURI(
      final String scheme,
      final Iterable<String> zookeeperQuorum,
      final int zookeeperClientPort,
      final String instanceName,
      final String tableName,
      final Iterable<KijiColumnName> columnNames) {
    super(scheme, zookeeperQuorum, zookeeperClientPort, instanceName, tableName, columnNames);
  }

  /**
   * Builder class for constructing HBaseKijiURIs.
   */
  public static final class HBaseKijiURIBuilder extends KijiURIBuilder {

    /**
     * Constructs a new builder for HBaseKijiURIs.
     *
     * @param scheme of the URI.
     * @param zookeeperQuorum The initial zookeeper quorum.
     * @param zookeeperClientPort The initial zookeeper client port.
     * @param instanceName The initial instance name.
     * @param tableName The initial table name.
     * @param columnNames The initial column names.
     */
    private HBaseKijiURIBuilder(
        final String scheme,
        final Iterable<String> zookeeperQuorum,
        final int zookeeperClientPort,
        final String instanceName,
        final String tableName,
        final Iterable<KijiColumnName> columnNames) {
      super(scheme, zookeeperQuorum, zookeeperClientPort, instanceName, tableName, columnNames);
    }

    /**
     * Constructs a new builder for HBaseKijiURIs.
     */
    public HBaseKijiURIBuilder() {
      super();
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public HBaseKijiURIBuilder withZookeeperQuorum(String[] zookeeperQuorum) {
      super.withZookeeperQuorum(zookeeperQuorum);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public HBaseKijiURIBuilder withZookeeperQuorum(Iterable<String> zookeeperQuorum) {
      super.withZookeeperQuorum(zookeeperQuorum);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public HBaseKijiURIBuilder withZookeeperClientPort(int zookeeperClientPort) {
      super.withZookeeperClientPort(zookeeperClientPort);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public HBaseKijiURIBuilder withInstanceName(String instanceName) {
      super.withInstanceName(instanceName);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public HBaseKijiURIBuilder withTableName(String tableName) {
      super.withTableName(tableName);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public HBaseKijiURIBuilder withColumnNames(Collection<String> columnNames) {
      super.withColumnNames(columnNames);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public HBaseKijiURIBuilder addColumnNames(Collection<KijiColumnName> columnNames) {
      super.addColumnNames(columnNames);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public HBaseKijiURIBuilder addColumnName(KijiColumnName columnName) {
      super.addColumnName(columnName);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public HBaseKijiURIBuilder withColumnNames(Iterable<KijiColumnName> columnNames) {
      super.withColumnNames(columnNames);
      return this;
    }

    /**
     * Builds the configured HBaseKijiURI.
     *
     * @return A HBaseKijiURI.
     * @throws KijiURIException If the HBaseKijiURI was configured improperly.
     */
    @Override
    public HBaseKijiURI build() {
      return new HBaseKijiURI(
          mScheme,
          mZookeeperQuorum,
          mZookeeperClientPort,
          mInstanceName,
          mTableName,
          mColumnNames);
    }
  }

  /**
   * A {@link KijiURIParser} for {@link HBaseKijiURI}s.
   */
  public static final class HBaseKijiURIParser implements KijiURIParser {
    /** {@inheritDoc} */
    @Override
    public HBaseKijiURIBuilder parse(final URI uri) {
      final ZooKeeperAuthorityParser authorityParser =
          ZooKeeperAuthorityParser.getAuthorityParser(uri);
      final PathParser segmentParser = new PathParser(uri);

      return new HBaseKijiURIBuilder(
          uri.getScheme(),
          authorityParser.getZookeeperQuorum(),
          authorityParser.getZookeeperClientPort(),
          segmentParser.getInstance(),
          segmentParser.getTable(),
          segmentParser.getColumns());
    }

    @Override
    public String getName() {
      return HBASE_SCHEME;
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
  public static HBaseKijiURIBuilder newBuilder() {
    return new HBaseKijiURIBuilder();
  }

  /**
   * Gets a builder configured with a Kiji URI.
   *
   * @param uri The Kiji URI to configure the builder from.
   * @return A builder configured with uri.
   */
  public static HBaseKijiURIBuilder newBuilder(HBaseKijiURI uri) {
    return new HBaseKijiURIBuilder(
        uri.getScheme(),
        uri.getZookeeperQuorumOrdered(),
        uri.getZookeeperClientPort(),
        uri.getInstance(),
        uri.getTable(),
        uri.getColumnsOrdered());
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
  public static HBaseKijiURIBuilder newBuilder(final String uri) {
    final String uriWithScheme;
    if (uri.startsWith(KIJI_SCHEME) || uri.startsWith(HBASE_SCHEME)) {
      uriWithScheme = uri;
    } else {
      uriWithScheme = String.format("%s/%s/", KConstants.DEFAULT_HBASE_URI, uri);
    }
    try {
      return new HBaseKijiURIParser().parse(new URI(uriWithScheme));
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
  public HBaseKijiURI resolve(String path) {
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
  protected HBaseKijiURIBuilder getBuilder() {
    return newBuilder(this);
  }

  /** {@inheritDoc} */
  @Override
  protected HBaseKijiFactory getKijiFactory() {
    return new HBaseKijiFactory();
  }

  /** {@inheritDoc} */
  @Override
  protected HBaseKijiInstaller getKijiInstaller() {
    return HBaseKijiInstaller.get();
  }
}
