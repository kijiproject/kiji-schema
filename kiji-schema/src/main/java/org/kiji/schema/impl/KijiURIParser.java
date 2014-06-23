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

package org.kiji.schema.impl;

import java.net.URI;
import java.util.List;
import java.util.regex.Matcher;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import org.kiji.annotations.ApiAudience;
import org.kiji.delegation.Lookups;
import org.kiji.delegation.NamedProvider;
import org.kiji.delegation.NoSuchProviderException;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiURI.KijiURIBuilder;
import org.kiji.schema.KijiURIException;
import org.kiji.schema.hbase.HBaseKijiURI;

/**
 * A Parser for {@link KijiURI}s in text form.  {@code KijiURIParser}s register themselves to
 * handle parsing of certain schemes, and the correct parser to use for a given string is decided
 * by its scheme.
 */
@ApiAudience.Private
public interface KijiURIParser extends NamedProvider {
  /**
   * Parse a URI and return a {@link KijiURIBuilder}.
   *
   * @param uri to parse.
   * @return a KijiURIBuilder parsed from the URI.
   */
  KijiURIBuilder parse(URI uri);

  /**
   * A factory for retrieving a {@link KijiURIParser} for a URI, and parsing it.
   */
  public static class Factory {
    /**
     * Parse a {@link URI} into a {@link KijiURIBuilder} based on the {@link URI}'s scheme.
     *
     * @param uri to parse.
     * @return a {@link KijiURIBuilder} for the uri.
     */
    public static KijiURIBuilder get(URI uri) {
      String scheme = uri.getScheme();
      if (scheme.equals(KijiURI.KIJI_SCHEME)) {
        scheme = HBaseKijiURI.HBASE_SCHEME;
      }

      try {
        return Lookups.getNamed(KijiURIParser.class).lookup(scheme).parse(uri);
      } catch (NoSuchProviderException e) {
        throw new KijiURIException(
            uri.toString(),
            String.format("No parser available for Kiji scheme '%s'.", scheme));
      }
    }
  }


  /**
   * Class for parsing the authority portion of a KijiURI.
   */
  public static final class AuthorityParser {
    /** ZooKeeper quorum configured from the local environment.*/
    public static final ImmutableList<String> ENV_ZOOKEEPER_QUORUM;

    /** ZooKeeper client port configured from the local environment. */
    public static final int ENV_ZOOKEEPER_CLIENT_PORT;

    /**
     * Resolves the local environment ZooKeeper parameters.
     *
     * Local environment refers to the hbase-site.xml configuration file available on the classpath.
     */
    static {
      Configuration conf = HBaseConfiguration.create();
      ENV_ZOOKEEPER_QUORUM = ImmutableList.copyOf(conf.get(HConstants.ZOOKEEPER_QUORUM).split(","));
      ENV_ZOOKEEPER_CLIENT_PORT =
          conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, KijiURI.DEFAULT_ZOOKEEPER_CLIENT_PORT);
    }

    private final ImmutableList<String> mZookeeperQuorum;
    private final int mZookeeperClientPort;

    /**
     * Parse the hosts from a host / port segment. The segment must be in one of the following
     * formats:
     *
     * <li> {@code host}
     * <li> {@code host:port}
     * <li> {@code host1,host2}
     * <li> {@code (host1,host2):port}
     *
     * @param uri being parsed. Used for error messaging.
     * @param segment containing the host(s) and optionally the port.
     * @return list of hosts.
     */
    public static ImmutableList<String> parseHosts(URI uri, String segment) {
      final Matcher zkMatcher = KijiURI.RE_AUTHORITY_GROUP.matcher(segment);
      if (zkMatcher.matches()) {
        return ImmutableList.copyOf(Splitter.on(',').split(zkMatcher.group(1)));
      } else {
        final List<String> splits =
            ImmutableList.copyOf(Splitter.on(':').omitEmptyStrings().split(segment));
        switch (splits.size()) {
          case 1:
            return ImmutableList.copyOf(Splitter.on(',').split(segment));
          case 2:
            if (splits.get(0).contains(",")) {
              throw new KijiURIException(
                  uri.toString(), "Multiple ZooKeeper hosts must be parenthesized.");
            }
            return ImmutableList.of(splits.get(0));
          default:
            throw new KijiURIException(
                uri.toString(), "Invalid ZooKeeper ensemble cluster identifier.");
        }
      }
    }

    /**
     * Parse the port from a host / port segment. The segment must be in one of the following
     * formats:
     *
     * <li> {@code host}
     * <li> {@code host:port}
     * <li> {@code host1,host2}
     * <li> {@code (host1,host2):port}
     *
     * @param uri being parsed. Used for error messaging.
     * @param segment containing the host(s) and optionally the port.
     * @return the port, or {@code null} if no port is specified.
     */
    public static Integer parsePort(URI uri, String segment) {
      final List<String> parts = ImmutableList.copyOf(Splitter.on(':').split(segment));
      if (parts.size() != 2) {
        return null;
      }
      try {
        return Integer.parseInt(parts.get(1));
      } catch (NumberFormatException nfe) {
        throw new KijiURIException(uri.toString(),
            String.format("Can not parse port '%s'.", parts.get(1)));
      }
    }

    /**
     * Constructs an AuthorityParser.
     *
     * @param uri The uri whose authority is to be parsed.
     * @return an {@code AuthorityParser} for the provided {@code URI}.
     * @throws KijiURIException If the authority is invalid.
     */
    public static AuthorityParser getAuthorityParser(URI uri) {
      return new AuthorityParser(uri);
    }

    /**
     * Constructs an AuthorityParser.
     *
     * @param uri The uri whose authority is to be parsed.
     * @throws KijiURIException If the authority is invalid.
     */
    private AuthorityParser(URI uri) {
      String authority = uri.getAuthority();
      if (null == authority) {
        throw new KijiURIException(uri.toString(), "ZooKeeper ensemble missing.");
      }

      if (authority.equals(KijiURI.ENV_URI_STRING)) {
        mZookeeperQuorum = ENV_ZOOKEEPER_QUORUM;
        mZookeeperClientPort = ENV_ZOOKEEPER_CLIENT_PORT;
      } else {
        mZookeeperQuorum = parseHosts(uri, authority);
        final Integer port = parsePort(uri, authority);
        mZookeeperClientPort = port == null ? KijiURI.DEFAULT_ZOOKEEPER_CLIENT_PORT : port;
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
   * Parses {@code URI} path segments into Kiji componenets.
   */
  public static class PathParser {

    private final String mInstance;
    private final String mTable;
    private final List<KijiColumnName> mColumns;

    /**
     * Create a new path segment parser for a KijiURI.
     *
     * @param uri to parse.
     */
    public PathParser(URI uri) {
      this(uri, 0);
    }

    /**
     * Create a new path segment parser for a KijiURI with the provided instanceIndex.  The Kiji
     * components begin at the {@code instanceIndex} offset of the path segments.
     *
     * @param uri to parse.
     * @param instanceIndex of Kiji component.
     */
    public PathParser(URI uri, int instanceIndex) {
      final List<String> segments =
          ImmutableList.copyOf(Splitter.on('/').omitEmptyStrings().split(uri.getPath()));
      final int size = segments.size();

      if (size - instanceIndex > 3) {
        throw new KijiURIException(uri.toString(), String.format("Too many path segments."));
      }
      final int tableIndex = instanceIndex + 1;
      final int columnsIndex = tableIndex + 1;

      if (size > instanceIndex  && !segments.get(instanceIndex).equals(KijiURI.UNSET_URI_STRING)) {
        mInstance = segments.get(instanceIndex);
      } else {
        mInstance = null;
      }

      if (size > tableIndex && !segments.get(tableIndex).equals(KijiURI.UNSET_URI_STRING)) {
        mTable = segments.get(tableIndex);
      } else {
        mTable = null;
      }

      if (size > columnsIndex && !segments.get(columnsIndex).equals(KijiURI.UNSET_URI_STRING)) {
        ImmutableList.Builder<KijiColumnName> columns = ImmutableList.builder();
        for (String column
            : Splitter.on(',').omitEmptyStrings().split(segments.get(columnsIndex))) {
          columns.add(KijiColumnName.create(column));
        }
        mColumns = columns.build();
      } else {
        mColumns = ImmutableList.of();
      }
    }

    /**
     * Get the parsed instance name.
     *
     * @return the instance name.
     */
    public String getInstance() {
      return mInstance;
    }

    /**
     * Get the parsed table name.
     *
     * @return the table name.
     */
    public String getTable() {
      return mTable;
    }

    /**
     * Get the column names.
     *
     * @return the column names.
     */
    public List<KijiColumnName> getColumns() {
      return mColumns;
    }
  }
}
