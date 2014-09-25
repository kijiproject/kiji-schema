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

import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
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
   * Class for parsing an authority portion in a KijiURI.
   *
   * An HBase KijiURI has only a single authority, whereas a Cassandra KijiURI has two, one for
   * ZooKeeper and another for Cassandra (the Cassandra KijiURI is not strictly a legal URI).
   *
   * The authority in a Cassandra KijiURI can also have a username and password.
   */
  public static final class AuthorityParser {
    private final ImmutableList<String> mHosts;
    private final Integer mHostPort;
    private final String mUsername;
    private final String mPassword;

    /**
     * Constructs an AuthorityParser.
     *
     * @param authority The authority within the URI.
     * @param uri The uri whose authority is to be parsed (used for generating messages in
     *     exceptions).
     * @return an {@code AuthorityParser} for the provided {@code URI}.
     * @throws KijiURIException If the authority is invalid.
     */
    public static AuthorityParser getAuthorityParser(String authority, URI uri) {
      return new AuthorityParser(authority, uri);
    }

    /**
     * Return a new authority parser, which will parse a substring of the entire URI.
     *
     * We specify the authority explicitly because in a Cassandra KijiURI, the Cassandra hosts
     * authority will be different from `uri.getAuthority()`.
     *
     * Parse the hosts from a host / port segment. The segment must be in one of the following
     * formats:
     *
     * <li> {@code host}
     * <li> {@code host:port}
     * <li> {@code host1,host2}
     * <li> {@code (host1,host2):port}
     * <li> {@code user@host}
     * <li> {@code user@host:port}
     * <li> {@code user@host1,host2}
     * <li> {@code user@(host1,host2):port}
     * <li> {@code user:password@host}
     * <li> {@code user:password@host:port}
     * <li> {@code user:password@host1,host2}
     * <li> {@code user:password@(host1,host2):port}
     *
     * @param authority The substring of the URI containing the authority.
     * @param uri The URI itself (needed here for error messages).
     */
    private AuthorityParser(String authority, URI uri) {
      // Extract the user info, if it is present.
      final String hostPortInfo = getHostAndPortInfoFromAuthority(uri, authority);
      final String userInfo = getUserInfoFromAuthority(uri, authority);

      mUsername = parseUsername(uri, userInfo);
      mPassword = parsePassword(uri, userInfo);

      // Get the hosts and ports.
      mHosts = parseHosts(uri, hostPortInfo);
      mHostPort = parsePort(uri, hostPortInfo);
    }

    /**
     * Extract the user information (username and password) from an authority.
     *
     * The returned value is of the form `username:password` or just `username`.
     *
     * @param uri The URI containing the authority (used to generate exception messages).
     * @param authority The authority to parse.
     * @return A string containing the user information, or null if the authority does not contain
     *     user information.
     */
    private static String getUserInfoFromAuthority(URI uri, String authority) {
      if (!authority.contains("@")) {
        return null;
      }
      // Should have no more than a single @ appear!
      final List<String> userInfoAndHostPortInfo =
          ImmutableList.copyOf(Splitter.on('@').split(authority));
      if (2 != userInfoAndHostPortInfo.size()) {
        throw new KijiURIException(
            uri.toString(), "Cannot have more than one '@' in URI authority");
      }
      return userInfoAndHostPortInfo.get(0);
    }

    /**
     * Extract the host(s) and port from an authority.
     *
     * The returned value is of the form `host`, `host:port`, `(host1,host2,...)`, or
     * `(host1,host2,...):port`.
     *
     * @param uri The URI containing the authority (used to generate exception messages).
     * @param authority The authority to parse.
     * @return A string containing the host(s) and optional port.
     */
    private static String getHostAndPortInfoFromAuthority(URI uri, String authority) {
      if (!authority.contains("@")) {
        return authority;
      }
      // Should have no more than a single @ appear!
      final List<String> userInfoAndHostPortInfo =
          ImmutableList.copyOf(Splitter.on('@').split(authority));
      if (2 != userInfoAndHostPortInfo.size()) {
        throw new KijiURIException(
            uri.toString(), "Cannot have more than one '@' in URI authority");
      }
      Preconditions.checkArgument(2 == userInfoAndHostPortInfo.size());
      return userInfoAndHostPortInfo.get(1);
    }

    /**
     * Extract the username from a user information string.
     *
     * @param uri The URI from which the user information comes (used for exception messages).
     * @param userInfo The user info, of the form `username` or `username:password` (can be null).
     * @return The username, or null if the `userInfo` is null.
     */
    private static String parseUsername(URI uri, String userInfo) {
      if (null == userInfo) {
        return null;
      }
      if (!userInfo.contains(":")) {
        return userInfo;
      }
      if (1 != CharMatcher.is(':').countIn(userInfo)) {
        throw new KijiURIException(
            uri.toString(), "Cannot have more than one ':' in URI user info");
      }
      final List<String> usernameAndPassword =
          ImmutableList.copyOf(Splitter.on(':').split(userInfo));
      Preconditions.checkArgument(2 == usernameAndPassword.size());
      return usernameAndPassword.get(0);
    }

    /**
     * Extract the password from a user information string.
     *
     * @param uri The URI from which the user information comes (used for exception messages).
     * @param userInfo The user info, of the form `username` or `username:password` (can be null).
     * @return The password, or null if it is not specified.
     */
    private static String parsePassword(URI uri, String userInfo) {
      if (null == userInfo) {
        return null;
      }
      if (!userInfo.contains(":")) {
        return null;
      }
      if (1 != CharMatcher.is(':').countIn(userInfo)) {
        throw new KijiURIException(
            uri.toString(), "Cannot have more than one ':' in URI user info");
      }
      final List<String> usernameAndPassword =
          ImmutableList.copyOf(Splitter.on(':').split(userInfo));
      Preconditions.checkArgument(2 == usernameAndPassword.size());
      return usernameAndPassword.get(1);
    }

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
    private static ImmutableList<String> parseHosts(URI uri, String segment) {
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
    private static Integer parsePort(URI uri, String segment) {
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
     * @return the hosts from the parsed URI authority.
     */
    public ImmutableList<String> getHosts() {
      return mHosts;
    }

    /**
     * @return the host port from the parsed URI authority (can be null).
     */
    public Integer getHostPort() {
      return mHostPort;
    }

    /**
     * @return the username from the parsed URI authority (can be null).
     */
    public String getUsername() {
      return mUsername;
    }

    /**
     * @return the password from the parsed URI authority (can be null).
     */
    public String getPassword() {
      return mPassword;
    }
  }

  /**
   * Parser for the ZooKeeper authority section in a URI.
   */
  public static final class ZooKeeperAuthorityParser {
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

    /** List of nodes in the ZooKeeper quorum for this authority. */
    private final ImmutableList<String> mZookeeperQuorum;

    /** The ZooKeeper port for this authority. */
    private final int mZookeeperClientPort;

    /**
     * Constructs an AuthorityParser.
     *
     * @param uri The uri whose authority is to be parsed.
     * @return an {@code AuthorityParser} for the provided {@code URI}.
     * @throws KijiURIException If the authority is invalid.
     */
    public static ZooKeeperAuthorityParser getAuthorityParser(URI uri) {
      return new ZooKeeperAuthorityParser(uri);
    }

    /**
     * Constructs an AuthorityParser for ZooKeeper authorities.
     *
     * @param uri The uri whose authority is to be parsed.
     * @throws KijiURIException If the authority is invalid.
     */
    private ZooKeeperAuthorityParser(URI uri) {
      final String authority = uri.getAuthority();
      if (null == authority) {
        throw new KijiURIException(uri.toString(), "ZooKeeper ensemble missing.");
      }

      if (authority.equals(KijiURI.ENV_URI_STRING)) {
        mZookeeperQuorum = ENV_ZOOKEEPER_QUORUM;
        mZookeeperClientPort = ENV_ZOOKEEPER_CLIENT_PORT;
      } else {
        final AuthorityParser authorityParser = AuthorityParser.getAuthorityParser(authority, uri);
        mZookeeperQuorum = authorityParser.getHosts();
        final Integer port = authorityParser.getHostPort();
        mZookeeperClientPort = port == null ? KijiURI.DEFAULT_ZOOKEEPER_CLIENT_PORT : port;
        Preconditions.checkArgument(null == authorityParser.getUsername());
        Preconditions.checkArgument(null == authorityParser.getPassword());
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
