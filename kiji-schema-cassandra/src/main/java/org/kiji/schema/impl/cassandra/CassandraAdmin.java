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

package org.kiji.schema.impl.cassandra;

import java.io.Closeable;
import java.util.Collection;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiURI;
import org.kiji.schema.cassandra.CassandraTableName;

/**
 * Lightweight wrapper to mimic the functionality of HBaseAdmin (and provide other functionality).
 *
 * This class exists mostly so that we are not passing around instances of
 * com.datastax.driver.core.Session everywhere.
 *
 * TODO: Handle reference counting, closing of Session.
 *
 */
public abstract class CassandraAdmin implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraAdmin.class);

  /** Current C* session for the Kiji instance.. */
  private final Session mSession;

  /** URI for this instance. **/
  private final KijiURI mKijiURI;

  /** Keep a cache of all of the prepared CQL statements. */
  private final CassandraStatementCache mStatementCache;

  /**
   * Getter for open Session.
   *
   * @return The Session.
   */
  private Session getSession() {
    return mSession;
  }

  /**
   * Constructor for use by classes that extend this class.  Creates a CassandraAdmin object for a
   * given Kiji instance.
   *
   * @param session Session for this Kiji instance.
   * @param kijiURI URI for this Kiji instance.
   */
  protected CassandraAdmin(Session session, KijiURI kijiURI) {
    Preconditions.checkNotNull(session);
    this.mSession = session;
    this.mKijiURI = kijiURI;
    createKeyspaceIfMissingForURI(mKijiURI);
    mStatementCache = new CassandraStatementCache(mSession);
  }

  /**
   * Given a URI, create a keyspace for the Kiji instance if none yet exists.
   *
   * @param kijiURI The URI.
   */
  private void createKeyspaceIfMissingForURI(KijiURI kijiURI) {
    String keyspace = CassandraTableName.getQuotedKeyspace(kijiURI);
    LOG.info(String.format("Creating keyspace %s (if missing) for %s.", keyspace, kijiURI));

    // TODO: Check whether keyspace is > 48 characters long and if so provide Kiji error to user.
    String queryText = "CREATE KEYSPACE IF NOT EXISTS " + keyspace
        + " WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}";
    ResultSet resultSet = getSession().execute(queryText);
    LOG.info(resultSet.toString());
    getSession().execute(String.format("USE %s", keyspace));

    // Check that the keyspace actually exists!
    assert(keyspaceExists(keyspace));

  }

  /**
   * Check whether a keyspace exists.
   *
   * @param keyspace The keyspace name (can include quotes - this method strips them).
   * @return Whether the keyspace exists.
   */
  private boolean keyspaceExists(String keyspace) {
    return (null != mSession.getCluster().getMetadata().getKeyspace(keyspace));
  }

  /**
   * Create a table in the given keyspace.
   *
   * This wrapper exists (rather than having various classes create tables themselves) so that we
   * can add lots of extra boilerplate checks in here.
   *
   * @param tableName The name of the table to create.
   * @param createTableStatement A string with the table layout.
   */
  public void createTable(CassandraTableName tableName, String createTableStatement) {
    // TODO: Keep track of all tables associated with this session
    LOG.info("Creating table {} with statement {}.", tableName, createTableStatement);
    getSession().execute(createTableStatement);

    // Check that the table actually exists
    assert(tableExists(tableName));
  }

  // TODO: Add something for disabling this table.

  /**
   * Disable a table.
   *
   * @param tableName of the table to disable.
   */
  public void disableTable(CassandraTableName tableName) { }

  // TODO: Just return true for now since we aren't disabling any Cassandra tables yet.

  /**
   * Check whether a table is enabled.
   *
   * @param tableName of the table to check.
   * @return whether the table is enabled.
   */
  public boolean isTableEnabled(CassandraTableName tableName) {
    return true;
  }

  /**
   * Delete a Cassandra table.
   *
   * @param tableName of the table to delete.
   */
  public void deleteTable(CassandraTableName tableName) {
    // TODO: Check first that the table actually exists?
    String queryString = String.format("DROP TABLE IF EXISTS %s;", tableName);
    LOG.info("Deleting table " + tableName);
    getSession().execute(queryString);
  }

  /**
   * Check whether the keyspace for this Kiji instance is empty.
   *
   * @return whether the keyspace is empty.
   */
  public boolean keyspaceIsEmpty() {
    Preconditions.checkNotNull(getSession());
    Preconditions.checkNotNull(getSession().getCluster());
    Preconditions.checkNotNull(getSession().getCluster().getMetadata());
    String keyspace = CassandraTableName.getQuotedKeyspace(mKijiURI);
    Preconditions.checkNotNull(getSession().getCluster().getMetadata().getKeyspace(keyspace));
    Collection<TableMetadata> tables =
        getSession().getCluster().getMetadata().getKeyspace(keyspace).getTables();
    return (tables.isEmpty());
  }

  /**
   * Delete the keyspace for this Kiji instance.
   */
  public void deleteKeyspace() {
    // TODO: Track whether keyspace exists and assert appropriate keyspace state in all methods.
    String keyspace = CassandraTableName.getQuotedKeyspace(mKijiURI);
    String queryString = "DROP KEYSPACE " + keyspace;
    getSession().execute(queryString);
    assert (!keyspaceExists(keyspace));
  }

  /**
   * Check whether a given Cassandra table exists.
   *
   * Useful for double checking that `CREATE TABLE` statements have succeeded.
   *
   * @param tableName of the Cassandra table to check for.
   * @return Whether the table exists.
   */
  public boolean tableExists(CassandraTableName tableName) {
    Preconditions.checkNotNull(getSession());
    Metadata metadata = getSession().getCluster().getMetadata();

    String keyspace = CassandraTableName.getQuotedKeyspace(mKijiURI);

    if (null == metadata.getKeyspace(keyspace)) {
      assert(!keyspaceExists(CassandraTableName.getQuotedKeyspace(mKijiURI)));
      return false;
    }

    return metadata.getKeyspace(keyspace).getTable(tableName.getQuotedTable()) != null;
  }

  // TODO: Implement close method
  /** {@inheritDoc} */
  @Override
  public void close() {
    // Cannot close this right now without wreaking havoc in the unit tests.
    getSession().getCluster().close();
  }

  // ----------------------------------------------------------------------------------------------
  // Code to wrap around the Cassandra Session to ensure that all queries are cached.

  /**
   * Execute a statement, using a prepared statement if one already exists for the statement, and
   * creating and caching a prepared statement otherwise.
   *
   * @param statement The statement to execute.
   * @return The results of executing the statement.
   */
  public ResultSet execute(Statement statement) {
    return mSession.execute(statement);
  }

  /**
   * Execute a query, using a prepared statement if one already exists for the query, and creating
   * and caching a prepared statement otherwise.
   *
   * @param query The query to execute.
   * @return The results of executing the query.
   */
  public ResultSet execute(String query) {
    PreparedStatement preparedStatement = mStatementCache.getPreparedStatement(query);
    return mSession.execute(preparedStatement.bind());
  }

  /**
   * Asynchronously execute a statement, using a prepared statement if one already exists for the
   * statement, and creating and caching a prepared statement otherwise.
   *
   * @param statement The statement to execute.
   * @return The results of executing the statement.
   */
  public ResultSetFuture executeAsync(Statement statement) {
    return mSession.executeAsync(statement);
  }

  /**
   * Asynchronously execute a query, using a prepared statement if one already exists for the
   * query, and creating and caching a prepared statement otherwise.
   *
   * @param query The query to execute.
   * @return The results of executing the query.
   */
  public ResultSetFuture executeAsync(String query) {
    PreparedStatement preparedStatement = mStatementCache.getPreparedStatement(query);
    return mSession.executeAsync(preparedStatement.bind());
  }

  /**
   * Get the prepared statement for a query.
   *
   * @param query for which to get the prepared statement.
   * @return a prepared statement for the query.
   */
  public PreparedStatement getPreparedStatement(String query) {
    return mStatementCache.getPreparedStatement(query);
  }
}

