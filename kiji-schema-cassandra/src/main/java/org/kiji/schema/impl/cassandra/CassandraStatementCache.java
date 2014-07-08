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

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

/**
 * Caches prepared statement corresponding to Cassandra query strings.
 *
 */
public class CassandraStatementCache {

  // TODO: this class is a serious memory leak, as well as pretty slow due to the synchronization.
  //       Replace with a Guava cache with proper expiration.

  private final Session mSession;

  // TODO (SCHEMA-747): Use a concurrent map, with entries that expire.
  private final Map<String, PreparedStatement> mStatementCache;

  /**
   * Create a new statement cache.
   *
   * @param session Open session to the Cassandra cluster.
   */
  CassandraStatementCache(Session session) {
    mSession = session;
    mStatementCache = new HashMap<String, PreparedStatement>();
  }

  /**
   * Get a prepared statement for a `String` query.
   *
   * If we have already cached this query, return the cached value.  Otherwise, prepare the
   * statement, insert the prepared statement into the cache, and return it.
   *
   * @param query to turn into a prepared statement.
   * @return a prepared statement for the query.
   */
  synchronized PreparedStatement getPreparedStatement(String query) {
    if (!mStatementCache.containsKey(query)) {
      PreparedStatement statement = mSession.prepare(query);
      mStatementCache.put(query, statement);
    }
    return mStatementCache.get(query);
  }
}
