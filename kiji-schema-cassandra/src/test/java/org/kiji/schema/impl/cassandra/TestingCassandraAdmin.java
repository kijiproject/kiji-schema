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

import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiURI;

/**
 * Lightweight wrapper to mimic the functionality of HBaseAdmin.
 *
 * We can pass instances of this class around the C* code instead of passing around just Sessions
 * or something like that.
 *
 * NOTE: We assume that this mSession does NOT currently have a keyspace selected.
 *
 * TODO: Need to figure out who is in charge of closing out the open mSession here...
 *
 */
public final class TestingCassandraAdmin extends CassandraAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(TestingCassandraAdmin.class);

  /**
   * Constructor for creating a C* admin from an open C* mSession.
   * @param session An open Session connected to a cluster with a keyspace selected.
   * @param kijiURI The URI of the Kiji instance for the admin.
   */
  private TestingCassandraAdmin(Session session, KijiURI kijiURI) {
    super(session, kijiURI);
  }

  /**
   * Create new instance of CassandraAdmin from an already-open C* mSession.
   *
   * @param session An already-created Cassandra mSession for testing.
   * @param kijiURI The URI of the Kiji instance for the admin.
   * @return A C* admin instance for the Kiji instance.
   */
  public static TestingCassandraAdmin makeFromKijiURI(Session session, KijiURI kijiURI) {
    return new TestingCassandraAdmin(session, kijiURI);
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    // Don't close the session for unit tests!  We want to reuse the same session across
    // different tests.
  }
}
