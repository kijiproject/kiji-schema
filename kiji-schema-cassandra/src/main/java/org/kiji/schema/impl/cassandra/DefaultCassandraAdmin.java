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

import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.cassandra.CassandraKijiURI;

/**
 * Lightweight wrapper to mimic the functionality of HBaseAdmin.
 *
 * We can pass instances of this class around the C* code instead of passing around just Sessions
 * or something like that.
 *
 * NOTE: We assume that this session does NOT currently have a keyspace selected.
 *
 *
 */
public final class DefaultCassandraAdmin extends CassandraAdmin {
  // TODO: Need to figure out who is in charge of closing out the open session here...

  /**
   * Create a CassandraAdmin object from a URI.
   *
   * @param kijiURI The KijiURI specifying the Kiji instance for the CassandraAdmin.
   *                Note: Must be an instance of CassandraKijiURI.
   * @return A CassandraAdmin for the given Kiji instance.
   */
  public static DefaultCassandraAdmin makeFromKijiURI(KijiURI kijiURI) {
    CassandraKijiURI cassandraKijiURI;
    if (kijiURI instanceof CassandraKijiURI) {
       cassandraKijiURI = (CassandraKijiURI) kijiURI;
    } else {
      throw new KijiIOException("Need a Cassandra URI for a CassandraAdmin.");
    }
    List<String> hosts = cassandraKijiURI.getContactPoints();
    String[] hostStrings = hosts.toArray(new String[hosts.size()]);
    int port = cassandraKijiURI.getContactPort();
    Cluster cluster = Cluster
        .builder()
        .addContactPoints(hostStrings)
        .withPort(port)
        .build();
    Session cassandraSession = cluster.connect();
    return new DefaultCassandraAdmin(cassandraSession, kijiURI);
  }


  /**
   * Create a CassandraAdmin from an open Cassandra Session and a URI.
   *
   * @param session The open C* Session.
   * @param kijiURI The KijiURI specifying the Kiji instance for the CassandraAdmin.
   *                Note: Must be an instance of CassandraKijiURI.
   */
  private DefaultCassandraAdmin(Session session, KijiURI kijiURI) {
    super(session, kijiURI);
  }
}
