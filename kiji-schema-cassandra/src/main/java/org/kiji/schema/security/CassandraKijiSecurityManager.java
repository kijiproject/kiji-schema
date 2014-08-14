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

package org.kiji.schema.security;

import java.io.IOException;
import java.util.Set;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiURI;

/**
 * {@link KijiSecurityManager} for Cassandra. Currently security is not implemented for Kiji
 * Cassandra.
 */
@ApiAudience.Private
public final class CassandraKijiSecurityManager implements KijiSecurityManager {

  /**
   * Factory method.
   *
   * @param uri of the Kiji instance.
   * @return a new security manager.
   * @throws IOException if there is a problem talking to Cassandra.
   */
  public static CassandraKijiSecurityManager create(final KijiURI uri) throws IOException {
    return new CassandraKijiSecurityManager();
  }

  /** {@inheritDoc} */
  @Override
  public void lock() throws IOException {
    throw new UnsupportedOperationException("Kiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public void unlock() throws IOException {
    throw new UnsupportedOperationException("Kiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public void grant(final KijiUser user, final KijiPermissions.Action action) throws IOException {
    throw new UnsupportedOperationException("Kiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public void grantAll(final KijiUser user) throws IOException {
    throw new UnsupportedOperationException("Kiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public void revoke(final KijiUser user, final KijiPermissions.Action action) throws IOException {
    throw new UnsupportedOperationException("Kiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public void revokeAll(final KijiUser user) throws IOException {
    throw new UnsupportedOperationException("Kiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public void reapplyInstancePermissions() throws IOException {
    throw new UnsupportedOperationException("Kiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public void applyPermissionsToNewTable(final KijiURI tableURI) throws IOException {
    throw new UnsupportedOperationException("Kiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public void grantInstanceCreator(final KijiUser user) throws IOException {
    throw new UnsupportedOperationException("Kiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public KijiPermissions getPermissions(final KijiUser user) throws IOException {
    throw new UnsupportedOperationException("Kiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public Set<KijiUser> listAllUsers() throws IOException {
    throw new UnsupportedOperationException("Kiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public void checkCurrentGrantAccess() throws IOException {
    throw new UnsupportedOperationException("Kiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException("Kiji Cassandra does not implement security.");
  }
}
