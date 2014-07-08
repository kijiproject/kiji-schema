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

package org.kiji.schema.impl.cassandra;

import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiURI;

/**
 * Factory for CassandraAdmin.
 *
 * Useful for creating subclasses that can create different CassandraAdmin objects for testing
 * versus for normal production runtime.
 *
 */
@ApiAudience.Private
public interface CassandraAdminFactory {
  /**
   * Create an instance of CassandraAdmin for the Cassandra Kiji instance specified by the URI.
   *
   * @param uri The URI for a Cassandra Kiji instance.
   * @return A CassandraAdmin for this instance.
   * @throws java.io.IOException if there is a problem creating the Kiji instance.
   */
  CassandraAdmin create(KijiURI uri) throws IOException;

}
