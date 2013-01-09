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

package org.kiji.schema.util;

import java.io.Closeable;
import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;

/** Lock interface. */
@ApiAudience.Private
@Inheritance.Sealed
public interface Lock extends Closeable {

  /**
   * Unconditionally acquires the lock.
   *
   * @throws IOException on I/O error.
   */
  void lock() throws IOException;

  /**
   * Acquires the lock.
   *
   * @param timeout
   *          Deadline, in seconds, to acquire the lock. 0 means no timeout.
   * @return whether the lock is acquired (ie. false means timeout).
   * @throws IOException on I/O error.
   */
  boolean lock(double timeout) throws IOException;

  /**
   * Releases the lock.
   *
   * @throws IOException on I/O error.
   */
  void unlock() throws IOException;
}
