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

import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;

/** Factory for Lock instances. */
@ApiAudience.Private
@Inheritance.Sealed
public interface LockFactory {

  /**
   * Creates a new lock with the specified name.
   *
   * @param name Lock name (eg. a ZooKeeper node path).
   * @return the lock with the specified name.
   * @throws IOException on I/O error.
   */
  Lock create(String name) throws IOException;
}
