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

/**
 * Factory for Lock instances.
 *
 * @deprecated use the lock factory methods in {@link org.kiji.schema.zookeeper.ZooKeeperUtils}.
 */
@ApiAudience.Private
@Inheritance.Sealed
@Deprecated
public interface LockFactory {

  /**
   * Creates a new lock in the specified directory.  Locks within a directory are exclusive; no two
   * locks may be taken out from the same directory at the same time.
   *
   * @param directory identifying an exclusive lock location (e.g. a ZooKeeper ZNode path).
   * @return a lock which is exclusive in the specified directory.
   * @throws IOException on unrecoverable error.
   */
  Lock create(String directory) throws IOException;
}
