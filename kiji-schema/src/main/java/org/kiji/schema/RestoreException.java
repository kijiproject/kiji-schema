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

package org.kiji.schema;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Exception thrown during a metadata restore operation due to user error.
 */
@ApiAudience.Public
@ApiStability.Stable
final class RestoreException extends Exception {
  /**
   * Creates a new RestoreException instance, with the specified message. A user-readable
   * message must be specified with this exception type.
   * @param msg the message to include.
   */
  RestoreException(String msg) {
    super(msg);
  }
}
