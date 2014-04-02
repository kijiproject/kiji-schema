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
 * Thrown when there is something wrong with the internal Kiji
 * implementation.  Clients should never catch one of these.  Please
 * file a bug report if you ever see one at the
 * <a target="_top" href="https://jira.kiji.org/">Kiji Issue tracker</a>.
 */
@ApiAudience.Public
@ApiStability.Stable
public final class InternalKijiError extends Error {
  /**
   * Creates a new <code>InternalKijiError</code> with the specified cause.
   *
   * @param cause A throwable cause.
   */
  public InternalKijiError(Throwable cause) {
    super(cause);
  }

  /**
   * Creates a new <code>InternalKijiError</code> with the specified detail message.
   *
   * @param message The exception message.
   */
  public InternalKijiError(String message) {
    super(message);
  }
}
