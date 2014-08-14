/**
 * (c) Copyright 2013 WibiData, Inc.
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

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Thrown when an error occurs with the configuration or internal operation of Kiji security.
 * Errors encountered due to a user's lack of permission throw {@link KijiAccessException}.
 */
@ApiAudience.Public
@ApiStability.Stable
public final class KijiSecurityException extends RuntimeException {
  /**
   * Creates a new <code>KijiSecurityException</code> with the specified detail message.
   *
   * @param message The exception message.
   * @param cause The cause of the exception, which may contain additional information.
   */
  public KijiSecurityException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a new <code>KijiSecurityException</code> with the specified detail message.
   *
   * @param message The exception message.
   */
  public KijiSecurityException(String message) {
    super(message);
  }
}
