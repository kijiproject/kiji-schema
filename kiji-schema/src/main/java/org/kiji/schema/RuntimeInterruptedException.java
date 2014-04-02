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
 * Runtime (unchecked) InterruptedException.
 *
 * <p>
 *   Use this exception when you cannot throw InterruptedException directly
 *   (eg. because checked exceptions do not allow IOException).
 * </p>
 */
@ApiAudience.Public
@ApiStability.Stable
@SuppressWarnings("serial")
public final class RuntimeInterruptedException extends RuntimeException {

  /** Creates an unchecked IOException. */
  public RuntimeInterruptedException() {
  }

  /**
   * Creates an unchecked IOException with the specified message and cause.
   *
   * @param message Message to include in this IOException.
   * @param cause Underlying cause of the IOException.
   */
  public RuntimeInterruptedException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates an unchecked IOException with the specified message.
   *
   * @param message Message to include in the IOException.
   */
  public RuntimeInterruptedException(String message) {
    super(message);
  }

  /**
   * Wraps an IOException into an unchecked exception.
   *
   * @param cause Underlying cause of the IOException.
   */
  public RuntimeInterruptedException(Throwable cause) {
    super(cause);
  }
}
