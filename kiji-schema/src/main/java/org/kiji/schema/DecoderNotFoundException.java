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
package org.kiji.schema;

import java.io.IOException;

/**
 * Exception thrown if a decoder cannot be found while
 * {@link KijiReaderFactory.KijiTableReaderOptions.OnDecoderCacheMiss} is set to FAIL.
 */
public final class DecoderNotFoundException extends IOException {

  /**
   * Create a new DecoderNotFoundException with the given message.
   *
   * @param message message for this exception.
   */
  public DecoderNotFoundException(String message) {
    super(message);
  }

  /**
   * Create a new DecoderNotFoundException with the given message and cause.
   *
   * @param message message for this exception.
   * @param cause cause of this exception.
   */
  public DecoderNotFoundException(
      final String message,
      final Throwable cause
  ) {
    super(message, cause);
  }

  /**
   * Create a new DecoderNotFoundException from the given throwable.
   *
   * @param cause cause of this exception.
   */
  public DecoderNotFoundException(
      final Throwable cause
  ) {
    super(cause);
  }
}
