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

/** Thrown when parsing a bogus Kiji URI. */
@ApiAudience.Public
@ApiStability.Stable
public final class KijiURIException extends RuntimeException {
  /**
   * Creates a new <code>KijiURIException</code>.
   *
   * @param message Human readable message.
   */
  public KijiURIException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>KijiURIException</code> for the specified uri and detail message.
   *
   * @param uri Bogus URI.
   * @param message Human readable explanation.
   */
  public KijiURIException(String uri, String message) {
    super(String.format("Invalid Kiji URI: '%s' : %s", uri, message));
  }
}
