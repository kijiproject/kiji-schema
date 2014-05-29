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
package org.kiji.schema.layout;

import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Exception which indicates that an operation has failed due to a concurrent KijiTableLayout
 * update.
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class LayoutUpdatedException extends IOException {

  /**
   * Construct a new LayoutUpdatedException with the given message.
   *
   * @param message the message with which to construct this exception.
   */
  public LayoutUpdatedException(String message) {
    super(message);
  }

  /**
   * Construct a new LayoutUpdatedException with the given exception as its cause.
   *
   * @param exn the exception which caused this LayoutUpdatedException.
   */
  public LayoutUpdatedException(Exception exn) {
    super(exn);
  }
}
