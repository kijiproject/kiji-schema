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

import java.io.IOException;

import org.kiji.annotations.ApiAudience;

/** Runtime (unchecked) IOException. */
@ApiAudience.Public
public final class KijiIOException extends RuntimeException {
  private static final long serialVersionUID = -3267145246303892406L;

  /**
   * Wraps an IOException into an unchecked exception.
   *
   * @param ioe Wraps this IOException.
   */
  public KijiIOException(IOException ioe) {
    super(ioe);
  }
}
