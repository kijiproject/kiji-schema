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

package org.kiji.schema.layout;

import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/**
 * Thrown when an invalid Kiji layout is encountered.  Possible reasons why a layout may be
 * invalid include:
 * <ul>
 *   <li>Invalid data schemas.</li>
 *   <li>Missing family or column names.</li>
 *   <li>The family or column ids were not assigned.</li>
 *   <li>The update layout is inconsistent with respect to a reference layout.  See
 *       {@link org.kiji.schema.layout.KijiTableLayout KijiTableLayout}
 *       for a description of update layouts.</li>
 * </ul>
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
public class InvalidLayoutException extends IOException {
  /**
   * Creates a new {@link InvalidLayoutException} with the specified reason.
   *
   * @param reason A message describing the reason the layout is invalid.
   */
  public InvalidLayoutException(String reason) {
    super(reason);
  }

  /**
   * Constructs an exception indicated a table layout is invalid.
   *
   * @param tableLayout The table layout that is invalid.
   * @param reason A message describing the reason the layout is invalid.
   */
  public InvalidLayoutException(KijiTableLayout tableLayout, String reason) {
    this(String.format("Invalid table layout: %s%n%s", reason,  tableLayout.toString()));
  }
}
