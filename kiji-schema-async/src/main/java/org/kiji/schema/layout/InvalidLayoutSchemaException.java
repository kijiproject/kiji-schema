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

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Thrown when a table layout or a layout update includes invalid or incompatible reader and writer
 * schemas.
 */
@ApiAudience.Public
@ApiStability.Stable
public final class InvalidLayoutSchemaException extends InvalidLayoutException {

  private final List<String> mReasons;

  /**
   * Creates a new {@link InvalidLayoutSchemaException} with the specified reason.
   *
   * @param reason A message describing the reason the layout is invalid.
   */
  public InvalidLayoutSchemaException(String reason) {
    super(reason);
    mReasons = Lists.newArrayList(reason);
  }

  /**
   * Creates a new {@link InvalidLayoutSchemaException} with the specified reasons.
   *
   * @param reasons A list of reasons the layout is invalid.
   */
  public InvalidLayoutSchemaException(List<String> reasons) {
    super(Joiner.on("\n").join(reasons));
    mReasons = reasons;
  }

  /**
   * Constructs an exception indicated a table layout is invalid.
   *
   * @param tableLayout The table layout that is invalid.
   * @param reason A message describing the reason the layout is invalid.
   */
  public InvalidLayoutSchemaException(KijiTableLayout tableLayout, String reason) {
    this(String.format("Invalid table layout: %s%n%s", reason,  tableLayout.toString()));
  }

  /**
   * Get a list of the reasons a table layout was invalid.
   * @return a list of the reasons a table layout was invalid.
   */
  public List<String> getReasons() {
    return mReasons;
  }
}
