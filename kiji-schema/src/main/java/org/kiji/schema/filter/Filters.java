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

package org.kiji.schema.filter;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/** Utility class to manipulate row and column filters. */
@ApiAudience.Public
@ApiStability.Experimental
public final class Filters {

  /**
   * Combines row filters using a logical OR.
   *
   * @param filters Ordered list of row filters to combine using a logical OR.
   *     Nulls are filtered out.
   * @return a row filter combining the specified row filters using a logical OR.
   */
  public static KijiRowFilter or(KijiRowFilter... filters) {
    return new OrRowFilter(filters);
  }

  /**
   * Combines row filters using a logical AND.
   *
   * @param filters Ordered list of row filters to combine using a logical AND.
   *     Nulls are filtered out.
   * @return a row filter combining the specified row filters using a logical AND.
   */
  public static KijiRowFilter and(KijiRowFilter... filters) {
    return new AndRowFilter(filters);
  }

  /**
   * Combines column filters using a logical OR.
   *
   * @param filters Ordered list of column filters to combine using a logical OR.
   *     Nulls are filtered out.
   * @return a column filter combining the specified column filters using a logical OR.
   */
  public static KijiColumnFilter or(KijiColumnFilter... filters) {
    return new OrColumnFilter(filters);
  }

  /**
   * Combines column filters using a logical AND.
   *
   * @param filters Ordered list of column filters to combine using a logical AND.
   *     Nulls are filtered out.
   * @return a column filter combining the specified column filters using a logical AND.
   */
  public static KijiColumnFilter and(KijiColumnFilter... filters) {
    return new AndColumnFilter(filters);
  }

  /** Utility class may not be instantiated. */
  private Filters() {
  }
}
