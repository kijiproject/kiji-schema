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

import java.util.List;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Combines a list of row filters using a logical AND operator.
 *
 * <p> Column filters are applied in order and lazily. </p>
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class AndRowFilter extends OperatorRowFilter {
  /**
   * Creates a row filter that combines a list of row filters with an AND operator.
   *
   * @param filters Row filters to combine with a logical OR.
   */
  public AndRowFilter(List<? extends KijiRowFilter> filters) {
    super(OperatorRowFilter.Operator.AND, filters.toArray(new KijiRowFilter[filters.size()]));
  }

  /**
   * Creates a row filter that combines a list of row filters with an AND operator.
   *
   * @param filters Row filters to combine with a logical AND.
   */
  public AndRowFilter(KijiRowFilter... filters) {
    super(OperatorRowFilter.Operator.AND, filters);
  }
}
