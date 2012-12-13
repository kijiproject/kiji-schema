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


/**
 * A KijiRowFilter for a disjunction (OR operator) of other filters.
 *
 * <p>If a row <i>R</i> is accepted by filter <i>A</i> or <i>B</i>, <i>R</i>
 * will be accepted by OrRowFilter(<i>A</i>, <i>B</i>).</p>
 */
@ApiAudience.Public
public final class OrRowFilter extends OperatorRowFilter {
  /**
   * Creates a new <code>OrRowFilter</code> instance.
   *
   * @param filters The filters that should be used in the filter conjunction.
   */
  public OrRowFilter(List<? extends KijiRowFilter> filters) {
    super(OperatorRowFilter.Operator.OR, filters);
  }
}
