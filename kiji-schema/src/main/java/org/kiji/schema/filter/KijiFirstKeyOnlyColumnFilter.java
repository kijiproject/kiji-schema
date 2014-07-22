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

package org.kiji.schema.filter;

import java.io.IOException;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiColumnName;

/**
 * A column filter that limits the number of versions to 1.
 *
 * <p> This filter restricts the number of cells returned for the column it applies to to 1. </p>
 * <p> Wraps the HBase {@link org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter}. </p>
 * @deprecated Instead of using this filter, set the column's max values to 1.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Deprecated
public final class KijiFirstKeyOnlyColumnFilter extends KijiColumnFilter {

  private static final long serialVersionUID = 1L;

  /** {@inheritDoc} */
  @Override
  public Filter toHBaseFilter(KijiColumnName kijiColumnName, Context context) throws IOException {
    return new FirstKeyOnlyFilter();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    // No state, so all FirstKeyOnlyFilters are the same
    return other instanceof KijiFirstKeyOnlyColumnFilter;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    // No state, so all FirstKeyOnlyFilters are the same
    return new HashCodeBuilder().toHashCode();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return new ToStringBuilder(FirstKeyOnlyFilter.class).toString();
  }
}
