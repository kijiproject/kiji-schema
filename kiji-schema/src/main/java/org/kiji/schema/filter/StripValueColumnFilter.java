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

import com.google.common.base.Objects;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiColumnName;

/**
 * Column filter to strip the cell value.
 *
 * <p> This filter is useful to retrieve the qualifiers and/or version timestamps in a column. </p>
 * <p>
 *   TODO(SCHEMA-334): there is a bug that may cause this filter to apply to all columns in a row
 *       even if this filter is attached to a subset of the columns only in a KijiDataRequest.
 *       For now, if you want to strip the value on specific columns within a data request,
 *       you need to issue separate Get requests for this set of columns.
 * </p>
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class StripValueColumnFilter extends KijiColumnFilter {
  private static final long serialVersionUID = 1L;

  /** All StripValueColumnFilter instances are the same: generate hash-code ahead of time. */
  private static final int HASH_CODE =
      new HashCodeBuilder().append(StripValueColumnFilter.class).toHashCode();

  /** {@inheritDoc} */
  @Override
  public Filter toHBaseFilter(KijiColumnName kijiColumnName, Context context) throws IOException {
    return new KeyOnlyFilter();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    // All StripValueRowFilters are the same.
    return other instanceof StripValueColumnFilter;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return HASH_CODE;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(StripValueColumnFilter.class).toString();
  }
}
