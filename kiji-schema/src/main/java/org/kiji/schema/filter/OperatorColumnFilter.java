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
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiColumnName;

/**
 * Column filter that combines a list of columns filter using some boolean logical operator.
 *
 * <p> Users should use {@link AndColumnFilter} or {@link OrColumnFilter} instead of this class.
 * </p>.
 */
@ApiAudience.Private
class OperatorColumnFilter extends KijiColumnFilter {
  private static final long serialVersionUID = 1L;

  /** Logical operator to use on the filter operands. */
  private final Operator mOperator;

  /** Column filters to combine. May contain nulls. */
  private final KijiColumnFilter[] mFilters;

  /** Available logical operators. */
  public static enum Operator {
    /** Conjunction. */
    AND(FilterList.Operator.MUST_PASS_ALL),

    /** Disjunction. */
    OR(FilterList.Operator.MUST_PASS_ONE);

    /** HBase FilterList operator this operator translates to. */
    private final FilterList.Operator mHBaseOperator;

    /**
     * Constructs an Operator.
     *
     * @param hbaseOperator HBase FilterList operator this operator maps to.
     */
    Operator(FilterList.Operator hbaseOperator) {
      mHBaseOperator = hbaseOperator;
    }

    /** @return the HBase FilterList operator this operator maps to. */
    public FilterList.Operator getFilterListOp() {
      return mHBaseOperator;
    }
  }

  /**
   * Creates a new <code>OperatorRowFilter</code> instance.
   *
   * @param operator The operator to use for joining the filters into a logical expression.
   * @param filters The filters that should be used in the filter conjunction.
   *     Nulls are filtered out.
   */
  OperatorColumnFilter(Operator operator, KijiColumnFilter[] filters) {
    Preconditions.checkArgument(filters.length > 0, "filters must be non-empty");
    mOperator = operator;
    mFilters = filters;
  }

  /** {@inheritDoc} */
  @Override
  public Filter toHBaseFilter(KijiColumnName kijiColumnName, Context context) throws IOException {
    final List<Filter> hbaseFilters = Lists.newArrayList();
    for (KijiColumnFilter filter : mFilters) {
      if (filter != null) {
        hbaseFilters.add(filter.toHBaseFilter(kijiColumnName, context));
      }
    }
    return new FilterList(mOperator.getFilterListOp(), hbaseFilters);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if ((null == other) || (other.getClass() != getClass())) {
      return false;
    }
    final OperatorColumnFilter that = (OperatorColumnFilter) other;
    return Objects.equal(this.mOperator, that.mOperator)
        && Arrays.equals(this.mFilters, that.mFilters);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(mOperator).append(mFilters).toHashCode();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(getClass())
        .add("operator", mOperator)
        .add("filters", mFilters)
        .toString();
  }
}
