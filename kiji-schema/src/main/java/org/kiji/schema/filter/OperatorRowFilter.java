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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiDataRequest;

/**
 * A KijiRowFilter for a logical operator of composed of other filters.
 *
 * Clients should access this functionality through one of its subclasses, {@link AndRowFilter}
 * or {@link OrRowFilter}.
 */
@ApiAudience.Private
class OperatorRowFilter extends KijiRowFilter {
  /** The operator to use on the filter operands. */
  private final Operator mOperator;

  /** The filters that should be used in the operands. */
  private final List<? extends KijiRowFilter> mFilters;

  /**
   * Available logical operators.
   */
  public static enum Operator {
    /** Conjunction. */
    AND,

    /** Disjunction. */
    OR,
  }

  /**
   * Creates a new <code>OperatorRowFilter</code> instance.
   *
   * @param operator The operator to use for joining the filters into a logical expression.
   * @param filters The filters that should be used in the filter conjunction.
   */
  OperatorRowFilter(Operator operator, List<? extends KijiRowFilter> filters) {
    if (null == filters || filters.isEmpty()) {
      throw new IllegalArgumentException("filters must be non-empty");
    }

    mOperator = operator;
    mFilters = new ArrayList<KijiRowFilter>(filters);
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequest dataRequest = KijiDataRequest.builder().build();
    for (KijiRowFilter filter : mFilters) {
      dataRequest = dataRequest.merge(filter.getDataRequest());
    }
    return dataRequest;
  }

  /** {@inheritDoc} */
  @Override
  public Filter toHBaseFilter(Context context) throws IOException {
    List<Filter> hbaseFilters = new ArrayList<Filter>();
    for (KijiRowFilter filter : mFilters) {
      hbaseFilters.add(filter.toHBaseFilter(context));
    }
    return new FilterList(toHBaseFilterOperator(mOperator), hbaseFilters);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (null == other || other.getClass() != getClass()) {
      return false;
    } else {
      final OperatorRowFilter otherFilter = (OperatorRowFilter) other;
      return Objects.equal(otherFilter.mOperator, mOperator)
          && Objects.equal(otherFilter.mFilters, mFilters);
    }
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mOperator, mFilters);
  }

  /**
   * Converts an Operator to the corresponding HBase FilterList operator.
   *
   * @param operator The operator to convert.
   * @return The HBase FilterList.Operator.
   */
  private FilterList.Operator toHBaseFilterOperator(Operator operator) {
    switch (operator) {
    case AND:
      return FilterList.Operator.MUST_PASS_ALL;
    case OR:
      return FilterList.Operator.MUST_PASS_ONE;
    default:
      throw new RuntimeException("Unsupported operator: " + operator);
    }
  }
}
