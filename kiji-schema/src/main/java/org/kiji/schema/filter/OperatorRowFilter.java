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
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiDataRequest;

/**
 * Row filter that combines a list of row filters using some boolean logical operator.
 *
 * <p> Users should use {@link AndRowFilter} or {@link OrRowFilter} instead of this class. </p>.
 */
@ApiAudience.Private
abstract class OperatorRowFilter extends KijiRowFilter {
  /** The name of the node holding the operator. */
  private static final String OPERATOR_NODE = "operator";

  /** The name of the node holding the filters. */
  private static final String FILTERS_NODE = "filters";

  /** The operator to use on the filter operands. */
  private final Operator mOperator;

  /** The filters that should be used in the operands. May contain nulls. */
  private final KijiRowFilter[] mFilters;

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
  OperatorRowFilter(Operator operator, KijiRowFilter... filters) {
    Preconditions.checkArgument(filters.length > 0, "filters must be non-empty");
    mOperator = operator;
    mFilters = filters;
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequest dataRequest = KijiDataRequest.builder().build();
    for (KijiRowFilter filter : mFilters) {
      if (filter != null) {
        dataRequest = dataRequest.merge(filter.getDataRequest());
      }
    }
    return dataRequest;
  }

  /** {@inheritDoc} */
  @Override
  public Filter toHBaseFilter(Context context) throws IOException {
    final List<Filter> hbaseFilters = Lists.newArrayList();
    for (KijiRowFilter filter : mFilters) {
      if (filter != null) {
        hbaseFilters.add(filter.toHBaseFilter(context));
      }
    }
    return new FilterList(mOperator.getFilterListOp(), hbaseFilters);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object object) {
    if ((null == object) || (object.getClass() != getClass())) {
      return false;
    }
    final OperatorRowFilter that = (OperatorRowFilter) object;
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

  /** {@inheritDoc} */
  @Override
  protected JsonNode toJsonNode() {
    final ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(OPERATOR_NODE, mOperator.name());
    final ArrayNode filters = root.arrayNode();
    for (KijiRowFilter filter : mFilters) {
      if (filter != null) {
        filters.add(filter.toJson());
      }
    }
    root.put(FILTERS_NODE, filters);
    return root;
  }

  /**
   * Deserializes the filters that are internal to this filter.
   *
   * @param root The {@code JsonNode} that holds the internal fields for this
   *        filter
   * @return A list of the filters that are internal to this filter
   */
  protected static List<KijiRowFilter> parseFilterList(JsonNode root) {
    final JsonNode filtersNode = root.path(FILTERS_NODE);
    Preconditions.checkArgument(filtersNode.isArray(),
        "Node 'filters' is not an array: %s", filtersNode);
    final List<KijiRowFilter> filters = Lists.newArrayList();
    for (JsonNode filterNode : filtersNode) {
      Preconditions.checkArgument(filterNode.isObject(),
          "filter node is not an object: %s", filterNode);
      filters.add(KijiRowFilter.toFilter(filterNode));
    }
    return filters;
  }
}
