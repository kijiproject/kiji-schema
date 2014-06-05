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

import com.google.common.base.Objects;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.hbase.HBaseColumnName;

/**
 * A KijiRowFilter that excludes rows that have no data for some column <code>columnName</code>.
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class HasColumnDataRowFilter extends KijiRowFilter {
  /** The name of the family node. */
  private static final String FAMILY_NODE = "family";

  /** The name of the qualifier node. */
  private static final String QUALIFIER_NODE = "qualifier";

  /** The name of the column family to check for data in. */
  private final String mFamily;

  /** The name of the column qualifier to check for data in. */
  private final String mQualifier;

  /**
   * Constructs a row filter that excludes rows that have no data in <code>columnName</code>.
   *
   * @param family The column family of interest.
   * @param qualifier The column qualifier of interest.
   */
  public HasColumnDataRowFilter(String family, String qualifier) {
    if (null == family || family.isEmpty()) {
      throw new IllegalArgumentException("family is required");
    }
    if (null == qualifier || qualifier.isEmpty()) {
      throw new IllegalArgumentException("qualifier is required");
    }
    mFamily = family;
    mQualifier = qualifier;
  }

  /**
   * Constructs a row filter that excludes rows that have no data in <code>columnName</code>.
   *
   * @param columnName The column family:qualifier of interest.
   */
  public HasColumnDataRowFilter(String columnName) {
    if (null == columnName) {
      throw new IllegalArgumentException("columnName is required");
    }

    KijiColumnName kijiColName = KijiColumnName.create(columnName);
    if (!kijiColName.isFullyQualified()) {
      throw new IllegalArgumentException("Cannot use an unqualified column family.");
    }

    mFamily = kijiColName.getFamily();
    mQualifier = kijiColName.getQualifier();
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add(mFamily, mQualifier);
    return builder.build();
  }

  /** {@inheritDoc} */
  @Override
  public Filter toHBaseFilter(Context context) throws IOException {
    // Create a filter that accepts a cell from mFamily and mQualifier only if it is
    // not an empty byte array.  Since all kiji cells have data (at the very least, the
    // schema hash), this will accept all cells that exist.
    HBaseColumnName hbaseColumnName = context.getHBaseColumnName(
        KijiColumnName.create(mFamily, mQualifier));
    SingleColumnValueFilter filter = new SingleColumnValueFilter(
        hbaseColumnName.getFamily(),
        hbaseColumnName.getQualifier(),
        CompareOp.NOT_EQUAL,
        new byte[0]);

    // If there are no cells in mFamily and mQualifier, skip the entire row.
    filter.setFilterIfMissing(true);

    return filter;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof HasColumnDataRowFilter)) {
      return false;
    } else {
      final HasColumnDataRowFilter otherFilter = (HasColumnDataRowFilter) other;
      return Objects.equal(otherFilter.mFamily, mFamily)
          && Objects.equal(otherFilter.mQualifier, mQualifier);
    }
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mFamily, mQualifier);
  }

  /** {@inheritDoc} */
  @Override
  protected JsonNode toJsonNode() {
    final ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(FAMILY_NODE, mFamily);
    root.put(QUALIFIER_NODE, mQualifier);
    return root;
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends KijiRowFilterDeserializer> getDeserializerClass() {
    return HasColumnDataRowFilterDeserializer.class;
  }

  /** Deserializes {@code HasColumnDataRowFilter}. */
  public static final class HasColumnDataRowFilterDeserializer
      implements KijiRowFilterDeserializer {
    /** {@inheritDoc} */
    @Override
    public KijiRowFilter createFromJson(JsonNode root) {
      final String family = root.path(FAMILY_NODE).getTextValue();
      final String qualifier = root.path(QUALIFIER_NODE).getTextValue();
      return new HasColumnDataRowFilter(family, qualifier);
    }
  }
}
