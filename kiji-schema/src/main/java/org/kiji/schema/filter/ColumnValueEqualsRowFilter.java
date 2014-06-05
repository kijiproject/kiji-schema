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
import org.apache.avro.Schema;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.util.FromJson;
import org.kiji.schema.util.ToJson;

/**
 * A KijiRowFilter that only includes rows where a specific column's most recent value
 * equals a specified value.
 *
 * <p>This filter will only pass if the data in the Kiji cell matches the specified value
 * <i>exactly</i>.  Both the data and the Avro schema must be the same.</p>
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class ColumnValueEqualsRowFilter extends KijiRowFilter {
  /** The name of the family node. */
  private static final String FAMILY_NODE = "family";

  /** The name of the qualifier node. */
  private static final String QUALIFIER_NODE = "qualifier";

  /** The name of the value node. */
  private static final String VALUE_NODE = "value";

  /** The name of the writer schema node. */
  private static final String SCHEMA_NODE = "writerSchema";

  /** The name of the data node. */
  private static final String DATA_NODE = "data";

  /** The name of the column family to check for data in. */
  private final String mFamily;

  /** The name of the column qualifier to check for data in. */
  private final String mQualifier;

  /** The value the most recent column value must equal to pass the filter. */
  private final DecodedCell<?> mValue;

  /**
   * Creates a new <code>ColumnValueEqualsRowFilter</code> instance.
   *
   * @param family The column family of interest.
   * @param qualifier The column qualifier of interest.
   * @param value The value the most recent cell in the column must equal to pass the filter.
   */
  public ColumnValueEqualsRowFilter(String family, String qualifier, DecodedCell<?> value) {
    if (null == family || family.isEmpty()) {
      throw new IllegalArgumentException("family is required");
    }
    if (null == qualifier || qualifier.isEmpty()) {
      throw new IllegalArgumentException("qualifier is required");
    }
    if (null == value) {
      throw new IllegalArgumentException(
          "value may not be null. If you want to check for column data presence, use "
          + HasColumnDataRowFilter.class.getName());
    }
    mFamily = family;
    mQualifier = qualifier;
    mValue = value;
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    return KijiDataRequest.create(mFamily, mQualifier);
  }

  /** {@inheritDoc} */
  @Override
  public Filter toHBaseFilter(Context context) throws IOException {
    // Create a filter that accepts a cell from mFamily and mQualifier only if it is
    // equals to the specified value.
    final KijiColumnName column = KijiColumnName.create(mFamily, mQualifier);
    HBaseColumnName hbaseColumnName = context.getHBaseColumnName(column);
    SingleColumnValueFilter filter = new SingleColumnValueFilter(
        hbaseColumnName.getFamily(),
        hbaseColumnName.getQualifier(),
        CompareOp.EQUAL,
        context.getHBaseCellValue(column, mValue));

    filter.setLatestVersionOnly(true);
    filter.setFilterIfMissing(true);

    // Skip the entire row if the filter does not allow the column value.
    return new SkipFilter(filter);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ColumnValueEqualsRowFilter)) {
      return false;
    } else {
      final ColumnValueEqualsRowFilter otherFilter = (ColumnValueEqualsRowFilter) other;
      return Objects.equal(otherFilter.mFamily, this.mFamily)
          && Objects.equal(otherFilter.mQualifier, this.mQualifier)
          && Objects.equal(otherFilter.mValue, this.mValue);
    }
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mFamily, mQualifier, mValue);
  }

  /** {@inheritDoc} */
  @Override
  protected JsonNode toJsonNode() {
    final ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(FAMILY_NODE, mFamily);
    root.put(QUALIFIER_NODE, mQualifier);
    final ObjectNode value = root.with(VALUE_NODE);
    // Schema's documentation for toString says it is rendered as JSON.
    value.put(SCHEMA_NODE, mValue.getWriterSchema().toString());
    try {
      value.put(DATA_NODE, ToJson.toAvroJsonString(mValue.getData(), mValue.getWriterSchema()));
    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }
    return root;
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends KijiRowFilterDeserializer> getDeserializerClass() {
    return ColumnValueEqualsRowFilterDeserializer.class;
  }

  /** Deserializes {@code ColumnValueEqualsRowFilter}. */
  public static final class ColumnValueEqualsRowFilterDeserializer
      implements KijiRowFilterDeserializer {
    /** {@inheritDoc} */
    @Override
    public KijiRowFilter createFromJson(JsonNode root) {
      final String family = root.path(FAMILY_NODE).getTextValue();
      final String qualifier = root.path(QUALIFIER_NODE).getTextValue();
      final String schema = root.path(VALUE_NODE).path(SCHEMA_NODE).getTextValue();
      final Schema writerSchema = (new Schema.Parser()).parse(schema);
      final String data = root.path(VALUE_NODE).path(DATA_NODE).getTextValue();
      try {
        final DecodedCell<?> cell = new DecodedCell<Object>(writerSchema,
            FromJson.fromAvroJsonString(data, writerSchema));
        return new ColumnValueEqualsRowFilter(family, qualifier, cell);
      } catch (IOException ioe) {
        throw new KijiIOException(ioe);
      }
    }
  }
}
