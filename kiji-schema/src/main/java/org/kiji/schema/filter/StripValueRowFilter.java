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
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiDataRequest;

/**
 * A KijiRowFilter that applies HBase's KeyOnly filter to the KijiDataRequest. This strips out
 * all values from all columns and is useful if you just want to retrieve timestamps. Note that
 * attempting to decode the data (through calls like KijiRowData.getValues()) will
 * result in an IOException.
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class StripValueRowFilter extends KijiRowFilter {

  /** All StripValueRowFilter instances are the same: generate hash-code ahead of time. */
  private static final int HASH_CODE =
      new HashCodeBuilder().append(StripValueRowFilter.class).toHashCode();

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    return KijiDataRequest.builder().build();
  }

  /** {@inheritDoc} */
  @Override
  public Filter toHBaseFilter(Context context) throws IOException {
    return new KeyOnlyFilter();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    // All StripValueRowFilters are the same.
    return other instanceof StripValueRowFilter;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return HASH_CODE;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(StripValueRowFilter.class).toString();
  }

  /** {@inheritDoc} */
  @Override
  protected JsonNode toJsonNode() {
    return JsonNodeFactory.instance.nullNode();
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends KijiRowFilterDeserializer> getDeserializerClass() {
    return StripValueRowFilterDeserializer.class;
  }

  /** Deserializes {@code StripValueRowFilter}. */
  public static class StripValueRowFilterDeserializer implements KijiRowFilterDeserializer {
    /** {@inheritDoc} */
    @Override
    public KijiRowFilter createFromJson(JsonNode root) {
      return new StripValueRowFilter();
    }
  }
}
