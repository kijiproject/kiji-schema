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
 * Filter that replaces the value of each cell by its size, in number of bytes.
 *
 * <p> Each cell size is encoded as a 32 bits integer (see
 *   {@link org.apache.hadoop.hbase.util.Bytes#toInt(byte[])}).</p>
 * <p>
 *   In order to use this filter, you must override the reader schema of the affected columns
 *   as a final fixed byte array of size 4.
 *
 *   <pre><tt>{@code
 *   final KijiTable table = ...;
 *   final KijiTableLayout layout = table.getLayout();
 *   final KijiColumnName column = KijiColumnName.create("family", "qualifier");
 *   final Map<KijiColumnName, CellSpec> overrides =
 *       ImmutableMap.<KijiColumnName, CellSpec>builder()
 *       .put(column, layout.getCellSpec(column)
 *           .setCellSchema(CellSchema.newBuilder()
 *               .setType(SchemaType.INLINE)
 *               .setStorage(SchemaStorage.FINAL)
 *               .setValue("{\"type\": \"fixed\", \"size\": 4, \"name\": \"Int32\"}")
 *               .build()))
 *       .build();
 *
 *   final KijiTableReader reader = table.getReaderFactory().openTableReader(overrides);
 *   try {
 *     final KijiDataRequest dataRequest = KijiDataRequest.builder()
 *         .addColumns(ColumnsDef.create().withFilter(new CellByteSizeAsValueFilter()).add(column))
 *         .build();
 *     final EntityId eid = table.getEntityId(...);
 *     final KijiRowData row = reader.get(eid, dataRequest);
 *     final GenericData.Fixed fixed32 = row.getMostRecentValue("family", "qualifier");
 *     final int cellSize = Bytes.toInt(fixed32.bytes());
 *     ...
 *   } finally {
 *     reader.close();
 *   }
 *   }</tt></pre>
 * </p>
 * <p> TODO(SCHEMA-334): This filter may currently not be combined properly via
 *     {@link org.apache.hadoop.hbase.filter.FilterList}.
 *     If you need to apply this filter to specific columns, you must currently send separate get
 *     requests.
 * </p>
 */
@ApiAudience.Framework
@ApiStability.Experimental
public final class CellByteSizeAsValueFilter extends KijiColumnFilter {
  private static final long serialVersionUID = 1L;

  /** All StripValueColumnFilter instances are the same: generate hash-code ahead of time. */
  private static final int HASH_CODE =
      new HashCodeBuilder().append(CellByteSizeAsValueFilter.class).toHashCode();

  /** {@inheritDoc} */
  @Override
  public Filter toHBaseFilter(KijiColumnName kijiColumnName, Context context) throws IOException {
    return new KeyOnlyFilter(true);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    // All StripValueRowFilters are the same.
    return other instanceof CellByteSizeAsValueFilter;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return HASH_CODE;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(CellByteSizeAsValueFilter.class).toString();
  }
}
