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

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;

import org.kiji.schema.HBaseColumnName;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;

/**
 * A KijiRowFilter that only includes rows where a specific column's most recent value
 * equals a specified value.
 *
 * <p>This filter will only pass if the data in the Kiji cell matches the specified value
 * <i>exactly</i>.  Both the data and the Avro schema must be the same.</p>
 */
public class ColumnValueEqualsRowFilter extends KijiRowFilter {
  /** The name of the column family to check for data in. */
  private final String mFamily;

  /** The name of the column qualifier to check for data in. */
  private final String mQualifier;

  /** The value the most recent column value must equal to pass the filter. */
  private final KijiCell<?> mValue;

  /**
   * Creates a new <code>ColumnValueEqualsRowFilter</code> instance.
   *
   * @param family The column family of interest.
   * @param qualifier The column qualifier of interest.
   * @param value The value the most recent cell in the column must equal to pass the filter.
   */
  public ColumnValueEqualsRowFilter(String family, String qualifier, KijiCell<?> value) {
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
    return new KijiDataRequest().addColumn(new KijiDataRequest.Column(mFamily, mQualifier));
  }

  /** {@inheritDoc} */
  @Override
  public Filter toHBaseFilter(Context context) throws IOException {
    // Create a filter that accepts a cell from mFamily and mQualifier only if it is
    // equals to the specified value.
    final KijiColumnName column = new KijiColumnName(mFamily, mQualifier);
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
}
