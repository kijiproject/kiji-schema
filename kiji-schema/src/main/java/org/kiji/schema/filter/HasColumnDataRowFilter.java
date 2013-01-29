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

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.hbase.HBaseColumnName;

/**
 * A KijiRowFilter that excludes rows that have no data for some column <code>columnName</code>.
 */
@ApiAudience.Public
public final class HasColumnDataRowFilter extends KijiRowFilter {
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

    KijiColumnName kijiColName = new KijiColumnName(columnName);
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
    builder.addColumns().add(mFamily, mQualifier);
    return builder.build();
  }

  /** {@inheritDoc} */
  @Override
  public Filter toHBaseFilter(Context context) throws IOException {
    // Create a filter that accepts a cell from mFamily and mQualifier only if it is
    // not an empty byte array.  Since all kiji cells have data (at the very least, the
    // schema hash), this will accept all cells that exist.
    HBaseColumnName hbaseColumnName = context.getHBaseColumnName(
        new KijiColumnName(mFamily, mQualifier));
    SingleColumnValueFilter filter = new SingleColumnValueFilter(
        hbaseColumnName.getFamily(),
        hbaseColumnName.getQualifier(),
        CompareOp.NOT_EQUAL,
        new byte[0]);

    // If there are no cells in mFamily and mQualifier, skip the entire row.
    filter.setFilterIfMissing(true);

    return filter;
  }
}
