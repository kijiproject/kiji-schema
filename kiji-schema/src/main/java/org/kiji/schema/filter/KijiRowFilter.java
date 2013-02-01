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

import org.apache.hadoop.hbase.filter.Filter;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;

/**
 * The abstract base class for filters that exclude data from KijiRows.
 */
@ApiAudience.Public
@Inheritance.Extensible
public abstract class KijiRowFilter {
  /**
   * A helper class for converting between Kiji objects and their HBase counterparts.
   */
  @ApiAudience.Public
  @Inheritance.Sealed
  public abstract static class Context {
    /**
     * Converts a Kiji row key into an HBase row key.
     *
     * <p>This method is useful only on tables with row key hashing disabled, since hashed
     * row keys have no ordering or semantics beyond being an identifier.</p>
     *
     * @param kijiRowKey A kiji row key.
     * @return The corresponding HBase row key.
     */
    public abstract byte[] getHBaseRowKey(String kijiRowKey);

    /**
     * Converts a Kiji column name to an HBase column family name.
     *
     * @param kijiColumnName The name of a kiji column.
     * @return The name of the HBase column that stores the kiji column data.
     * @throws NoSuchColumnException If there is no such column in the kiji table.
     */
    public abstract HBaseColumnName getHBaseColumnName(KijiColumnName kijiColumnName)
        throws NoSuchColumnException;

    /**
     * Converts a Kiji cell value into an HBase cell value.
     *
     * @param column Name of the column this cell belongs to.
     * @param kijiCell A kiji cell value.
     * @return The Kiji cell encoded as an HBase value.
     * @throws IOException If there is an error encoding the cell value.
     */
    public abstract byte[] getHBaseCellValue(KijiColumnName column, DecodedCell<?> kijiCell)
        throws IOException;
  }

  /**
   * Describes the data the filter requires to determine whether a row should be accepted.
   *
   * @return The data request.
   */
  public abstract KijiDataRequest getDataRequest();

  /**
   * Constructs an HBase <code>Filter</code> instance that can be used to instruct the
   * HBase region server which rows to filter.
   *
   * <p>You must use the given <code>context</code> object when referencing any HBase
   * table coordinates or values.  Using a Kiji row key, column family name, or column
   * qualifier name when configuring an HBase filter will result in undefined
   * behavior.</p>
   *
   * <p>For example, when constructing an HBase <code>SingleColumnValueFilter</code> to
   * inspect the "info:name" column of a Kiji table, use {@link
   * KijiRowFilter.Context#getHBaseColumnName(KijiColumnName)} to
   * retrieve the HBase column family and qualifier.  Here's an implementation that
   * filters out rows where the latest version of the 'info:name' is equal to 'Bob'.
   *
   * <pre>
   * KijiCell&lt;CharSequence&gt; bobCellValue = new KijiCell&lt;CharSequence&gt;(
   *     Schema.create(Schema.Type.STRING), "Bob");
   * HBaseColumnName hbaseColumn = context.getHBaseColumnName(
   *     new KijiColumnName("info", "name"));
   * SingleColumnValueFilter filter = new SingleColumnValueFilter(
   *     hbaseColumn.getFamily(),
   *     hbaseColumn.getQualifier(),
   *     CompareOp.NOT_EQUAL,
   *     context.getHBaseCellValue(bobCellValue));
   * filter.setLatestVersionOnly(true);
   * filter.setFilterIfMissing(false);
   * return new SkipFilter(filter);</pre>
   * </p>
   *
   * @param context A helper object you can use to convert Kiji objects into their HBase
   *     counterparts.
   * @return The HBase filter the implements the semantics of this KijiRowFilter.
   * @throws IOException If there is an error.
   */
  public abstract Filter toHBaseFilter(Context context) throws IOException;
}
