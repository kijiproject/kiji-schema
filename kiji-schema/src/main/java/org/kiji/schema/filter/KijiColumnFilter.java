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
import java.io.Serializable;

import org.apache.hadoop.hbase.filter.Filter;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;

/**
 * A column filter provides a means of filtering cells from a column on the server side.
 *
 * <p>To make your jobs more efficient, you may use a KijiColumnFilter to specify that
 * certain cells from a column be filtered. The cells will be filtered on the server,
 * which reduces the amount of data that needs to be sent to the client.</p>
 *
 * <p>KijiColumnFilters filter cells from a column, in contrast with KijiRowFilters, which
 * filters rows from a table.</p>
 *
 * <p>Classes extending KijiColumnFilter must implement the <code>hashCode</code> and
 * <code>equals</code> methods.</p>
 *
 * @see org.kiji.schema.filter.KijiRowFilter
 * @see org.kiji.schema.KijiDataRequestBuilder.ColumnsDef#withFilter(KijiColumnFilter)
 */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Extensible
public abstract class KijiColumnFilter implements Serializable {
  /**
   * An object available to KijiColumnFilters that can be used to help implement the
   * toHBaseFilter() method.
   */
  @ApiAudience.Public
  @Inheritance.Sealed
  public abstract static class Context {
    /**
     * Converts a Kiji column name to an HBase column name.
     *
     * @param kijiColumnName The name of a kiji column.
     * @return The name of the HBase column that stores the kiji column data.
     * @throws NoSuchColumnException If there is no such column in the kiji table.
     */
    public abstract HBaseColumnName getHBaseColumnName(KijiColumnName kijiColumnName)
        throws NoSuchColumnException;
  }

  /**
   * Expresses the KijiColumnFilter in terms an equivalent HBase Filter.
   *
   * @param kijiColumnName The column this filter applies to.
   * @param context The context.
   * @return An equivalent HBase Filter.
   * @throws IOException If there is an error.
   */
  public abstract Filter toHBaseFilter(KijiColumnName kijiColumnName, Context context)
      throws IOException;
}
