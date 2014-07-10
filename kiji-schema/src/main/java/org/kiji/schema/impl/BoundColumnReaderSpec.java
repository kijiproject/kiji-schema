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
package org.kiji.schema.impl;

import com.google.common.base.Objects;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.layout.ColumnReaderSpec;

/**
 * Binding of a {@link ColumnReaderSpec} to a specific column. The table in which the column can be
 * found is assumed based on the context in which the BoundColumnReaderSpec is used.
 */
@ApiAudience.Private
@ApiStability.Experimental
public final class BoundColumnReaderSpec {
  private final ColumnReaderSpec mColumnReaderSpec;
  private final KijiColumnName mColumn;

  /**
   * Initialize a new BoundColumnReaderSpec. This binding does not
   * modify the ColumnReaderSpec, nor prevent it from being bound to other columns simultaneously.
   *
   * @param columnReaderSpec the ColumnReaderSpec to bind.
   * @param column column to which to bind the ColumnReaderSpec.
   */
  private BoundColumnReaderSpec(
      final ColumnReaderSpec columnReaderSpec,
      final KijiColumnName column
  ) {
    mColumnReaderSpec = columnReaderSpec;
    mColumn = column;
  }

  /**
   * Bind a {@link ColumnReaderSpec} to the given column in the given table. This binding does not
   * modify the ColumnReaderSpec, nor prevent it from being bound to other columns simultaneously.
   *
   * @param columnReaderSpec the ColumnReaderSpec to bind.
   * @param column column to which to bind the ColumnReaderSpec.
   * @return a new BoundColumnReaderSpec.
   */
  public static BoundColumnReaderSpec create(
      final ColumnReaderSpec columnReaderSpec,
      final KijiColumnName column
  ) {
    return new BoundColumnReaderSpec(columnReaderSpec, column);
  }

  /**
   * Get the ColumnReaderSpec which is bound to the given column.
   *
   * @return the ColumnReaderSpec which is bound to the given column.
   */
  public ColumnReaderSpec getColumnReaderSpec() {
    return mColumnReaderSpec;
  }

  /**
   * Get the column which the ColumnReaderSpec is bound.
   *
   * @return the column which the ColumnReaderSpec is bound.
   */
  public KijiColumnName getColumn() {
    return mColumn;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mColumnReaderSpec, mColumn);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(
      final Object object
  ) {
    if (!(object instanceof BoundColumnReaderSpec)) {
      return false;
    }
    final BoundColumnReaderSpec that = (BoundColumnReaderSpec) object;
    return (Objects.equal(this.mColumnReaderSpec, that.mColumnReaderSpec)
        && (Objects.equal(this.mColumn, that.mColumn)));
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(BoundColumnReaderSpec.class)
        .add("column", mColumn)
        .add("column_reader_spec", mColumnReaderSpec)
        .toString();
  }
}
