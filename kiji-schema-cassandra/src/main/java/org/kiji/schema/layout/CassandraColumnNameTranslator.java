/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.schema.layout;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.cassandra.CassandraColumnName;
import org.kiji.schema.layout.impl.cassandra.ShortColumnNameTranslator;

/**
 * Translates between Cassandra and Kiji table column names.
 *
 * <p>This abstract class defines an interface for mapping between names of Cassandra HTable
 * families/qualifiers and Kiji table family/qualifiers.</p>
 *
 * TODO: Update Cassandra column name translators with identity and native.
 */
@ApiAudience.Framework
@ApiStability.Experimental
public abstract class CassandraColumnNameTranslator {
  /**
   * Creates a new {@link org.kiji.schema.layout.CassandraColumnNameTranslator} instance.
   * Supports the {@link ShortColumnNameTranslator} based on the table layout.
   *
   * @param tableLayout The layout of the table to translate column names for.
   * @return {@link org.kiji.schema.layout.CassandraColumnNameTranslator} of the appropriate type.
   */
  public static CassandraColumnNameTranslator from(KijiTableLayout tableLayout) {
    switch (tableLayout.getDesc().getColumnNameTranslator()) {
      case SHORT:
        return new ShortColumnNameTranslator(tableLayout);
      default:
        throw new UnsupportedOperationException(String.format(
            "Unsupported CassandraColumnNameTranslator: %s for column: %s.",
            tableLayout.getDesc().getColumnNameTranslator(),
            tableLayout.getName()));
    }
  }

  /**
   * Translates a Cassandra column name to a Kiji column name.
   *
   * @param cassandraColumnName the Cassandra column name.
   * @return The Kiji column name.
   * @throws NoSuchColumnException If the column name cannot be found.
   */
  public abstract KijiColumnName toKijiColumnName(CassandraColumnName cassandraColumnName)
      throws NoSuchColumnException;

  /**
   * Translates a Kiji column name into a Cassandra column name.
   *
   * @param kijiColumnName The Kiji column name.
   * @return The Cassandra column name.
   * @throws NoSuchColumnException If the column name cannot be found.
   */
  public abstract CassandraColumnName toCassandraColumnName(KijiColumnName kijiColumnName)
      throws NoSuchColumnException;
}
