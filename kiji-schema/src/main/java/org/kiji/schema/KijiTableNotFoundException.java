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

package org.kiji.schema;

import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Thrown when an attempt to access a table fails because it does not exist.
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class KijiTableNotFoundException extends IOException {
  /** Name of the missing table. */
  private final String mTableName;

  /**
   * Creates a new <code>KijiTableNotFoundException</code> for the specified table.
   *
   * @param tableName The name of the table that wasn't found.
   */
  public KijiTableNotFoundException(String tableName) {
    super("Table not found: " + tableName);
    mTableName = tableName;
  }

  /** @return the name of the missing table. */
  public String getTableName() {
    return mTableName;
  }
}
