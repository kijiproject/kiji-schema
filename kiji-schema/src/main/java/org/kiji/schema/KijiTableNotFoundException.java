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
@ApiStability.Stable
public final class KijiTableNotFoundException extends IOException {
  /** URI of the missing table. */
  private final KijiURI mTableURI;

  /**
   * Creates a new <code>KijiTableNotFoundException</code> for the specified table.
   *
   * @param tableURI URI of the table that wasn't found.
   * @deprecated Use {@link KijiTableNotFoundException#KijiTableNotFoundException(KijiURI).
   */
  @Deprecated
  public KijiTableNotFoundException(String tableURI) {
    super("KijiTable not found: " + tableURI);
    mTableURI = KijiURI.newBuilder(tableURI).build();
  }

  /**
   * Creates a new <code>KijiTableNotFoundException</code> for the specified table.
   *
   * @param tableURI URI of the table that wasn't found.
   */
  public KijiTableNotFoundException(KijiURI tableURI) {
    super("KijiTable not found: " + tableURI);
    mTableURI = tableURI;
  }

  /**
   * Returns the name of the missing table.
   * @return the name of the missing table.
   */
  public String getTableName() {
    return mTableURI.getTable();
  }

  /**
   * Returns the URI of the missing table.
   * @return the URI of the missing table.
   */
  public KijiURI getTableURI() {
    return mTableURI;
  }
}
