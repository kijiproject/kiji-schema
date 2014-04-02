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
 * Thrown when a client attempts to fetch the next page of data from a Kiji column, but paging is
 * not enabled on the column.
 *
 * <p>To enable paging on a column, use the
 * {@link org.kiji.schema.KijiDataRequestBuilder.ColumnsDef#withPageSize(int)}
 * method in your {@link org.kiji.schema.KijiDataRequestBuilder}.</p>
 */
@ApiAudience.Public
@ApiStability.Stable
public final class KijiColumnPagingNotEnabledException extends IOException {
  /**
   * Creates a new <code>KijiColumnPagingNotEnabledException</code> with the specified
   * detail message.
   *
   * @param message An error message.
   */
  public KijiColumnPagingNotEnabledException(String message) {
    super(message);
  }
}
