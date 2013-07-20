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

import java.io.Closeable;
import java.util.Iterator;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/**
 * Retrieves pages of values from a column in a Kiji table.
 *
 * <p> KijiPager is useful when requesting a large number of values from a column.
 *   The page size is an upper limit to the number of cells retrieved from the region servers
 *   at a time, to bound the amount of memory consumed on the client machine.
 * </p>
 * <p> To enable paging on a column, use
 *   {@link KijiDataRequestBuilder$ColumnsDef#withPageSize(int)} when building a
 *   {@link KijiDataRequest}. For example, using a group type column <code>"info:name"</code>,
 *   you may request a maximum of 2 versions per page, with a maximum of 5 versions per qualifier
 *   as follows:
 *   <pre>
 *     final KijiDataRequest dataRequest = KijiDataRequest.builder()
 *         .addColumnsDef(ColumnsDef.create()
 *             .withMaxVersions(5)  // at most 5 versions per column qualifier
 *             .withPageSize(2)     // at most 2 cells per page
 *             .add("info", "name"))
 *         .build();
 *   </pre>
 * </p>
 * <p> To get a {@link KijiPager}, call {@link KijiRowData#getPager(java.lang.String)} or
 *   {@link KijiRowData#getPager(String, String)} on a {@link KijiRowData} constructed with paging
 *   enabled:
 *   <pre>
 *     final KijiTableReader reader = ...
 *     final EntityId entityId = ...
 *     final KijiRowData row = reader.get(entityId, dataRequest);
 *     final KijiPager pager = row.getPager("info", "name");
 *     try  {
 *       while(pager.hasNext()) {
 *         final KijiRowData page = pager.next();
 *         // Use: page.getValues("info", "name")
 *         // ...
 *       }
 *     } finally {
 *       // Always close pagers:
 *       pager.close();
 *     }
 *   </pre>
 * </p>
 *
 * Notes:
 * <ul>
 *   <li> The page size in an upper bound to the number of cells retrieved in a page.
 *     Concretely, a page of cells returned by {@link KijiPager#next()} or
 *     {@link KijiPager#next(int)} may contain less cells than the page size,
 *     even if there are more pages coming.
 *     In particular, there may be empty pages even when more pages follow.
 *     Use {@link KijiPager#hasNext()} to determine if more pages follow.
 *   </li>
 *   <li> When paging over a map-type family, retrieved pages only contain the qualifiers
 *     in the family, and no cell content.
 *     You may retrieve the cell content with a get request with the qualifiers from the pager.
 *     If you need to retrieve many versions for all these qualifier, you may combine the
 *     map-family pager with per-qualifier pagers.
 *   </li>
 * </ul>
 *
 * <p>
 *   If you don't need access to individual pages, you may use the following iterator/iterable
 *   helpers:
 *   <ul>
 *     <li> {@link ColumnVersionIterator} iterates through the versions in a given column. </li>
 *     <li>
 *       {@link MapFamilyQualifierIterator} iterates through the qualifiers in a map-type family.
 *     </li>
 *     <li>
 *       {@link MapFamilyVersionIterator} iterates through the versions in all columns
 *       from a map-type family.
 *     </li>
 *   </ul>
 * </p>
 *
 * @see ColumnVersionIterator
 * @see MapFamilyQualifierIterator
 * @see MapFamilyVersionIterator
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
public interface KijiPager extends Iterator<KijiRowData>, Closeable {
  /**
   * Fetches the next page of cells from the Kiji table using the configured page size.
   *
   * <ul>
   *   <li> The page size is an upper bound to the number of cells retrieved from the table. </li>
   *   <li> The page may contain fewer cells than the specified page size.
   *        In particular, the page can sometimes be empty, even though more pages follow. </li>
   *        Use {@link KijiPager#hasNext()} to determine if more pages follow.
   * </ul>
   *
   * @return the next page of cells as a {@link KijiRowData}.
   *     Never null, but potentially empty.
   */
  KijiRowData next();

  /**
   * Fetches the next page of cells from the Kiji table using the specified page size.
   *
   * <ul>
   *   <li> The page size is an upper bound to the number of cells retrieved from the table. </li>
   *   <li> The page may contain less cells than the specified page size.
   *        In particular, the page can sometimes be empty, even though more pages follow. </li>
   *        Use {@link KijiPager#hasNext()} to determine if more pages follow.
   * </ul>
   *
   * @param pageSize The maximum number of cells to retrieve for this page.
   * @return the next page of data as a {@link KijiRowData}.
   *     Never null, but potentially empty.
   */
  KijiRowData next(int pageSize);

  /**
   * Throws {@link UnsupportedOperationException}.
   *
   * <p> Pagers do not support remove(). </p>.
   */
  void remove();
}
