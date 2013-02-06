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
import org.kiji.annotations.Inheritance;

/**
 * <p> Manages retrieving pages of values from a column in a Kiji table. This is useful when you
 * are requesting a large number of values from a column.</p>
 *
 * <p> To get a KijiPager, first configure the default page size for the column on the corresponding
 * {@link KijiDataRequest.Column}. Then call {@link KijiRowData#getPager(java.lang.String)} on the
 * the {@link KijiRowData} constructed with your data request</p>
 *
 * <p> For example, you could request two versions per page, and a maximum of 5 versions for the
 *  whole request:
 *
 * <pre>
 *  KijiDataRequestBuilder builder = KijiDataRequest.builder();
 *  builder.addColumns().withMaxVersions(5).withPageSize(2).add("info", "name");
 *  final KijiDataRequest dataRequest = builder.build();
 * </pre>
 * </p>
 *
 * <p>
 * The row data constructed using this data request may then be used to retrieve a pager and
 * process results:
 * <pre>
 *  KijiPager pager = myRowData.getPager("info", "name");
 *  while(pager.hasNext()) {
 *    final NavigableMap<Long, CharSequence> resultMap = pager.next().getValues("info", "name");
 *    ...
 * }
 * </pre>
 * </p>
 */
@ApiAudience.Public
@Inheritance.Sealed
public interface KijiPager extends Iterator<KijiRowData>, Closeable {

  /**
  * Gets the next page of results, with specified page size, for a column or family. The
  * KijiRowData returned may potentially have no values. This method does not throw a
  * NoSuchElementException.
  *
  * @param pageSize The number of cells to retrieve for this page.
  * @return The HBase result containing the next page of data, or an empty {@link KijiRowData}
  * if there is no more data.
  */
  KijiRowData next(int pageSize);
}
