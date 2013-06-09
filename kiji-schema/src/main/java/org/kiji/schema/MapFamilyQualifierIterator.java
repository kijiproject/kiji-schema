/**
 * (c) Copyright 2013 WibiData, Inc.
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
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Iterates through the qualifiers in a map-type family.
 *
 * <p> Wraps a map-type family pager. </p>
 * <p>
 *   You may construct a qualifier iterator on a map-type family with paging enabled as follows:
 *   <tt><pre>{@code
 *   final EntityId eid = ...;
 *   final String family = ...;
 *   final KijiDataRequest dataRequest = KijiDataRequest.builder()
 *       .addColumns(ColumnsDef.create()
 *           .withMaxVersions(HConstants.ALL_VERSIONS)
 *           .withPageSize(1)  // Enable paging on this family (this page size isn't used).
 *           .addFamily(family))
 *       .build();
 *
 *   final KijiRowData row = mReader.get(eid, dataRequest);
 *   final int qualifierPageSize = 3;  // retrieve 3 qualifiers at a time.
 *   final MapFamilyQualifierIterator iterable =
 *       new MapFamilyQualifierIterator(row, family, qualifierPageSize);
 *   try {
 *     for (String qualifier : iterable) {
 *       // Use the qualifier...
 *     }
 *   } finally {
 *     iterable.close();
 *   }
 *   }</pre></tt>
 * </p>
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class MapFamilyQualifierIterator
    implements Closeable, Iterator<String>, Iterable<String> {

  private final KijiRowData mRow;
  private final String mFamily;
  private final KijiPager mQualifierPager;
  private final int mPageSize;
  private Iterator<String> mPage = null;
  private String mNext = null;

  /**
   * Initializes a new iterator over the qualifiers from a map-type family.
   *
   * <p>
   *   The qualifier iterator wraps a map-type family pager.
   *   The size of the qualifier page determines how many qualifiers to retrieve per RPC.
   *   Smaller values will reduce the memory footprint but increase the number of RPCs.
   *   The size of a qualifier can be estimated or bounded as the size of a KeyValue with no value.
   * </p>
   *
   * @param row Kiji row data with paging enabled on the specified map-type family.
   * @param family Map-type family name.
   * @param pageSize Size of the underlying qualifier pages.
   * @throws IOException on I/O error.
   */
  public MapFamilyQualifierIterator(KijiRowData row, String family, int pageSize)
      throws IOException {

    this.mRow = row;
    this.mFamily = family;
    this.mPageSize = pageSize;
    this.mQualifierPager = this.mRow.getPager(family);
    this.mNext = getNext();
  }

  /**
   * Reports the next qualifier from the map-type family, or null.
   *
   * <p>
   *   In effect, this pre-loads the next entry to return.
   *   This allows to determine whether the iterator is finished or has more elements.
   * </p>
   *
   * @return the next qualifier from the map-type family, or null.
   */
  private String getNext() {
    while ((mPage == null) || !mPage.hasNext()) {
      if (!mQualifierPager.hasNext()) {
        return null;
      }
      final KijiRowData rowPage = mQualifierPager.next(this.mPageSize);
      mPage = rowPage.getQualifiers(mFamily).iterator();
    }
    return mPage.next();
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    return (mNext != null);
  }

  /** {@inheritDoc} */
  @Override
  public String next() {
    final String qualifier = mNext;
    if (qualifier == null) {
      throw new NoSuchElementException();
    }
    mNext = getNext();
    return qualifier;
  }

  /** {@inheritDoc} */
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /** {@inheritDoc} */
  @Override
  public Iterator<String> iterator() {
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mQualifierPager.close();
  }
}
