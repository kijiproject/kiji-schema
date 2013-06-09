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
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Iterates through the versions in a fully-qualified column.
 *
 * <p> Wraps a fully-qualified column pager. </p>
 * <p>
 *   You may construct a version iterator on a column with paging enabled as follows:
 *   <pre><tt>{@code
 *   final String family = ...;
 *   final String qualifier = ...;
 *   final KijiDataRequest dataRequest = KijiDataRequest.builder()
 *       .addColumns(ColumnsDef.create()
 *           .withMaxVersions(HConstants.ALL_VERSIONS)
 *           .withPageSize(1)  // enable paging on this column (this page size isn't used)
 *           .add(family, qualifier))
 *       .build();
 *
 *   final KijiRowData row = mReader.get(eid, dataRequest);
 *   final int versionPageSize = 3;  // retrieve 3 cells at a time
 *   final ColumnVersionIterator<String> iterable =
 *       new ColumnVersionIterator<String>(row, family, qualifier, versionPageSize);
 *   try {
 *     for (Entry<Long, String> entry : iterable) {
 *       // entry.getKey() is the timestamp of the cell version.
 *       // entry.getValue() is the content of the cell version.
 *     }
 *   } finally {
 *     iterable.close();
 *   }
 *   }</tt></pre>
 *   Done
 * </p>
 *
 * @param <T> Type of the cells in the column.
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class ColumnVersionIterator<T>
    implements Closeable, Iterator<Entry<Long, T>>, Iterable<Entry<Long, T>> {

  private final KijiRowData mRow;
  private final String mFamily;
  private final String mQualifier;
  private final KijiPager mVersionPager;
  private final int mPageSize;
  private Iterator<Entry<Long, T>> mPage = null;
  private Entry<Long, T> mNext = null;

  /**
   * Initializes a new iterator over the versions in a fully-qualified column.
   *
   * <p>
   *   The version iterator wraps a column pager.
   *   The size of the version page determines how many cells/versions to retrieve per RPC.
   *   Smaller values will reduce the memory footprint but increase the number of RPCs.
   *   To properly configure the version page size, the user must estimate or bound the expected
   *   size of the cells being retrieved (ie. the size of the KeyValue objects).
   * </p>
   *
   * @param row Kiji row data with paging enabled on the specified map-type family.
   * @param family Family name containing the column to iterate through.
   * @param qualifier Qualifier of the column to iterate through.
   * @param pageSize Size of the underlying pages of versions.
   * @throws IOException on I/O error.
   */
  public ColumnVersionIterator(KijiRowData row, String family, String qualifier, int pageSize)
      throws IOException {

    this.mRow = row;
    this.mFamily = family;
    this.mQualifier = qualifier;
    this.mPageSize = pageSize;
    this.mVersionPager = this.mRow.getPager(this.mFamily, this.mQualifier);
    this.mNext = getNext();
  }

  /**
   * Reports the next version for the specified column, or null.
   *
   * <p>
   *   In effect, this pre-loads the next entry to return.
   *   This allows to determine whether the iterator is finished or has more elements.
   * </p>
   *
   * @return the next version for the specified column, or null.
   * @throws IOException on I/O error.
   */
  private Entry<Long, T> getNext() throws IOException {
    while ((mPage == null) || !mPage.hasNext()) {
      if (!mVersionPager.hasNext()) {
        return null;
      }
      final KijiRowData rowPage = mVersionPager.next(this.mPageSize);
      mPage = rowPage.<T>getValues(mFamily, mQualifier).entrySet().iterator();
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
  public Entry<Long, T> next() {
    final Entry<Long, T> entry = mNext;
    if (entry == null) {
      throw new NoSuchElementException();
    }
    try {
      mNext = getNext();
    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }
    return entry;
  }

  /** {@inheritDoc} */
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /** {@inheritDoc} */
  @Override
  public Iterator<Entry<Long, T>> iterator() {
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mVersionPager.close();
  }
}
