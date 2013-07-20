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
import java.util.Map;
import java.util.NoSuchElementException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Iterators through the cells in a map-type family,
 *
 * <p>
 *   This iterator combines a {@link MapFamilyQualifierIterator} and many
 *   {@link ColumnVersionIterator}s
 * </p>
 * <p>
 *   You may construct a version iterator on a map-type family with paging enabled as follows:
 *   <tt><pre>{@code
 *   final String family = ...;
 *   final KijiDataRequest dataRequest = KijiDataRequest.builder()
 *       .addColumns(ColumnsDef.create()
 *           .withMaxVersions(HConstants.ALL_VERSIONS)
 *           .withPageSize(1)  // Enable paging on this family (this page size isn't used).
 *           .addFamily(family))
 *       .build();
 *
 *   final KijiRowData row = mReader.get(eid, dataRequest);
 *   final int qualifierPageSize = 10;  // Retrieve 10 qualifiers at a time.
 *   final int versionPageSize = 3;     // Retrieve 3 cells at a time.
 *   final MapFamilyVersionIterator<String> iterable =
 *       new MapFamilyVersionIterator<String>(row, family, qualifierPageSize, versionPageSize);
 *   try {
 *     for (Entry<String> entry : iterable) {
 *       // Each entry has a qualifier, a timestamp and a value.
 *     }
 *   } finally {
 *     iterable.close();
 *   }
 *   }</pre></tt>
 * </p>
 *
 * @param <T> Type of the cells in the map-type family.
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class MapFamilyVersionIterator<T>
    implements Closeable,
        Iterator<MapFamilyVersionIterator.Entry<T>>,
        Iterable<MapFamilyVersionIterator.Entry<T>> {

  /**
   * Entry from a map-type family.
   *
   * @param <T> Type of the values in the map-type family.
   */
  public static final class Entry<T> {
    private final String mQualifier;
    private final long mTimestamp;
    private final T mValue;

    /**
     * Constructs a new entry.
     *
     * @param qualifier Qualifier of the new entry.
     * @param timestamp Timestamp of the new entry.
     * @param value Value of the new entry.
     */
    private Entry(String qualifier, long timestamp, T value) {
      mQualifier = qualifier;
      mTimestamp = timestamp;
      mValue = value;
    }

    /**
     * Reports the qualifier for this entry.
     *
     * @return the qualifier for this entry.
     */
    public String getQualifier() {
      return mQualifier;
    }

    /**
     * Reports the timestamp for this entry, in milliseconds since the Epoch.
     *
     * @return the timestamp for this entry, in milliseconds since the Epoch.
     */
    public long getTimestamp() {
      return mTimestamp;
    }

    /**
     * Reports the value for this entry.
     *
     * @return the value for this entry.
     */
    public T getValue() {
      return mValue;
    }
  }

  // -----------------------------------------------------------------------------------------------

  private final KijiRowData mRow;
  private final String mFamily;
  private final MapFamilyQualifierIterator mQualifierIterator;
  private final int mVersionPageSize;
  private String mQualifier = null;
  private ColumnVersionIterator<T> mVersionIterator = null;
  private Entry<T> mNext;

  /**
   * Initializes a new iterator over the cells from a map-type family.
   *
   * <p>
   *   This iterator combines a {@link MapFamilyQualifierIterator} and many
   *   {@link ColumnVersionIterator}s, both of which rely on different pagers.
   *   The page size for the first one is controlled by the parameter {@code qualifierPageSize},
   *   which determines how many qualifiers to fetch at a time; the page size for the second one
   *   is controlled by the parameter {@code versionPageSize} which determines how many cells to
   *   retrieve at a time from a given qualifier.
   * </p>
   *
   * @param row Kiji row data with paging enabled on the specified map-type family.
   * @param family Map-type family name.
   * @param qualifierPageSize Size of the underlying pages of qualifiers.
   * @param versionPageSize Size of the underlying pages of versions.
   * @throws IOException on I/O error.
   */
  public MapFamilyVersionIterator(
      KijiRowData row,
      String family,
      int qualifierPageSize,
      int versionPageSize)
      throws IOException {

    this.mRow = row;
    this.mFamily = family;
    this.mQualifierIterator = new MapFamilyQualifierIterator(mRow, mFamily, qualifierPageSize);
    this.mVersionPageSize = versionPageSize;

    this.mNext = getNext();
  }

  /**
   * Reports the next entry from the map-type family, or null.
   *
   * <p>
   *   In effect, this pre-loads the next entry to return.
   *   This allows to determine whether the iterator is finished or has more elements.
   * </p>
   *
   * @return the next entry from the map-type family, or null.
   * @throws IOException on I/O error.
   */
  private Entry<T> getNext() throws IOException {
    while ((mVersionIterator == null) || !mVersionIterator.hasNext()) {
      if (!mQualifierIterator.hasNext()) {
        return null;
      }
      mQualifier = mQualifierIterator.next();
      if (mVersionIterator != null) {
        mVersionIterator.close();
      }
      mVersionIterator =
          new ColumnVersionIterator<T>(mRow,  mFamily,  mQualifier, mVersionPageSize);
    }
    final Map.Entry<Long, T> mapEntry = mVersionIterator.next();
    return new Entry<T>(mQualifier, mapEntry.getKey(), mapEntry.getValue());
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    return (mNext != null);
  }

  /** {@inheritDoc} */
  @Override
  public Entry<T> next() {
    final Entry<T> entry = mNext;
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
  public void close() throws IOException {
    mQualifierIterator.close();
    if (mVersionIterator != null) {
      mVersionIterator.close();
    }
  }

  /** {@inheritDoc} */
  @Override
  public Iterator<Entry<T>> iterator() {
    return this;
  }
}
