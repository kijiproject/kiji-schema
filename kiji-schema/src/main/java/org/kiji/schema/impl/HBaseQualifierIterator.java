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
package org.kiji.schema.impl;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.collect.Iterators;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiColumnPagingNotEnabledException;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.util.CloseableIterable;

/**
 * Iterates over all the qualifiers in a map-type family.
 *
 * <p> Uses paging under the hood to page through the qualifiers. </p>
 */
@ApiAudience.Private
public final class HBaseQualifierIterator implements CloseableIterable<String>, Iterator<String> {
  /** Underlying qualifier pager. */
  private final HBaseQualifierPager mPager;

  /** Iterates over the current page of qualifiers. Never null, but potentially empty. */
  private Iterator<String> mPageIterator;

  /**
   * Constructs a new iterator over the qualifiers of a map-type family.
   *
   * @param entityId ID of the row to page through.
   * @param dataRequest Data request for the map-type family to page through.
   * @param table HBase kiji table containing the row.
   * @param family Family column name.
   * @throws KijiColumnPagingNotEnabledException if paging is not configured for the family.
   */
  public HBaseQualifierIterator(
      EntityId entityId,
      KijiDataRequest dataRequest,
      HBaseKijiTable table,
      KijiColumnName family)
      throws KijiColumnPagingNotEnabledException {
    mPager = new HBaseQualifierPager(entityId, dataRequest, table, family);

    mPageIterator = Iterators.emptyIterator();
    while (!mPageIterator.hasNext() && mPager.hasNext()) {
      mPageIterator = Iterators.forArray(mPager.next());
    }
  }

  /** {@inheritDoc} */
  @Override
  public Iterator<String> iterator() {
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    // TODO: Make sure the iterator is closed explicitly.
    mPager.close();
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    return mPageIterator.hasNext();
  }

  /** {@inheritDoc} */
  @Override
  public String next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final String qualifier = mPageIterator.next();

    while (!mPageIterator.hasNext() && mPager.hasNext()) {
      mPageIterator = Iterators.forArray(mPager.next());
    }

    return qualifier;
  }

  /** {@inheritDoc} */
  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove() is not supported.");
  }
}
