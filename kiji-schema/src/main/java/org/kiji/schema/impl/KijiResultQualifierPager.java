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

package org.kiji.schema.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiPager;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * A Kiji qualifier pager backed by a {@link KijiResult}.
 */
@ApiAudience.Private
public class KijiResultQualifierPager implements KijiPager {
  private static final Logger LOG = LoggerFactory.getLogger(KijiResultQualifierPager.class);

  private final KijiResult<Object> mResult;
  private final PeekingIterator<KijiCell<Object>> mCells;
  private final Column mColumnRequest;
  private final KijiTableLayout mLayout;
  private String mLastQualifier = null;

  /**
   * Create a Kiji qualifier pager backed by a {@code KijiResult}.
   *
   * @param result The {@code KijiResult} backing this pager.
   * @param layout The {@code KijiTableLayout} of the table.
   */
  public KijiResultQualifierPager(
      final KijiResult<Object> result,
      final KijiTableLayout layout
  ) {
    mResult = result;
    mCells = Iterators.peekingIterator(mResult.iterator());
    mLayout = layout;

    final KijiDataRequest dataRequest = mResult.getDataRequest();
    final Collection<Column> columnRequests = dataRequest.getColumns();
    Preconditions.checkArgument(columnRequests.size() == 1,
        "Can not create KijiResultPager with multiple columns. Data request: %s.", dataRequest);
    mColumnRequest = columnRequests.iterator().next();
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowData next() {
    return next(mColumnRequest.getPageSize());
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowData next(final int pageSize) {
    if (!hasNext()) {
      throw new NoSuchElementException("Kiji qualifier pager is exhausted.");
    }
    final KijiColumnName column = mColumnRequest.getColumnName();
    final ColumnsDef columnDef = ColumnsDef
        .create()
        .withFilter(mColumnRequest.getFilter())
        .withPageSize(KijiDataRequest.PAGING_DISABLED)
        .withMaxVersions(mColumnRequest.getMaxVersions())
        .add(column, mColumnRequest.getReaderSpec());
    final KijiDataRequestBuilder dataRequest = KijiDataRequest.builder();
    dataRequest.addColumns(columnDef);

    final List<KijiCell<Object>> cells = Lists.newArrayListWithCapacity(pageSize);

    while (mCells.hasNext() && cells.size() < pageSize) {
      final KijiCell<Object> cell = mCells.next();
      final String qualifier = cell.getColumn().getQualifier();
      if (!qualifier.equals(mLastQualifier)) {
        cells.add(
            KijiCell.create(
                cell.getColumn(),
                cell.getTimestamp(),
                new DecodedCell<Object>(cell.getWriterSchema(), null)));
        mLastQualifier = qualifier;
      }
    }

    final KijiResult<Object> result = MaterializedKijiResult.create(
        mResult.getEntityId(),
        dataRequest.build(),
        mLayout,
        ImmutableSortedMap.<KijiColumnName, List<KijiCell<Object>>>naturalOrder()
            .put(column, cells)
            .build());
    return new KijiResultRowData(mLayout, result);
  }

  /** {@inheritDoc} */
  @Override
  public void remove() {
    throw new UnsupportedOperationException("KijiPager does not support remove.");
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mResult.close();
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    while (mCells.hasNext() && mCells.peek().getColumn().getQualifier().equals(mLastQualifier)) {
      mCells.next();
    }
    return mCells.hasNext();
  }
}
