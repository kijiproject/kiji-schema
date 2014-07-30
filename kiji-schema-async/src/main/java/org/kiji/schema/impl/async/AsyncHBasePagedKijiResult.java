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

package org.kiji.schema.impl.async;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator;
import org.hbase.async.ColumnPrefixFilter;
import org.hbase.async.FilterList;
import org.hbase.async.FilterList.Operator;
import org.hbase.async.KeyValue;
import org.hbase.async.ScanFilter;
import org.hbase.async.Scanner;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiResult;
import org.kiji.schema.filter.KijiColumnFilter;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.impl.hbase.HBaseDataRequestAdapter.NameTranslatingFilterContext;
import org.kiji.schema.impl.hbase.HBaseKijiResult;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.util.DebugResourceTracker;

/**
 * A {@link org.kiji.schema.KijiResult} backed by on-demand AsyncHBase scans.
 *
 * <p>
 *   {@code AsyncPagedKiijResult} is <em>not</em> thread safe.
 * </p>
 *
 * @param <T> The type of {@code KijiCell} values in the view.
 */
@ApiAudience.Private
public class AsyncHBasePagedKijiResult<T> implements KijiResult<T> {
  private final EntityId mEntityId;
  private final KijiDataRequest mDataRequest;
  private final AsyncHBaseKijiTable mTable;
  private final KijiTableLayout mLayout;
  private final HBaseColumnNameTranslator mColumnTranslator;
  private final CellDecoderProvider mDecoderProvider;
  private final SortedMap<KijiColumnName, Iterable<KijiCell<T>>> mColumnResults;
  private final Closer mCloser;

  /**
   * This result does not need to be closed unless {@link #iterator()} is called, so we defer
   * registering with the debug resource tracker till that point. This variable keeps track of
   * whether we have registered yet. Does not need to be atomic, because this class is not thread
   * safe.
   */
  private boolean mDebugRegistered = false;

  /**
   * Create a new {@link AsyncHBasePagedKijiResult}.
   *
   * @param entityId EntityId of the row from which to read cells.
   * @param dataRequest KijiDataRequest defining the values to retrieve.
   * @param table The table being viewed.
   * @param layout The layout of the table.
   * @param columnTranslator A column name translator for the table.
   * @param decoderProvider A cell decoder provider for the table.
   */
  public AsyncHBasePagedKijiResult(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final AsyncHBaseKijiTable table,
      final KijiTableLayout layout,
      final HBaseColumnNameTranslator columnTranslator,
      final CellDecoderProvider decoderProvider
  ) {
    mEntityId = entityId;
    mDataRequest = dataRequest;
    mLayout = layout;
    mColumnTranslator = columnTranslator;
    mDecoderProvider = decoderProvider;
    mTable = table;
    mCloser = Closer.create();

    final ImmutableSortedMap.Builder<KijiColumnName, Iterable<KijiCell<T>>> columnResults =
        ImmutableSortedMap.naturalOrder();

    for (Column columnRequest : mDataRequest.getColumns()) {
      final PagedColumnIterable columnIterable = new PagedColumnIterable(columnRequest);
      mCloser.register(columnIterable);
      columnResults.put(columnRequest.getColumnName(), columnIterable);
    }

    mColumnResults = columnResults.build();
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mEntityId;
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    return mDataRequest;
  }

  /** {@inheritDoc} */
  @Override
  public Iterator<KijiCell<T>> iterator() {
    if (!mDebugRegistered) {
      DebugResourceTracker.get().registerResource(this);
      mDebugRegistered = true;
    }
    return Iterables.concat(mColumnResults.values()).iterator();
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public <U extends T> AsyncHBasePagedKijiResult<U> narrowView(final KijiColumnName column) {
    final KijiDataRequest narrowRequest = HBaseKijiResult.narrowRequest(column, mDataRequest);

    return new AsyncHBasePagedKijiResult<U>(
        mEntityId,
        narrowRequest,
        mTable,
        mLayout,
        mColumnTranslator,
        mDecoderProvider);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    try {
      mCloser.close();
    } finally {
      if (mDebugRegistered) {
        DebugResourceTracker.get().unregisterResource(this);
        mDebugRegistered = false;
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Helper classes and methods
  // -----------------------------------------------------------------------------------------------

  /**
   * An iterable which starts an HBase scan for each requested iterator.
   */
  public final class PagedColumnIterable implements Iterable<KijiCell<T>>, Closeable {
    private final KijiColumnName mColumn;
    private final Column mColumnRequest;
    private Scanner mScanner;
    private final byte[] mTableName;

    /**
     * Creates an iterable which starts an HBase scan for each requested iterator.
     *
     * @param columnRequest of column to scan.
     */
    private PagedColumnIterable(final Column columnRequest) {
      mColumn = columnRequest.getColumnName();
      mColumnRequest = columnRequest;

      mTableName = KijiManagedHBaseTableName
          .getKijiTableName(mTable.getURI().getInstance(), mTable.getURI().getTable()).toBytes();
    }

    /**
     * Closes a {@code Scanner} if it is not null.
     * @param scanner The scanner to be closed.
     */
    private void closeScanner(final Scanner scanner) {
      if (null != scanner) {
        scanner.clearFilter();
      }
    }

    /**
     * Initializes and sets up the scanner.
     */
    private void setUpScanner() {
      try {
        final KijiColumnFilter.Context filterContext =
            new NameTranslatingFilterContext(mColumnTranslator);
        final KijiColumnFilter kijiFilter = mColumnRequest.getFilter();
        final ScanFilter filter;
        final Map<byte [], NavigableSet<byte []>> familyMap =
            new TreeMap<byte [], NavigableSet<byte []>>(new ByteArrayComparator());

        if (kijiFilter != null) {
          filter = AsyncHBaseDataRequestAdapter.toScanFilter(kijiFilter, mColumn, filterContext);
        } else {
          filter = null;
        }

        final byte[] rowkey = mEntityId.getHBaseRowKey();
        closeScanner(mScanner);
        mScanner = mTable.getHBClient().newScanner(mTableName);
        mScanner.setStartKey(rowkey);
        mScanner.setStopKey(Arrays.copyOf(rowkey, rowkey.length + 1));

        final HBaseColumnName hbaseColumn = mColumnTranslator.toHBaseColumnName(mColumn);

        if (mColumn.isFullyQualified()) {
          AsyncHBaseDataRequestAdapter.addColumnOrFamily(
              familyMap,
              hbaseColumn.getFamily(),
              hbaseColumn.getQualifier());
          mScanner.setFilter(filter);
        } else {
          if (Arrays.equals(hbaseColumn.getQualifier(), new byte[0])) {
            // This can happen with the native translator
            AsyncHBaseDataRequestAdapter.addColumnOrFamily(familyMap, hbaseColumn.getFamily(), null);
            mScanner.setFilter(filter);
          } else if (hbaseColumn.getQualifier().length == 0) {
            AsyncHBaseDataRequestAdapter.addColumnOrFamily(familyMap, hbaseColumn.getFamily(), null);
            mScanner.setFilter(filter);
          }
          AsyncHBaseDataRequestAdapter.addColumnOrFamily(familyMap, hbaseColumn.getFamily(), null);

          final ScanFilter prefixFilter = new ColumnPrefixFilter(hbaseColumn.getQualifier());
          if (filter != null) {
            final List<ScanFilter> filterList = Lists.newArrayList();
            filterList.add(prefixFilter);
            filterList.add(filter);
            final FilterList filters = new FilterList(filterList, Operator.MUST_PASS_ALL);
            mScanner.setFilter(filters);
          } else {
            mScanner.setFilter(prefixFilter);
          }
        }

        AsyncHBaseDataRequestAdapter.setFamiliesForScanner(mScanner, familyMap);
        mScanner.setMaxVersions(mColumnRequest.getMaxVersions());
        mScanner.setTimeRange(mDataRequest.getMinTimestamp(), mDataRequest.getMaxTimestamp());
        mScanner.setMaxNumKeyValues(mColumnRequest.getPageSize());
      } catch (IOException e) {
        throw new KijiIOException(e);
      }
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<KijiCell<T>> iterator() {
      setUpScanner();
      // Decoder functions are stateful, so they should not be shared among multiple iterators
      final Function<KeyValue, KijiCell<T>> decoder =
          ResultDecoders.getDecoderFunction(mColumn, mLayout, mColumnTranslator, mDecoderProvider);

      return
          Iterators.concat(
              Iterators.transform(
                  new ScannerIterator(),
                  new Function<ArrayList<KeyValue>, Iterator<KijiCell<T>>>() {
                    @Override
                    public Iterator<KijiCell<T>> apply(final ArrayList<KeyValue> result) {
                      return Iterators.transform(result.iterator(), decoder);
                    }
                  }));
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      closeScanner(mScanner);
    }

    private class ScannerIterator implements Iterator<ArrayList<KeyValue>> {
      private ArrayList<KeyValue> mNext = null;

      public ScannerIterator() {
        next();
      }

      @Override
      public boolean hasNext() {
        return mNext != null;
      }

      @Override
      public ArrayList<KeyValue> next() {
        final ArrayList<KeyValue> retVal = mNext;
        try {
          final ArrayList<ArrayList<KeyValue>> fullResults = mScanner.nextRows(1).join();
          if (null != fullResults && !fullResults.isEmpty()) {
            mNext = fullResults.get(0);
          } else {
            mNext = null;
          }
        } catch (Exception e) {
          if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          } else {
            throw new KijiIOException(e);
          }
        }
        return retVal;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("Removing is not supported");
      }

    }

  }

}
