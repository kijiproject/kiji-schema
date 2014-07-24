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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.hbase.async.KeyValue;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiResult;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;

/**
 * A {@link org.kiji.schema.KijiResult} backed by an {@link java.util.ArrayList ArrayList}
 * <{@link org.hbase.async.KeyValue}>.
 *
 * @param <T> The type of {@code KijiCell} values in the view.
 */
@ApiAudience.Private
public final class AsyncMaterializedKijiResult<T> implements KijiResult<T> {
  private final EntityId mEntityId;
  private final KijiDataRequest mDataRequest;
  private final KijiTableLayout mLayout;
  private final HBaseColumnNameTranslator mColumnTranslator;
  private final CellDecoderProvider mDecoderProvider;
  private final SortedMap<KijiColumnName, List<KeyValue>> mColumnResults;

  /**
   * Create a {@code KijiResult} backed by an ArrayList<{@link org.hbase.async.KeyValue}>.
   *
   * @param entityId The entity ID of the row to which the {@code Result} belongs.
   * @param dataRequest The data request which defines the columns in this {@code KijiResult}.
   * @param layout The Kiji table layout of the table.
   * @param columnTranslator The Kiji column name translator of the table.
   * @param decoderProvider The Kiji cell decoder provider of the table.
   * @param columnResults The materialized HBase results.
   */
  private AsyncMaterializedKijiResult(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final KijiTableLayout layout,
      final HBaseColumnNameTranslator columnTranslator,
      final CellDecoderProvider decoderProvider,
      final SortedMap<KijiColumnName, List<KeyValue>> columnResults
  ) {
    mEntityId = entityId;
    mDataRequest = dataRequest;
    mLayout = layout;
    mColumnTranslator = columnTranslator;
    mDecoderProvider = decoderProvider;
    mColumnResults = columnResults;
  }

  /**
   * Create a {@code KijiResult} backed by an ArrayList<{@link org.hbase.async.KeyValue}>.
   *
   * @param entityId The entity ID of the row to which the results belongs.
   * @param dataRequest The data request which defines the columns in this {@code KijiResult}.
   * @param result The ArrayList of KeyValues that represents the result.
   * @param layout The Kiji table layout of the table.
   * @param columnTranslator The Kiji column name translator of the table.
   * @param decoderProvider The Kiji cell decoder provider of the table.
   * @param <T> The type of {@code KijiCell} values in the view.
   * @return A {@code KijiResult} backed by an HBase {@code Result}.
   */
  public static <T> AsyncMaterializedKijiResult<T> create(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final List<KeyValue> result,
      final KijiTableLayout layout,
      final HBaseColumnNameTranslator columnTranslator,
      final CellDecoderProvider decoderProvider
  ) {
    final ImmutableSortedMap.Builder<KijiColumnName, List<KeyValue>> columnResults =
        ImmutableSortedMap.naturalOrder();

    for (Column columnRequest : dataRequest.getColumns()) {
      // TODO: determine via benchmarks whether it would be faster to make a copy of the
      // columnResult list so that the underlying Result may be garbage collected.
      List<KeyValue> columnResult = getColumnKeyValues(columnRequest, columnTranslator, result);
      columnResults.put(columnRequest.getColumnName(), columnResult);
    }

    return new AsyncMaterializedKijiResult<T>(
        entityId,
        dataRequest,
        layout,
        columnTranslator,
        decoderProvider,
        columnResults.build());
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
    final List<Iterator<KijiCell<T>>> columnIterators =
        Lists.newArrayListWithCapacity(mColumnResults.size());

    for (Map.Entry<KijiColumnName, List<KeyValue>> entry : mColumnResults.entrySet()) {
      final Function<KeyValue, KijiCell<T>> decoder =
          ResultDecoders.getDecoderFunction(
              entry.getKey(),
              mLayout,
              mColumnTranslator,
              mDecoderProvider);

      columnIterators.add(Iterators.transform(entry.getValue().iterator(), decoder));
    }
    return Iterators.concat(columnIterators.iterator());
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public <U extends T> AsyncMaterializedKijiResult<U> narrowView(final KijiColumnName column) {
    final KijiDataRequest narrowRequest = AsyncKijiResult.narrowRequest(column, mDataRequest);
    if (narrowRequest.equals(mDataRequest)) {
      return (AsyncMaterializedKijiResult<U>) this;
    }

    final ImmutableSortedMap.Builder<KijiColumnName, List<KeyValue>> narrowedResults =
        ImmutableSortedMap.naturalOrder();

    for (Column columnRequest : narrowRequest.getColumns()) {
      final KijiColumnName requestColumnName = columnRequest.getColumnName();

      final List<KeyValue> exactColumn = mColumnResults.get(requestColumnName);
      if (exactColumn != null) {
        narrowedResults.put(requestColumnName, exactColumn);
      } else {
        // The column request is fully qualified, and the original view contains a request for the
        // column's entire family.
        final List<KeyValue> familyResults =
            mColumnResults.get(KijiColumnName.create(requestColumnName.getFamily(), null));
        final List<KeyValue> qualifiedColumnResults =
            getQualifiedColumnKeyValues(columnRequest, mColumnTranslator, familyResults);

        narrowedResults.put(requestColumnName, qualifiedColumnResults);
      }
    }

    return new AsyncMaterializedKijiResult<U>(
        mEntityId,
        mDataRequest,
        mLayout,
        mColumnTranslator,
        mDecoderProvider,
        narrowedResults.build());
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    // No-op
  }

  // -----------------------------------------------------------------------------------------------
  // Static classes and helper methods
  // -----------------------------------------------------------------------------------------------

  private static final byte[] EMPTY_BYTES = new byte[0];

  /**
   * A custom comparator for KeyValues. This Comparator is identical to AsyncHBase's KeyValue's
   * natural order with the exception of comparing timestamps. Here we invert the calculation of
   * comparing timestamps so that newer timestamps are first (similar to how HBase's
   * {@link org.apache.hadoop.hbase.KeyValue} orders timestamps).
   */
  private static final Comparator<KeyValue> CustomKeyValueComparator = new Comparator<KeyValue>() {
    @Override
    public int compare(final KeyValue left, final KeyValue right) {
      int d;
      if ((d = org.hbase.async.Bytes.memcmp(left.key(), right.key())) != 0) {
        return d;
      } else if ((d = org.hbase.async.Bytes.memcmp(left.family(), right.family())) != 0) {
        return d;
      } else if ((d = org.hbase.async.Bytes.memcmp(left.qualifier(), right.qualifier())) != 0) {
        return d;
      } else if ((d = Long.signum(right.timestamp() - left.timestamp())) != 0) {//!Important change
        return d;
      } else {
        d = org.hbase.async.Bytes.memcmp(left.value(), right.value());
      }
      return d;
    }
  };

  /**
   * Get the list of {@code KeyValue}s belonging to a column request.
   *
   * <p>
   *   This method will filter extra version from the result if necessary.
   * </p>
   *
   * @param columnRequest of the column whose {@code KeyValues} to return.
   * @param translator for the table.
   * @param result the scan results.
   * @return the {@code KeyValue}s for the column.
   */
  private static List<KeyValue> getColumnKeyValues(
      final Column columnRequest,
      final HBaseColumnNameTranslator translator,
      final List<KeyValue> result
  ) {
    final KijiColumnName column = columnRequest.getColumnName();
    final List<KeyValue> keyValues = Lists.newArrayList(result);

    if (column.isFullyQualified()) {
      return getQualifiedColumnKeyValues(columnRequest, translator, keyValues);
    } else {
      return getFamilyKeyValues(columnRequest, translator, keyValues);
    }
  }

  /**
   * Get the list of {@code KeyValue}s belonging to a fully-qualified column
   * request.
   *
   * <p>
   *   This method will filter extra versions from the result if the number of versions in the
   *   result is greater than the column's requested max versions.
   * </p>
   *
   * @param columnRequest of the column whose {@code KeyValues} to return.
   * @param translator for the table.
   * @param result the scan results.
   * @return the {@code KeyValue}s for the qualified column.
   */
  private static List<KeyValue> getQualifiedColumnKeyValues(
      final Column columnRequest,
      final HBaseColumnNameTranslator translator,
      final List<KeyValue> result
  ) {
    if (result.size() == 0) {
      return ImmutableList.of();
    }
    final byte[] rowkey = result.get(0).key();
    final byte[] family;
    final byte[] qualifier;
    try {
      final HBaseColumnName hbaseColumn =
          translator.toHBaseColumnName(columnRequest.getColumnName());
      family = hbaseColumn.getFamily();
      qualifier = hbaseColumn.getQualifier();
    } catch (NoSuchColumnException e) {
      throw new IllegalArgumentException(e);
    }

    final KeyValue start = new KeyValue(
        rowkey,
        family,
        qualifier,
        Long.MAX_VALUE, EMPTY_BYTES);
    // Add an 0 byte to get the next possible row.
    final KeyValue end = new KeyValue(
        rowkey,
        family,
        Arrays.copyOf(qualifier, qualifier.length + 1),
        Long.MAX_VALUE, EMPTY_BYTES);

    final List<KeyValue> columnKeyValues = getSublist(result, CustomKeyValueComparator, start, end);

    final int maxVersions = columnRequest.getMaxVersions();
    if (columnKeyValues.size() > maxVersions) {
      return columnKeyValues.subList(0, maxVersions);
    } else {
      return columnKeyValues;
    }
  }

  /**
   * Get the list of {@code KeyValue}s belonging to a familyRequest request.
   *
   * <p>
   *   This method will filter extra versions from each column if necessary.
   * </p>
   *
   * @param familyRequest The familyRequest whose {@code KeyValues} to return.
   * @param translator The column name translator for the table.
   * @param result The scan results.
   * @return the {@code KeyValue}s for the specified familyRequest.
   */
  private static List<KeyValue> getFamilyKeyValues(
      final Column familyRequest,
      final HBaseColumnNameTranslator translator,
      final List<KeyValue> result
  ) {
    final KijiColumnName column = familyRequest.getColumnName();
    if (result.size() == 0) {
      return ImmutableList.of();
    }
    final byte[] rowkey = result.get(0).key();
    final byte[] family;
    final byte[] qualifier;
    try {
      final HBaseColumnName hbaseColumn =
          translator.toHBaseColumnName(column);
      family = hbaseColumn.getFamily();
      qualifier = hbaseColumn.getQualifier();
    } catch (NoSuchColumnException e) {
      throw new IllegalArgumentException(e);
    }

    List<KeyValue> keyValues = Lists.newArrayList();

    final KeyValue familyStartKV =
        new KeyValue(rowkey, family, qualifier, Integer.MAX_VALUE, EMPTY_BYTES);

    // Index of the current qualified column index
    int columnStart = getElementIndex(result, 0, familyStartKV);
    while (columnStart < result.size()
        && column.getFamily().equals(getKeyValueColumnName(result.get(columnStart), translator)
            .getFamily())) {

      final KeyValue start = result.get(columnStart);
      final KeyValue end = new KeyValue(
          rowkey,
          start.family(),
          Arrays.copyOf(start.qualifier(), start.qualifier().length + 1),
          Long.MAX_VALUE,
          EMPTY_BYTES);


      final int columnEnd = getElementIndex(result, columnStart, end);

      final int length = Math.min(columnEnd - columnStart, familyRequest.getMaxVersions());

      keyValues.addAll(result.subList(columnStart, columnStart + length));
      columnStart = columnEnd;
    }

    return keyValues;
  }

  /**
   * Get the KijiColumnName encoded in the Key of a given KeyValue.
   *
   * @param kv KeyValue from which to get the encoded KijiColumnName.
   * @param translator for table.
   * @return the KijiColumnName encoded in the Key of a given KeyValue.
   */
  private static KijiColumnName getKeyValueColumnName(
      final KeyValue kv,
      final HBaseColumnNameTranslator translator
  ) {
    final HBaseColumnName hBaseColumnName = new HBaseColumnName(kv.family(), kv.qualifier());
    try {
      return translator.toKijiColumnName(hBaseColumnName);
    } catch (NoSuchColumnException nsce) {
      // This should not happen since it's only called on data returned by HBase.
      throw new IllegalStateException(
          String.format("Unknown column name in KeyValue: %s.", kv));
    }
  }

  /**
   * Get the index of a {@code KeyValue} in an array.
   *
   * @param result to search for the {@code KeyValue}. Must be sorted.
   * @param start index to start search for the {@code KeyValue}.
   * @param element to search for.
   * @return the index that the element resides, or would reside if it were to be inserted into the
   *     sorted array.
   */
  private static int getElementIndex(
      final List<KeyValue> result,
      final int start,
      final KeyValue element
  ) {
    final List<KeyValue> sublist = result.subList(start, result.size());
    final int index = Collections.binarySearch(sublist, element, CustomKeyValueComparator);
    if (index < 0) {
      return (-1 - index) + start;
    } else {
      return index + start;
    }
  }

  /**
   * Get a sublist of a list starting at the {@code start} element (inclusive), and extending to the
   * {@code end} element (exclusive).
   *
   * @param list from which to take the sublist. Must be sorted.
   * @param comparator which the list is sorted with.
   * @param start element for the sublist (inclusive).
   * @param end element for the sublist (exclusive).
   * @param <T> The type of list element.
   * @return the sublist from the provided sorted list which contains the {@code start} element and
   *    excludes the {@code end} element.
   */
  private static <T> List<T> getSublist(
      final List<T> list,
      final Comparator<? super T> comparator,
      final T start,
      final T end
  ) {
    int startIndex = Collections.binarySearch(list, start, comparator);
    if (startIndex < 0) {
      startIndex = -1 - startIndex;
    }
    int endIndex = Collections.binarySearch(list, end, comparator);
    if (endIndex < 0) {
      endIndex = -1 - endIndex;
    }

    return list.subList(startIndex, endIndex);
  }
}
