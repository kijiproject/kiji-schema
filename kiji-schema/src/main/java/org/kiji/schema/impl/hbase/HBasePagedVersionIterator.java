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
package org.kiji.schema.impl.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.DecodedCell;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiResultIterator;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.layout.KijiColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Wraps calls to HBase to provide iteration and decoding of paged data.
 *
 * <p>
 *   Can iterate over versions of a qualified column or map-type family. Returns cells sorted
 *   lexicographically by qualifier then reverse chronologically.
 * </p>
 *
 * @param <T> type of the values returned by this iterator.
 */
public class HBasePagedVersionIterator<T> implements KijiResultIterator<T> {
  private static final Logger LOG = LoggerFactory.getLogger(HBasePagedVersionIterator.class);

  /** Internally paged iterator across the values in a single qualified column. */
  private final class PagedColumnIterator implements Iterator<KijiCell<T>> {

    // Column for only this PagedColumnIterator.
    private final KijiColumnName mInnerPagedColumn;

    private long mNextMaxTimestamp;
    private int mRemainingVersions;
    private HBaseResultIterator mCurrentResultIterator;
    private KijiCell<T> mNextCell;

    /**
     * Initialize a new PagedColumnIterator on the given column.
     *
     * @param column Kiji column across which to iterate by pages.
     */
    private PagedColumnIterator(
        final KijiColumnName column
    ) {
      Preconditions.checkArgument(column.isFullyQualified(),
          "Version pager can only operate on fully qualified columns. Found family: %s",
          column.getFamily());

      mInnerPagedColumn = column;

      mNextMaxTimestamp = mMaxTimestamp;
      mRemainingVersions = mMaxVersions;
      mNextCell = getNextCell(); // This initializes mCurrentResultIterator
    }

    /**
     * Get the next page of KeyValues for this column.
     *
     * <p>
     *   Returns an empty {@link org.apache.hadoop.hbase.client.Result} If no further cells are
     *   required by the data request which requested this column.
     * </p>
     *
     * @return the next page of KeyValues for this column.
     * @throws IOException in case of an error getting data from HBase.
     */
    private Result getNextPage() throws IOException {
      if (mNextMaxTimestamp < mMinTimestamp || mRemainingVersions == 0) {
        return new Result();
      } else {
        final KijiDataRequest request = KijiDataRequest.builder()
            .withTimeRange(mMinTimestamp, mNextMaxTimestamp)
            .addColumns(ColumnsDef.create().withMaxVersions(mPageSize).add(mInnerPagedColumn))
            .build();
        final HBaseDataRequestAdapter adapter =
            new HBaseDataRequestAdapter(request, mColumnNameTranslator);
        final Get get = adapter.toGet(mEntityId, mLayout);
        final HTableInterface hTable = mTable.openHTableConnection();
        try {
          LOG.debug("Fetching new page from HBase with Get: {}", get);
          final Result result = hTable.get(get);
          if (result.size() > mRemainingVersions) {
            final int remainingVersions = mRemainingVersions;
            mRemainingVersions = 0;
            return new Result(Arrays.copyOf(result.raw(), remainingVersions));
          } else if (result.size() == 0) {
            mRemainingVersions = 0;
            return result;
          } else {
            mNextMaxTimestamp = result.raw()[result.size() - 1].getTimestamp();
            mRemainingVersions -= result.size();
            return result;
          }
        } finally {
          hTable.close();
        }
      }
    }

    /**
     * Get the next KeyValue from the column or null if no more KeyValues are requested or exist.
     *
     * @return the next KeyValue from the column or null if no more KeyValues are requested or
     *     exist.
     */
    private KeyValue getNextKV() {
      if (null == mCurrentResultIterator || !mCurrentResultIterator.hasNext()) {
        try {
          mCurrentResultIterator = new HBaseResultIterator(getNextPage());
        } catch (IOException ioe) {
          throw new KijiIOException(ioe);
        }
      }
      if (mCurrentResultIterator.hasNext()) {
        return mCurrentResultIterator.next();
      } else {
        return null;
      }
    }
    /**
     * Get the KijiCell represented by the next KeyValue or null if the next KeyValue is null.
     *
     * @return the KijiCell represented by the next KeyValue or null if the next KeyValue is null.
     */
    @SuppressWarnings("unchecked")
    private KijiCell<T> getNextCell() {
      final KeyValue kv = getNextKV();
      if (null != kv) {
        final KijiColumnName kijiColumn = fromKeyValue(kv);
        try {
          final DecodedCell<T> decodedCell = mCellDecoder.decodeCell(kv.getValue());
          return KijiCell.create(
              kijiColumn,
              kv.getTimestamp(),
              decodedCell);
        } catch (IOException ioe) {
          throw new KijiIOException(ioe);
        }
      } else {
        return null;
      }
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
      return (null != mNextCell);
    }

    /** {@inheritDoc} */
    @Override
    public KijiCell<T> next() {
      final KijiCell<T> nextCell = mNextCell;
      if (null == nextCell) {
        throw new NoSuchElementException();
      } else {
        mNextCell = getNextCell();
        return nextCell;
      }
    }

    /** {@inheritDoc} */
    @Override
    public void remove() {
      throw new UnsupportedOperationException("PagedColumnIterator does not support remove().");
    }
  }

  private final EntityId mEntityId;
  // Name of the single qualified column or map-type family of this pager.
  private final KijiColumnName mOuterPagedColumn;
  private final int mMaxVersions;
  private final int mPageSize;
  private final long mMinTimestamp;
  private final long mMaxTimestamp;
  private final KijiCellDecoder<T> mCellDecoder;
  private final KijiColumnNameTranslator mColumnNameTranslator;
  private final KijiTableLayout mLayout;
  private final HBaseKijiTable mTable;
  private final HBaseQualifierIterator mQualifierIterator;

  private PagedColumnIterator mColumnIterator;
  private KijiCell<T> mNextCell;

  /**
   * Initialize a new HBasePagedVersionIterator.
   *
   * @param entityId EntityId of the row from which to retrieve cells.
   * @param dataRequest KijiDataRequest defining the data to retrieve.
   * @param column qualified column or map-type family from which to retrieve cells. Must be
   *     included in dataRequest.
   * @param cellDecoder decoder with which to decoder KeyValues into KijiCells.
   * @param columnNameTranslator translator to interpret encoded Kiji column names.
   * @param layout KijiTableLayout of the table from which to retrieve cells.
   * @param table KijiTable from which to retrieve cells.
   * @param qualifierIterator Iterator of the qualifiers in the map-type family defined by 'column'.
   *     Must be null if 'column' is fully qualified; may not be null if 'column' is a family.
   */
  // CSOFF: ParameterNumberCheck
  public HBasePagedVersionIterator(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final KijiColumnName column,
      final KijiCellDecoder<T> cellDecoder,
      final KijiColumnNameTranslator columnNameTranslator,
      final KijiTableLayout layout,
      final HBaseKijiTable table,
      final HBaseQualifierIterator qualifierIterator // Optional.
  ) {
    // CSON: ParameterNumberCheck
    final KijiDataRequest.Column columnRequest = dataRequest.getRequestForColumn(column);
    Preconditions.checkNotNull(columnRequest, "No request found for column: %s in data request: %s",
        column, dataRequest);
    Preconditions.checkArgument(columnRequest.isPagingEnabled(),
        "Paging is not enabled for column: %s", columnRequest.getColumnName());

    mEntityId = entityId;
    mOuterPagedColumn = column;
    mMaxVersions = columnRequest.getMaxVersions();
    mPageSize = columnRequest.getPageSize();
    mMinTimestamp = dataRequest.getMinTimestamp();
    mMaxTimestamp = dataRequest.getMaxTimestamp();

    mCellDecoder = cellDecoder;
    mColumnNameTranslator = columnNameTranslator;
    mLayout = layout;
    mTable = table;
    mQualifierIterator = qualifierIterator;
    if (null == mQualifierIterator) {
      Preconditions.checkArgument(column.isFullyQualified(), "Qualifier iterator may only be null "
          + "if column is fully qualified. Found null iterator with column: %s", column);
      mColumnIterator = new PagedColumnIterator(mOuterPagedColumn);
    } else {
      Preconditions.checkArgument(!column.isFullyQualified(), "Qualifier iterator may only be "
          + "specified if column is a map-family. Found iterator with column: %s", column);
    }

    mNextCell = getNextCell();
  }

  /**
   * Get a KijiColumnName from the HBase column name encoded in a KeyValue.
   *
   * @param kv KeyValue from which to get the KijiColumnName.
   * @return the KijiColumnName represented in the KeyValue.
   */
  private KijiColumnName fromKeyValue(
      final KeyValue kv
  ) {
    final HBaseColumnName hbaseColumnName = new HBaseColumnName(kv.getFamily(), kv.getQualifier());
    try {
      return mColumnNameTranslator.toKijiColumnName(hbaseColumnName);
    } catch (NoSuchColumnException nsce) {
      throw new KijiIOException(nsce);
    }
  }

  /**
   * Get the next column from the QualifierIterator.
   *
   * <p>
   *   Combines the base family of this iterator (mOuterPagedColumn.getFamily()) with the next
   *   qualifier from the qualifier iterator.
   * </p>
   *
   * @return the next column from the QualifierIterator.
   */
  private KijiColumnName getNextColumn() {
    Preconditions.checkNotNull(mQualifierIterator);
    return KijiColumnName.create(mOuterPagedColumn.getFamily(), mQualifierIterator.next());
  }

  /**
   * Get the next cell from this iterator or null if no more cells are requested or exist.
   *
   * @return the next cell from this iterator or null if no more cells are requested or exist.
   */
  private KijiCell<T> getNextCell() {
    if (null != mColumnIterator && mColumnIterator.hasNext()) {
      return mColumnIterator.next();
    } else if (null != mQualifierIterator && mQualifierIterator.hasNext()) {
      mColumnIterator = new PagedColumnIterator(getNextColumn());
      return getNextCell();
    } else if (mQualifierIterator != null) {
      try {
        mQualifierIterator.close();
      } catch (IOException ioe) {
        throw new KijiIOException(ioe);
      }
      return null;
    } else {
      return null;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    return (null != mNextCell);
  }

  /** {@inheritDoc} */
  @Override
  public KijiCell<T> next() {
    final KijiCell<T> nextCell = mNextCell;
    if (null == nextCell) {
      throw new NoSuchElementException();
    } else {
      mNextCell = getNextCell();
      return nextCell;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void remove() {
    throw new UnsupportedOperationException("HBasePagedVersionIterator does not support remove().");
  }
}
