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
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.base.Objects;
import org.apache.hadoop.hbase.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiResultIterator;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.layout.KijiColumnNameTranslator;

/**
 * Wraps a {@link org.apache.hadoop.hbase.client.Result} to provide iteration and decoding.
 *
 * @param <T> type of the values returned by this iterator.
 */
@ApiAudience.Private
@ApiStability.Experimental
public final class HBaseNonPagedVersionIterator<T> implements KijiResultIterator<T> {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseNonPagedVersionIterator.class);

  private final Iterator<KeyValue> mResultIterator;
  private final KijiColumnNameTranslator mColumnNameTranslator;
  private final KijiCellDecoder<T> mCellDecoder;
  private final EntityId mEntityId;

  /**
   * This cell will be returned and then mutated by calls to {@link #next()}. This will only be null
   * before the constructor has completed and when the iterator contains no more cells.
   */
  private KijiCell<T> mNextCell;

  /**
   * Initialize a new HBaseNonPagedVersionIterator.
   *
   * @param columnNameTranslator ColumnNameTranslator with which to translate KeyValues.
   * @param cellDecoder Decoder with which to decoder KeyValues into KijiCells.
   * @param entityId EntityId of the row from which the cells were retrieved.
   * @param result HBaseResultIterator containing cells from the table.
   */
  public HBaseNonPagedVersionIterator(
      final KijiColumnNameTranslator columnNameTranslator,
      final KijiCellDecoder<T> cellDecoder,
      final EntityId entityId,
      final Iterator<KeyValue> result
  ) {
    mColumnNameTranslator = columnNameTranslator;
    mCellDecoder = cellDecoder;
    mEntityId = entityId;
    mResultIterator = result;
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
   * Get the KijiCell represented by the next KeyValue.
   *
   * @return the KijiCell represented by the next KeyValue.
   */
  @SuppressWarnings("unchecked")
  private KijiCell<T> getNextCell() {
    if (mResultIterator.hasNext()) {
      final KeyValue kv = mResultIterator.next();
      final KijiColumnName kijiColumn = fromKeyValue(kv);
      try {
        return KijiCell.create(
            kijiColumn,
            kv.getTimestamp(),
            mCellDecoder.decodeCell(kv.getValue()));
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
    throw new UnsupportedOperationException(
        "HBaseNonPagedVersionIterator.remove() is not supported.");
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(HBaseNonPagedVersionIterator.class)
        .add("hbase-result-iterator", mResultIterator)
        .add("entity-id", mEntityId)
        .toString();
  }
}
