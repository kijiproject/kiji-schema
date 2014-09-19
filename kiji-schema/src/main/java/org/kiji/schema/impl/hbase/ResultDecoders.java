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

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Function;
import org.apache.hadoop.hbase.KeyValue;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;

/**
 * Provides decoding functions for Kiji columns.
 */
@ApiAudience.Private
@ThreadSafe
public final class ResultDecoders {
  /**
   * Get a decoder function for a column.
   *
   * @param column to decode.
   * @param layout of table.
   * @param translator for table.
   * @param decoderProvider for table.
   * @param <T> type of values in the column.
   * @return a decode for the column.
   */
  public static <T> Function<KeyValue, KijiCell<T>> getDecoderFunction(
      final KijiColumnName column,
      final KijiTableLayout layout,
      final HBaseColumnNameTranslator translator,
      final CellDecoderProvider decoderProvider
  ) {
    if (column.isFullyQualified()) {
      final KijiCellDecoder<T> decoder = decoderProvider.getDecoder(column);

      return new QualifiedColumnDecoder<T>(column, decoder);
    }

    final FamilyLayout family = layout.getFamilyMap().get(column.getFamily());

    if (family.isMapType()) {
      return new MapFamilyDecoder<T>(translator, decoderProvider.<T>getDecoder(column));
    } else {
      return new GroupFamilyDecoder<T>(translator, decoderProvider);
    }
  }

  /**
   * A function which will decode {@link KeyValue}s from a map-type column.
   *
   * <p>
   *   This function may apply optimizations that make it only suitable to decode {@code KeyValue}s
   *   from the specified map-type family, so do not use it over {@code KeyValue}s from another
   *   family.
   * </p>
   */
  @NotThreadSafe
  private static final class MapFamilyDecoder<T> implements Function<KeyValue, KijiCell<T>> {
    private final KijiCellDecoder<T> mCellDecoder;
    private final HBaseColumnNameTranslator mColumnTranslator;

    private KijiColumnName mLastColumn = null;
    private byte[] mLastQualifier = null;

    /**
     * Create a map-family column decoder.
     *
     * @param columnTranslator for the table.
     * @param decoder for the table.
     */
    public MapFamilyDecoder(
        final HBaseColumnNameTranslator columnTranslator,
        final KijiCellDecoder<T> decoder
    ) {
      mColumnTranslator = columnTranslator;
      mCellDecoder = decoder;
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     *   We cache the previously-used {@code KijiColumnName}. This saves parsing and allocations of
     *   the column name for the common case of iterating through multiple versions of each column
     *   in the family.
     * </p>
     *
     * @param keyValue to decode.
     * @return the decoded KijiCell.
     */
    @Override
    public KijiCell<T> apply(final KeyValue keyValue) {
      if (!Arrays.equals(mLastQualifier, keyValue.getQualifier())) {
        mLastQualifier = keyValue.getQualifier();
        try {
          mLastColumn =
              mColumnTranslator.toKijiColumnName(
                  new HBaseColumnName(keyValue.getFamily(), keyValue.getQualifier()));
        } catch (NoSuchColumnException e) {
          mLastQualifier = null;
          mLastColumn = null;
          throw new InternalKijiError(e);
        }
      }

      try {
        final DecodedCell<T> decodedCell = mCellDecoder.decodeCell(keyValue.getValue());
        return KijiCell.create(mLastColumn, keyValue.getTimestamp(), decodedCell);
      } catch (IOException e) {
        throw new KijiIOException(e);
      }
    }
  }

  /**
   * A function which will decode {@link KeyValue}s from a group-type family.
   *
   * <p>
   *   This function may use optimizations that make it only suitable to decode {@code KeyValue}s
   *   from the specified group-type family, so do not use it over {@code KeyValue}s from another
   *   family.
   * </p>
   */
  @NotThreadSafe
  private static final class GroupFamilyDecoder<T> implements Function<KeyValue, KijiCell<T>> {
    private final CellDecoderProvider mDecoderProvider;
    private final HBaseColumnNameTranslator mColumnTranslator;

    private KijiCellDecoder<T> mLastDecoder;
    private KijiColumnName mLastColumn;
    private byte[] mLastQualifier;

    /**
     * Create a qualified column decoder for the provided column.
     *
     * @param columnTranslator for the table.
     * @param decoderProvider for the table.
     */
    public GroupFamilyDecoder(
        final HBaseColumnNameTranslator columnTranslator,
        final CellDecoderProvider decoderProvider
    ) {
      mDecoderProvider = decoderProvider;
      mColumnTranslator = columnTranslator;
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     *   We cache the previously-used {@code KijiCellDecoder} and {@code KijiColumnName}. This saves
     *   lookups (of the decoder) and allocations (of the column name) for the common case of
     *   iterating through the versions of a column in the family.
     * </p>
     *
     * TODO: We know that all of the KijiCell's decoded from this function always have the same
     * Kiji family, so we should not decode it.  Currently the HBaseColumnNameTranslator does not
     * support this.
     *
     * @param keyValue to decode.
     * @return the decoded KijiCell.
     */
    @Override
    public KijiCell<T> apply(final KeyValue keyValue) {
      if (!Arrays.equals(mLastQualifier, keyValue.getQualifier())) {
        try {
          mLastQualifier = keyValue.getQualifier();
          mLastColumn =
              mColumnTranslator.toKijiColumnName(
                  new HBaseColumnName(keyValue.getFamily(), keyValue.getQualifier()));

          mLastDecoder = mDecoderProvider.getDecoder(mLastColumn);
        } catch (NoSuchColumnException e) {
          // TODO(SCHEMA-962): Critical! Handle this. Will happen when reading a deleted column
          mLastDecoder = null;
          mLastColumn = null;
          mLastQualifier = null;
          throw new IllegalArgumentException(e);
        }
      }

      try {
        final DecodedCell<T> decodedCell = mLastDecoder.decodeCell(keyValue.getValue());
        return KijiCell.create(mLastColumn, keyValue.getTimestamp(), decodedCell);
      } catch (IOException e) {
        throw new KijiIOException(e);
      }
    }
  }

  /**
   * A function which will decode {@link KeyValue}s from a qualified column.
   *
   * <p>
   *   The column may be from either a map-type or group-type family.
   * </p>
   *
   * <p>
   *   This function may apply optimizations that make it only suitable to decode {@code KeyValue}s
   *   from the specified column, so do not use it over {@code KeyValue}s from another column.
   * </p>
   *
   * @param <T> type of value in the column.
   */
  @Immutable
  private static final class QualifiedColumnDecoder<T> implements Function<KeyValue, KijiCell<T>> {
    private final KijiCellDecoder<T> mCellDecoder;
    private final KijiColumnName mColumnName;

    /**
     * Create a qualified column decoder for the provided column.
     *
     * @param columnName of the column.
     * @param cellDecoder for the table.
     */
    public QualifiedColumnDecoder(
        final KijiColumnName columnName,
        final KijiCellDecoder<T> cellDecoder
    ) {
      mCellDecoder = cellDecoder;
      mColumnName = columnName;
    }

    /** {@inheritDoc} */
    @Override
    public KijiCell<T> apply(final KeyValue keyValue) {
      try {
        final DecodedCell<T> decodedCell = mCellDecoder.decodeCell(keyValue.getValue());
        return KijiCell.create(mColumnName, keyValue.getTimestamp(), decodedCell);
      } catch (IOException e) {
        throw new KijiIOException(e);
      }
    }
  }

  /** private constructor for utility class. */
  private ResultDecoders() {
  }
}
