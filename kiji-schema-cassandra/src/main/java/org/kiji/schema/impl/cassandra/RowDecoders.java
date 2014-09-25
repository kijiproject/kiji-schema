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

package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;

import org.kiji.commons.ByteUtils;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.EntityId;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KConstants;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiRowKeyComponents;
import org.kiji.schema.KijiTable;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.avro.RowKeyComponent;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.cassandra.CassandraColumnName;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.layout.CassandraColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;

/**
 * Provides decoding functions for Kiji columns.
 */
public final class RowDecoders {

  /**
   * Create a new column family result set decoder function.
   *
   * @param tableName The Cassandra table that the results are from.
   * @param column The Kiji column name of the family.
   * @param columnRequest The column request defining the request for the family.
   * @param dataRequest The data request defining the request.
   * @param layout The layout of the Kiji table.
   * @param translator A column name translator for the table.
   * @param decoderProvider A cell decoder provider for the table.
   * @param <T> Type of cell values.
   * @return A function to convert a {@link ResultSet} containing a column family to cells.
   */
  public static <T> Function<ResultSet, Iterator<KijiCell<T>>> getColumnFamilyDecoderFunction(
      final CassandraTableName tableName,
      final KijiColumnName column,
      final Column columnRequest,
      final KijiDataRequest dataRequest,
      final KijiTableLayout layout,
      final CassandraColumnNameTranslator translator,
      final CellDecoderProvider decoderProvider
  ) {
    return new Function<ResultSet, Iterator<KijiCell<T>>>() {
      /** {@inheritDoc} */
      @Override
      public Iterator<KijiCell<T>> apply(final ResultSet resultSet) {
        final int mMaxVersions = columnRequest.getMaxVersions();
        final long mMinTimestamp = dataRequest.getMinTimestamp();
        final long mMaxTimestamp = dataRequest.getMaxTimestamp();

        Iterator<Row> rows = resultSet.iterator();

        if (mMinTimestamp != 0) {
          rows = Iterators.filter(rows, new MinTimestampPredicate(mMinTimestamp));
        }
        if (mMaxTimestamp != KConstants.END_OF_TIME) {
          rows = Iterators.filter(rows, new MaxTimestampPredicate(mMaxTimestamp));
        }
        rows = Iterators.filter(rows, new MaxVersionsPredicate(mMaxVersions));

        try {
          if (layout.getFamilyMap().get(column.getFamily()).isMapType()) {
            // Map-type family
            final Function<Row, KijiCell<T>> decoder =
                new MapFamilyDecoder<>(
                    tableName,
                    translator.toCassandraColumnName(column),
                    translator,
                    decoderProvider.<T>getDecoder(column));

            return Iterators.transform(rows, decoder);
          } else {
            // Group-type family
            final Function<Row, KijiCell<T>> decoder =
                new GroupFamilyDecoder<>(
                    tableName,
                    translator.toCassandraColumnName(column),
                    translator,
                    decoderProvider);

            // Group family decoder may return nulls, so filter them out
            return Iterators.filter(Iterators.transform(rows, decoder), Predicates.notNull());
          }
        } catch (NoSuchColumnException e) {
          throw new IllegalStateException(
              String.format("Column %s does not exist in Kiji table %s.",
                  column, layout.getName()));
        }
      }
    };
  }

  /**
   * Create a new qualified column result set decoder function.
   *
   * @param column The Kiji column of the Row.
   * @param decoderProvider A cell decoder provider for the table.
   * @param <T> The value type in the column.
   * @return A decoded cell.
   */
  public static <T> Function<ResultSet, Iterator<KijiCell<T>>> getQualifiedColumnDecoderFunction(
      final KijiColumnName column,
      final CellDecoderProvider decoderProvider
  ) {
    // No min/max timestamp or max versions filter is needed, because the CQL statement for
    // qualified gets only selects the required cells.
    return new Function<ResultSet, Iterator<KijiCell<T>>>() {
      /** {@inheritDoc} */
      @Override
      public Iterator<KijiCell<T>> apply(final ResultSet resultSet) {
        final Function<Row, KijiCell<T>> decoder =
            new QualifiedColumnDecoder<>(column, decoderProvider.<T>getDecoder(column));
        return Iterators.transform(resultSet.iterator(), decoder);
      }
    };
  }

  /**
   * Get a function for decoding row keys and tokens from Cassandra rows.
   *
   * @param layout The table layout.
   * @return A function to decode row keys and tokens for the table.
   */
  public static Function<Row, TokenRowKeyComponents> getRowKeyDecoderFunction(
      final KijiTableLayout layout
  ) {
    final RowKeyFormat2 keyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();

    switch (keyFormat.getEncoding()) {
      case RAW: return new RawRowKeyDecoder(layout);
      case FORMATTED: return new FormattedRowKeyDecoder(layout);
      default:
        throw new IllegalArgumentException(
            String.format("Unknown row key encoding %s.", keyFormat.getEncoding()));
    }
  }

  /**
   * Get a function for converting {@link TokenRowKeyComponents} to {@link EntityId}s.
   *
   * @param table The Kiji table the row keys belong to.
   * @return A function for converting {@link TokenRowKeyComponents} to {@link EntityId}s.
   */
  public static Function<TokenRowKeyComponents, EntityId> getEntityIdFunction(
      final KijiTable table
  ) {
    return new RowKeyComponentsToEntityId(table);
  }

  /**
   * A function which will decode {@link Row}s from a map-type column.
   *
   * <p>
   *   This function may apply optimizations that make it only suitable to decode {@code Row}s
   *   from the specified group-type family, so do not use it over {@code Row}s from another
   *   family.
   * </p>
   */
  @NotThreadSafe
  private static final class MapFamilyDecoder<T> implements Function<Row, KijiCell<T>> {
    private final CassandraTableName mTableName;
    private final CassandraColumnName mFamilyColumn;
    private final KijiCellDecoder<T> mCellDecoder;
    private final CassandraColumnNameTranslator mColumnTranslator;

    private KijiColumnName mLastColumn = null;
    private ByteBuffer mLastQualifier = null;

    /**
     * Create a map-family column decoder.
     * @param tableName The Cassandra table name.
     * @param familyColumn The Kiji column of the Row.
     * @param columnTranslator The column translator for the table.
     * @param decoder for the table.
     */
    public MapFamilyDecoder(
        final CassandraTableName tableName,
        final CassandraColumnName familyColumn,
        final CassandraColumnNameTranslator columnTranslator,
        final KijiCellDecoder<T> decoder
    ) {
      mFamilyColumn = familyColumn;
      mTableName = tableName;
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
     * @param row to decode.
     * @return the decoded KijiCell.
     */
    @Override
    public KijiCell<T> apply(final Row row) {
      final ByteBuffer qualifier = row.getBytes(CQLUtils.QUALIFIER_COL);
      if (!qualifier.equals(mLastQualifier)) {
        mLastQualifier = qualifier;
        try {
          mLastColumn =
              mColumnTranslator.toKijiColumnName(
                  mTableName,
                  new CassandraColumnName(
                      mFamilyColumn.getFamily(),
                      ByteUtils.toBytes(row.getBytes(CQLUtils.QUALIFIER_COL))));
        } catch (NoSuchColumnException e) {
          // There should be no columns that we can't decode, so this signals a logic error
          throw new InternalKijiError(e);
        }
      }

      final long version = row.getLong(CQLUtils.VERSION_COL);

      try {
        final DecodedCell<T> decodedCell =
            mCellDecoder.decodeCell(
                ByteUtils.toBytes(row.getBytes(CQLUtils.VALUE_COL)));
        return KijiCell.create(mLastColumn, version, decodedCell);
      } catch (IOException e) {
        throw new KijiIOException(e);
      }
    }
  }

  /**
   * A function which will decode {@link Row}s from a group-type family. If a column is read which
   * has been dropped, then this function will return null.
   *
   * <p>
   *   This function may use optimizations that make it only suitable to decode {@code Row}s
   *   from the specified group-type family, so do not use it over {@code Row}s from another
   *   family.
   * </p>
   */
  @NotThreadSafe
  private static final class GroupFamilyDecoder<T> implements Function<Row, KijiCell<T>> {
    private final CassandraTableName mTableName;
    private final CellDecoderProvider mDecoderProvider;
    private final CassandraColumnNameTranslator mColumnTranslator;
    private final CassandraColumnName mFamilyColumn;

    private KijiCellDecoder<T> mLastDecoder;
    private KijiColumnName mLastColumn;
    private ByteBuffer mLastQualifier;

    /**
     * Create a qualified column decoder for the provided column.
     *
     * @param tableName The Cassandra table name.
     * @param familyColumn The Kiji column of the Row.
     * @param columnTranslator The column translator for the table.
     * @param decoderProvider A cell decoder provider for the table.
     */
    public GroupFamilyDecoder(
        final CassandraTableName tableName,
        final CassandraColumnName familyColumn,
        final CassandraColumnNameTranslator columnTranslator,
        final CellDecoderProvider decoderProvider
    ) {
      mTableName = tableName;
      mDecoderProvider = decoderProvider;
      mColumnTranslator = columnTranslator;
      mFamilyColumn = familyColumn;
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
     * Kiji family, so we should not decode it. Currently the CassandraColumnNameTranslator does not
     * support this.
     *
     * @param row The row to decode.
     * @return the decoded KijiCell.
     */
    @Override
    public KijiCell<T> apply(final Row row) {
      final ByteBuffer qualifier = row.getBytes(CQLUtils.QUALIFIER_COL);

      if (!qualifier.equals(mLastQualifier)) {
        try {
          mLastQualifier = qualifier.duplicate();
          mLastColumn =
              mColumnTranslator.toKijiColumnName(
                  mTableName,
                  new CassandraColumnName(
                      mFamilyColumn.getFamily(),
                      ByteUtils.toBytes(qualifier)));
          mLastDecoder = mDecoderProvider.getDecoder(mLastColumn);
        } catch (NoSuchColumnException e) {
          // This can happen when a column is dropped from the group-family layout
          mLastDecoder = null;
          mLastColumn = null;
          mLastQualifier = null;
          return null;
        }
      }

      final long version = row.getLong(CQLUtils.VERSION_COL);

      try {
        final DecodedCell<T> decodedCell =
            mLastDecoder.decodeCell(
                ByteUtils.toBytes(row.getBytes(CQLUtils.VALUE_COL)));
        return KijiCell.create(mLastColumn, version, decodedCell);
      } catch (IOException e) {
        throw new KijiIOException(e);
      }
    }
  }

  /**
   * A function which will decode {@link Row}s from a qualified column.
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
  private static final class QualifiedColumnDecoder<T> implements Function<Row, KijiCell<T>> {
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
    public KijiCell<T> apply(final Row row) {
      try {
        final DecodedCell<T> decodedCell =
            mCellDecoder.decodeCell(ByteUtils.toBytes(row.getBytes(CQLUtils.VALUE_COL)));
        return KijiCell.create(mColumnName, row.getLong(CQLUtils.VERSION_COL), decodedCell);
      } catch (IOException e) {
        throw new KijiIOException(e);
      }
    }
  }

  /**
   * A 2-tuple combining a Cassandra token and Kiji row key components.
   */
  @Immutable
  public static class TokenRowKeyComponents {
    private final long mToken;
    private final KijiRowKeyComponents mComponents;

    /**
     * Create a token, row key components tuple.
     *
     * @param token The token.
     * @param components The components.
     */
    public TokenRowKeyComponents(final long token, final KijiRowKeyComponents components) {
      mToken = token;
      mComponents = components;
    }

    /**
     * Get the token.
     *
     * @return The token.
     */
    public long getToken() {
      return mToken;
    }

    /**
     * Get the components.
     *
     * @return The components.
     */
    public KijiRowKeyComponents getComponents() {
      return mComponents;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hashCode(mToken, mComponents);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final TokenRowKeyComponents other = (TokenRowKeyComponents) obj;
      return Objects.equal(this.mToken, other.mToken)
          && Objects.equal(this.mComponents, other.mComponents);
    }
  }

  /**
   * A comparator for {@link TokenRowKeyComponents}.
   */
  @Immutable
  public static final class TokenRowKeyComponentsComparator
      implements Comparator<TokenRowKeyComponents> {
    private static final TokenRowKeyComponentsComparator INSTANCE =
        new TokenRowKeyComponentsComparator();

    /**
     * Get an instance of the comparator.
     *
     * @return An instance of the comparator.
     */
    public static TokenRowKeyComponentsComparator getInstance() {
      return INSTANCE;
    }

    /** Private constructor for non-instantiable class. */
    private TokenRowKeyComponentsComparator() { }

    /** {@inheritDoc} */
    @Override
    public int compare(
        final TokenRowKeyComponents a,
        final TokenRowKeyComponents b
    ) {
      final long tokenCompare = a.getToken() - b.getToken();
      if (tokenCompare != 0) {
        return (int) tokenCompare;
      } else {
        return a.getComponents().compareTo(b.getComponents());
      }
    }
  }

  /**
   * Decodes a Cassandra row containing the token a raw row key olumn into a
   * {@link TokenRowKeyComponents}.
   */
  @Immutable
  private static final class RawRowKeyDecoder implements Function<Row, TokenRowKeyComponents> {
    private final String mTokenColumn;

    /**
     * Create a row key decoder for a raw row key format table.
     *
     * @param layout The layout of the table.
     */
    private RawRowKeyDecoder(final KijiTableLayout layout) {
      mTokenColumn = CQLUtils.getTokenColumn(layout);
    }

    /** {@inheritDoc} */
    @Override
    public TokenRowKeyComponents apply(final Row row) {
      final int token = row.getInt(mTokenColumn);
      final Object[] components =
          new Object[] { ByteUtils.toBytes(row.getBytes(CQLUtils.RAW_KEY_COL)) };
      return new TokenRowKeyComponents(token, KijiRowKeyComponents.fromComponents(components));
    }
  }

  /**
   * Decodes a Cassandra row containing the token and row key component columns into a
   * {@link TokenRowKeyComponents}.
   */
  @Immutable
  private static final class FormattedRowKeyDecoder
      implements Function<Row, TokenRowKeyComponents> {

    private final RowKeyFormat2 mKeyFormat;
    private final String mTokenColumn;

    /**
     * Create a new {@code FormattedRowKeyDecoder}.
     *
     * @param layout The table layout.
     */
    private FormattedRowKeyDecoder(final KijiTableLayout layout) {
      mTokenColumn = CQLUtils.getTokenColumn(layout);
      mKeyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();
    }

    /** {@inheritDoc} */
    @Override
    public TokenRowKeyComponents apply(final Row row) {

      final List<RowKeyComponent> formatComponents = mKeyFormat.getComponents();
      final Object[] components = new Object[formatComponents.size()];

      for (int i = 0; i < formatComponents.size(); i++) {
        RowKeyComponent component = formatComponents.get(i);
        // TODO: investigate whether we can do this by position instead of creating a bunch of
        // garbage through column name translation
        final String columnName =
            CQLUtils.translateEntityIDComponentNameToColumnName(component.getName());
        switch (component.getType()) {
          case STRING: {
            components[i] = row.getString(columnName);
            break;
          }
          case INTEGER: {
            components[i] = row.getInt(columnName);
            break;
          }
          case LONG: {
            components[i] = row.getLong(columnName);
            break;
          }
          default: throw new IllegalArgumentException("Unknown row key component type.");
        }
      }

      return new TokenRowKeyComponents(
          row.getLong(mTokenColumn),
          KijiRowKeyComponents.fromComponents(components));
    }
  }

  /**
   * A function for converting {@link TokenRowKeyComponents} to {@link EntityId}s.
   */
  private static class RowKeyComponentsToEntityId
      implements Function<TokenRowKeyComponents, EntityId> {

    private final KijiTable mTable;

    /**
     * Create a new function for converting a {@link TokenRowKeyComponents} to an {@link EntityId}.
     * The table must not be closed while the function could still evaluate.
     *
     * @param table The table the row key belongs to.
     */
    public RowKeyComponentsToEntityId(final KijiTable table) {
      mTable = table;
    }

    /** {@inheritDoc} */
    @Override
    public EntityId apply(final TokenRowKeyComponents input) {
      return input.getComponents().getEntityIdForTable(mTable);
    }
  }

  /**
   * A predicate to filter excess Kiji Cells of a column from a Cassandra result set.
   */
  @NotThreadSafe
  private static final class MaxVersionsPredicate implements Predicate<Row> {
    private final int mMaxVersions;

    private int mCurrentCount = 0;
    private ByteBuffer mCurrentFamily = null;
    private ByteBuffer mCurrentQualifier = null;

    /**
     * Create a new column limit predicate.
     *
     * @param maxVersions The number of cells from each column to limit to.
     */
    private MaxVersionsPredicate(final int maxVersions) {
      mMaxVersions = maxVersions;
    }

    /** {@inheritDoc} */
    @Override
    public boolean apply(final Row row) {
      final ByteBuffer family = row.getBytes(CQLUtils.FAMILY_COL);
      final ByteBuffer qualifier = row.getBytes(CQLUtils.QUALIFIER_COL);

      if (!family.equals(mCurrentFamily)) {
        mCurrentFamily = family;
        mCurrentQualifier = qualifier;
        mCurrentCount = 0;
      } else if (!qualifier.equals(mCurrentQualifier)) {
        mCurrentQualifier = qualifier;
        mCurrentCount = 0;
      }

      mCurrentCount += 1;
      return mCurrentCount <= mMaxVersions;
    }
  }

  /**
   * A predicate to filter Kiji cells below a minimum timestamp (inclusive).
   */
  @Immutable
  private static final class MinTimestampPredicate implements Predicate<Row> {

    private final long mTimestamp;

    /**
     * Create a new minimum timestamp predicate.
     *
     * @param timestamp The minimum timestamp.
     */
    private MinTimestampPredicate(final long timestamp) {
      mTimestamp = timestamp;
    }

    /** {@inheritDoc} */
    @Override
    public boolean apply(final Row row) {
      return row.getLong(CQLUtils.VERSION_COL) >= mTimestamp;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hashCode(mTimestamp);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      if (!super.equals(obj)) {
        return false;
      }
      final MinTimestampPredicate other = (MinTimestampPredicate) obj;
      return Objects.equal(this.mTimestamp, other.mTimestamp);
    }
  }

  /**
   * A predicate to filter Kiji cells above a maximum timestamp (exclusive).
   */
  @Immutable
  private static final class MaxTimestampPredicate implements Predicate<Row> {

    private final long mTimestamp;

    /**
     * Create a new maximum timestamp predicate.
     *
     * @param timestamp The maximum timestamp.
     */
    private MaxTimestampPredicate(final long timestamp) {
      mTimestamp = timestamp;
    }

    /** {@inheritDoc} */
    @Override
    public boolean apply(final Row input) {
      return input.getLong(CQLUtils.VERSION_COL) < mTimestamp;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hashCode(mTimestamp);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      if (!super.equals(obj)) {
        return false;
      }
      final MaxTimestampPredicate other = (MaxTimestampPredicate) obj;
      return Objects.equal(this.mTimestamp, other.mTimestamp);
    }
  }

  /** private constructor for utility class. */
  private RowDecoders() { }
}
