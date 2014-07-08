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

package org.kiji.schema.impl.cassandra;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiColumnPagingNotEnabledException;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.cassandra.CassandraColumnName;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.filter.KijiColumnFilter;
import org.kiji.schema.filter.KijiColumnRangeFilter;
import org.kiji.schema.layout.CassandraColumnNameTranslator;

/**
 * Pages through the many qualifiers of a map-type family.
 *
 * <p>
 *   The max-versions parameter on a map-type family applies on a per-qualifier basis.
 *   This does not limit the total number of versions returned for the entire map-type family.
 * </p>
 */
@ApiAudience.Private
public final class CassandraQualifierPager implements Iterator<String[]>, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraQualifierPager.class);

  /** Entity ID of the row being paged through. */
  private final EntityId mEntityId;

  /** KijiTable to read from. */
  private final CassandraKijiTable mTable;

  /** Name of the map-type family being paged through. */
  private final KijiColumnName mFamily;

  /** Full data request. */
  private final KijiDataRequest mDataRequest;

  /** Column data request for the map-type family to page through. */
  private final KijiDataRequest.Column mColumnRequest;

  /** True only if there is another page of data to read through {@link #next()}. */
  private boolean mHasNext;

  /** Paged Iterator over all of the Rows coming back from Cassandra. */
  private PeekingIterator<Row> mRowIterator;

  /**
   * Highest qualifier (according to the HBase bytes comparator) returned so far.
   * This is the low bound (exclusive) for qualifiers to retrieve next.
   */
  private ByteBuffer mMinQualifier = null;

  private final CassandraColumnNameTranslator mColumnNameTranslator;

  /**
   * Initializes a qualifier pager.
   *
   * @param entityId The entityId of the row.
   * @param dataRequest The requested data.
   * @param table The Kiji table that this row belongs to.
   * @param family Iterate through the qualifiers from this map-type family.
   * @throws org.kiji.schema.KijiColumnPagingNotEnabledException If paging is not enabled for the
   *   specified family.
   */
  public CassandraQualifierPager(
      EntityId entityId,
      KijiDataRequest dataRequest,
      CassandraKijiTable table,
      KijiColumnName family) throws KijiColumnPagingNotEnabledException {

    Preconditions.checkArgument(!family.isFullyQualified(),
        "Must use HBaseQualifierPager on a map-type family, but got '{}'.", family);
    mFamily = family;

    mDataRequest = dataRequest;
    mColumnRequest = mDataRequest.getColumn(family.getFamily(), null);
    if (!mColumnRequest.isPagingEnabled()) {
      throw new KijiColumnPagingNotEnabledException(
        String.format("Paging is not enabled for column [%s].", family));
    }

    mColumnNameTranslator = table.getColumnNameTranslator();

    mEntityId = entityId;
    mTable = table;
    mHasNext = true;  // there might be no page to read, but we don't know until we issue an RPC

    initializeRowIterator();

    // Only retain the table if everything else ran fine:
    mTable.retain();
  }

  /**
   * Initialize the row iterator.
   */
  private void initializeRowIterator() {
    // Issue a paged SELECT statement to get all of the qualifiers for this map family from C*.
    // Get the Cassandra table name for this column family
    String cassandraTableName = CassandraTableName.getKijiTableName(mTable.getURI()).toString();

    // Get the translated name for the column family.
    String translatedLocalityGroup = null;
    ByteBuffer translatedFamily = null;
    try {
      final CassandraColumnName cassandraColumn =
          mColumnNameTranslator.toCassandraColumnName(mFamily);
      translatedLocalityGroup = cassandraColumn.getLocalityGroup();
      translatedFamily = cassandraColumn.getFamilyBuffer();
    } catch (NoSuchColumnException nsce) {
      // TODO: Do something here!
      throw new InternalKijiError(nsce);
    }
    BoundStatement boundStatement;
    // Need to get versions here so that we can filter out versions that don't match the data
    // request.  Sadly, there is no way to put the version range restriction into this query, since
    // we aren't restricting the qualifiers at all.
    String queryString = String.format(
        "SELECT %s, %s from %s WHERE %s=? AND %s=? AND %s=?",
        CQLUtils.QUALIFIER_COL,
        CQLUtils.VERSION_COL,
        cassandraTableName,
        CQLUtils.RAW_KEY_COL,
        CQLUtils.LOCALITY_GROUP_COL,
        CQLUtils.FAMILY_COL
    );


    // TODO: Make this code more robust for different kinds of filters.
    KijiColumnFilter columnFilter = mColumnRequest.getFilter();

    CassandraAdmin admin = mTable.getAdmin();

    if (null == columnFilter) {

      PreparedStatement preparedStatement = admin.getPreparedStatement(queryString);
      boundStatement = preparedStatement.bind(
          CassandraByteUtil.bytesToByteBuffer(mEntityId.getHBaseRowKey()),
          translatedLocalityGroup,
          translatedFamily
      );
    } else if (columnFilter instanceof KijiColumnRangeFilter) {
      KijiColumnRangeFilter rangeFilter = (KijiColumnRangeFilter) columnFilter;
      boundStatement = createBoundStatementForFilter(
          admin,
          rangeFilter,
          queryString,
          translatedLocalityGroup,
          translatedFamily
      );

    } else {
      throw new UnsupportedOperationException(
          "CassandraQualifierPager supports only column ranger filters, not "
              + columnFilter.getClass()
      );
    }
    boundStatement.setFetchSize(mColumnRequest.getPageSize());
    mRowIterator = Iterators.peekingIterator(admin.execute(boundStatement).iterator());
  }

  /**
   * Create a bound version of a `SELECT` statement that includes additional `WHERE` clauses for
   * the qualifier.
   *
   * @param admin for this Kiji instance.
   * @param rangeFilter for the qualifiers.
   * @param queryString for the SELECT statement.
   * @param translatedLocalityGroup translated name for the locality group.
   * @param translatedFamily translated name for the column family.
   * @return A bound version of the statement with the column qualifier limits.
   */
  private BoundStatement createBoundStatementForFilter(
      CassandraAdmin admin,
      KijiColumnRangeFilter rangeFilter,
      String queryString,
      String translatedLocalityGroup,
      ByteBuffer translatedFamily
  ) {

    String minQualifier = rangeFilter.getMinQualifier();
    String maxQualifier = rangeFilter.getMaxQualifier();

    if (null != minQualifier && rangeFilter.isIncludeMin()) {
      queryString += String.format(" AND %s >= ? ", CQLUtils.QUALIFIER_COL);
    }

    if (null != minQualifier && !rangeFilter.isIncludeMin()) {
      queryString += String.format(" AND %s > ? ", CQLUtils.QUALIFIER_COL);
    }

    if (null != maxQualifier && rangeFilter.isIncludeMax()) {
      queryString += String.format(" AND %s <= ? ", CQLUtils.QUALIFIER_COL);
    }

    if (null != maxQualifier && !rangeFilter.isIncludeMax()) {
      queryString += String.format(" AND %s < ? ", CQLUtils.QUALIFIER_COL);
    }

    PreparedStatement preparedStatement = admin.getPreparedStatement(queryString);
    BoundStatement boundStatement = null;

    if (null == minQualifier && null == maxQualifier) {
      boundStatement = preparedStatement.bind(
        CassandraByteUtil.bytesToByteBuffer(mEntityId.getHBaseRowKey()),
        translatedLocalityGroup,
        translatedFamily
      );
    } else if (null != minQualifier && null == maxQualifier) {
      boundStatement = preparedStatement.bind(
          CassandraByteUtil.bytesToByteBuffer(mEntityId.getHBaseRowKey()),
          translatedLocalityGroup,
          translatedFamily,
          ByteBuffer.wrap(Bytes.toBytes(minQualifier)));
    } else if (null == minQualifier && null != maxQualifier) {
      boundStatement = preparedStatement.bind(
          CassandraByteUtil.bytesToByteBuffer(mEntityId.getHBaseRowKey()),
          translatedLocalityGroup,
          translatedFamily,
          ByteBuffer.wrap(Bytes.toBytes(maxQualifier)));
    } else if (null != minQualifier && null != maxQualifier) {
      boundStatement = preparedStatement.bind(
          CassandraByteUtil.bytesToByteBuffer(mEntityId.getHBaseRowKey()),
          translatedLocalityGroup,
          translatedFamily,
          ByteBuffer.wrap(Bytes.toBytes(minQualifier)),
          ByteBuffer.wrap(Bytes.toBytes(maxQualifier)));
    } else {
      assert(false);
    }

    return boundStatement;
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    return mHasNext;
  }

  /** {@inheritDoc} */
  @Override
  public String[] next() {
    return next(mColumnRequest.getPageSize());
  }

  /**
   * Fetches another page of qualifiers.
   *
   * @param pageSize Maximum number of qualifiers to retrieve in the page.
   * @return the next page of qualifiers.
   */
  public String[] next(int pageSize) {
    if (!mHasNext) {
      throw new NoSuchElementException();
    }
    Preconditions.checkArgument(pageSize > 0, "Page size must be >= 1, got %s", pageSize);

    // Verify that we properly finished off all of the repeated qualifier values during the last
    // call to this method.
    assert(null == mMinQualifier
        || !mMinQualifier.equals(mRowIterator.peek().getBytes(CQLUtils.QUALIFIER_COL)));

    LinkedHashSet<String> qualifiers = new LinkedHashSet<String>();

    // Only return qualifiers that fall within the time range specified in the data request.
    long minTimestamp = mDataRequest.getMinTimestamp();
    long maxTimestamp = mDataRequest.getMaxTimestamp();

    while (qualifiers.size() < pageSize && mRowIterator.hasNext()) {

      // Check whether *any* of the cells for this qualifier meet the time
      // range criteria.
      boolean bAtLeastOneExistsInTimeRange = false;

      mMinQualifier = mRowIterator.peek().getBytes(CQLUtils.QUALIFIER_COL);
      assert(null != mMinQualifier);

      // Remove all of the duplicates.
      while (mRowIterator.hasNext() && mRowIterator
          .peek()
          .getBytes(CQLUtils.QUALIFIER_COL)
          .equals(mMinQualifier)) {

        long timestamp = mRowIterator.peek().getLong(CQLUtils.VERSION_COL);
        if (timestamp >= minTimestamp && timestamp < maxTimestamp) {
          bAtLeastOneExistsInTimeRange = true;
        }
        mRowIterator.next();
      }
      if (bAtLeastOneExistsInTimeRange) {
        qualifiers.add(Bytes.toString(CassandraByteUtil.byteBuffertoBytes(mMinQualifier)));
      }
    }

    mHasNext = mRowIterator.hasNext();

    // Sanity check if we think there are more qualifiers to get that this set of results is full!
    if (mHasNext) {
      assert(qualifiers.size() == pageSize);
    }

    return qualifiers.toArray(new String[qualifiers.size()]);
  }

  /** {@inheritDoc} */
  @Override
  public void remove() {
    throw new UnsupportedOperationException("KijiPager.remove() is not supported.");
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mTable.release();
  }
}
