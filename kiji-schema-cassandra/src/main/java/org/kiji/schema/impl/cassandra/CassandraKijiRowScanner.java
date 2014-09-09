/**
 * (c) Copyright 2012 WibiData, Inc.
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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowKeyComponents;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.util.DebugResourceTracker;

/**
 * The internal implementation of KijiRowScanner that reads from C* tables.
 */
@ApiAudience.Private
public class CassandraKijiRowScanner implements KijiRowScanner {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiRowScanner.class);

  /** The request used to fetch the row data. */
  private final KijiDataRequest mDataRequest;

  /** The table being scanned. */
  private final CassandraKijiTable mTable;

  /** Provider for cell decoders. */
  private final CellDecoderProvider mCellDecoderProvider;

  /** States of a row scanner instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this row scanner. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** Factory for entity IDs. */
  private final EntityIdFactory mEntityIdFactory;

  /**
   * An iterator over the Row results of a scan. A single Kiji EntityID may have multiple Row
   * results in this iterator in the case of a scan with paged columns.  In this case, the Row
   * objects will be contiguous in the iterator.
   */
  private final PeekingIterator<Row> mRowsIterator;

  /** Holds the table layout for the table being scanned. */
  private final KijiTableLayout mLayout;

  /**
   * Creates a KijiRowScanner over a CassandraKijiTable.
   *
   * @param table being scanned.
   * @param dataRequest of scan.
   * @param cellDecoderProvider of table being scanned.
   * @param resultSets of scan.
   * @throws java.io.IOException if there is a problem creating the row scanner.
   */
  public CassandraKijiRowScanner(
      CassandraKijiTable table,
      KijiDataRequest dataRequest,
      CellDecoderProvider cellDecoderProvider,
      List<ResultSet> resultSets
    ) throws IOException {

    mDataRequest = dataRequest;
    mLayout = table.getLayout();
    mTable = table;
    mCellDecoderProvider = cellDecoderProvider;
    mEntityIdFactory = EntityIdFactory.getFactory(mTable.getLayout());

    // Create an iterator to hold the Row objects returned by the column scans.  The iterator should
    // return Row objects in order of token and then EntityID, so to that Kiji entities have their
    // Row objects contiguously served by the iterator.
    List<Iterator<Row>> rowIterators = Lists.newArrayList();
    for (ResultSet resultSet : resultSets) {
      Iterator<Row> rowIterator = resultSet.iterator();
      rowIterators.add(Iterators.peekingIterator(rowIterator));
    }

    // Merges all of the row iterators in order based on comparing C* tokens of entity IDs.
    mRowsIterator =
        Iterators.peekingIterator(
            Iterators.mergeSorted(rowIterators, new RowComparator(mTable.getLayout())));

    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open KijiRowScanner instance in state %s.", oldState);

    DebugResourceTracker.get().registerResource(this);
  }

  /** {@inheritDoc} */
  @Override
  public CassandraKijiRowIterator iterator() {
    return new CassandraKijiRowIterator();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close KijiRowScanner instance in state %s.", oldState);
    DebugResourceTracker.get().unregisterResource(this);
  }

  // -----------------------------------------------------------------------------------------------

  /** Wraps a Kiji row scanner into a Java iterator. */
  private class CassandraKijiRowIterator implements Iterator<KijiRowData> {
    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
      final State state = mState.get();
      Preconditions.checkState(state == State.OPEN,
          "Cannot check has next on KijiRowScanner instance in state %s.", state);
      return mRowsIterator.hasNext();
    }

    /** {@inheritDoc} */
    @Override
    public KijiRowData next() {
      final State state = mState.get();
      Preconditions.checkState(state == State.OPEN,
          "Cannot get next on KijiRowScanner instance in state %s.", state);

      LOG.info("Getting next row for CassandraKijiRowScanner.");

      // In this section of the code, we create a new KijiRowData instance by looking at the
      // Cassandra row data at the head of all of the various Row iterators that we have.
      // Creating the KijiRowData is tricky only because every Cassandra Row object may not contain
      // an entry for every Kiji entity ID.  We can use the backing Cassandra table's "token"
      // function to order the Cassandra Rows by partition key (Cassandra should always return Rows
      // to use in the order dictated by the token function of the partition keys).

      // The CassandraDataRequestAdapter adds token(key) to the query that creates the Cassandra Row
      // data that we deal with in this method.

      // Therefore, to create a KijiRowData, we get the entity ID with the lowest token value
      // from all of our iterators over Cassandra Rows, get all of the data for the rows with that
      // lowest-token-value entity ID, and then stitch them together.

      Row firstRow = mRowsIterator.next();

      KijiRowKeyComponents entityIDComponents = CQLUtils.getRowKeyComponents(mLayout, firstRow);

      // Get a big set of Row objects for the given entity ID.
      Set<Row> rows = Sets.newHashSet(firstRow);

      while (mRowsIterator.hasNext() && entityIDComponents.equals(
          CQLUtils.getRowKeyComponents(mLayout, mRowsIterator.peek()))) {
        rows.add(mRowsIterator.next());
      }

      EntityId entityID = mEntityIdFactory.getEntityId(entityIDComponents);

      // Now create a KijiRowData with all of these rows.
      try {
        return new CassandraKijiRowData(mTable, mDataRequest, entityID, rows, mCellDecoderProvider);
      } catch (IOException ioe) {
        throw new KijiIOException("Cannot create KijiRowData.", ioe);
      }
    }

    /** {@inheritDoc} */
    @Override
    public void remove() {
      throw new UnsupportedOperationException("KijiRowIterator does not support remove().");
    }
  }

// -----------------------------------------------------------------------------------------------

  /**
   * Compares {@link com.datastax.driver.core.Row} objects by their Kiji Entity ID. The Row objects
   * must be from the same table.  The Rows are first compared by their partion key token, and then
   * by the entity ID components they contain.
   */
  private static final class RowComparator implements Comparator<Row> {
    private final String mTokenColumn;
    private final KijiTableLayout mLayout;

    /**
     * Create a RowComparator for the given table layout.
     *
     * @param layout of the table from which Rows will be compared.
     */
    private RowComparator(KijiTableLayout layout) {
      mTokenColumn = CQLUtils.getTokenColumn(layout);
      mLayout = layout;
    }

    /** {@inheritDoc} */
    @Override
    public int compare(Row o1, Row o2) {
      return ComparisonChain.start()
          .compare(o1.getLong(mTokenColumn), o2.getLong(mTokenColumn))
          .compare(
              CQLUtils.getRowKeyComponents(mLayout, o1),
              CQLUtils.getRowKeyComponents(mLayout, o2))
          .result();
    }
  }
}
