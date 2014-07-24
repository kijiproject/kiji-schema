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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.hbase.async.UnknownScannerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiResultScanner;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

/**
 * {@inheritDoc}
 *
 * AsyncHBase implementation of {@code KijiResultScanner}.
 *
 * @param <T> type of {@code KijiCell} value returned by scanned {@code KijiResult}s.
 */
public class AsyncKijiResultScanner<T> implements KijiResultScanner<T> {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncKijiResultScanner.class);

  private static final int MAX_RETRIES_ON_TIMEOUT = 3;

  /** Possible states of the scanner. */
  private static enum State {
    UNINITIALIZED, OPEN, CLOSED
  }

  /**
   * Get the least HBase row key greater than the given key.
   *
   * @param key HBase row key for which to get the next possible key.
   * @return the least HBase row key greater than the given key.
   */
  private static byte[] leastGreaterThan(
      final byte[] key
  ) {
    return Arrays.copyOf(key, key.length + 1);
  }

  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);
  private final KijiDataRequest mRequest;
  private final AsyncKijiTable mTable;
  private final KijiTableLayout mLayout;
  private final CellDecoderProvider mDecoderProvider;
  private final HBaseColumnNameTranslator mColumnNameTranslator;
  private final EntityIdFactory mEidFactory;
  private final HBaseClient mHBClient;
  private final byte[] mTableName;
  private final boolean mReopenScannerOnTimeout;
  // Async Scanner does not allow access to startKey and stopKey,
  // so we keep track of them on our own
  private final EntityId mStartKey;
  private final EntityId mStopKey;
  /** Replaced if the current scanner times out. */
  private Scanner mScanner;
  /** Replaced after every call to {@link #next()} if necessary. */
  private ArrayList<ArrayList<KeyValue>> mNextResults;
  private int mRowCacheIndex;

  /**
   * Initialize a new AsyncKijiResultScanner.
   *
   * @param request data request which will be applied to each row by this scanner.
   * @param table Kiji table from which to scan rows.
   * @param scanner HBase Scan object with which defines the actual data to retrieve from HBase.
   * @param layout of Kiji table.
   * @param decoderProvider Provider for cell decoders with which to decode data from HBase.
   * @param columnNameTranslator Translator for Kiji columns with which to decode data from HBase.
   * @param reopenScannerOnTimeout Whether to reopen the underlying scanner if it times out.
   * @throws IOException in case of an error connecting to HBase.
   */
  public AsyncKijiResultScanner(
      final KijiDataRequest request,
      final AsyncKijiTable table,
      final Scanner scanner,
      final EntityId startKey,
      final EntityId stopKey,
      final KijiTableLayout layout,
      final CellDecoderProvider decoderProvider,
      final HBaseColumnNameTranslator columnNameTranslator,
      final boolean reopenScannerOnTimeout
  ) throws IOException {
    mRequest = request;
    mTable = table;
    mStartKey = startKey;
    mStopKey = stopKey;
    mScanner = scanner;
    mLayout = layout;
    mDecoderProvider = decoderProvider;
    mColumnNameTranslator = columnNameTranslator;
    mReopenScannerOnTimeout = reopenScannerOnTimeout;
    mEidFactory = EntityIdFactory.getFactory(mTable.getLayout());
    mHBClient = mTable.getHBClient();
    mTableName = KijiManagedHBaseTableName
        .getKijiTableName(mTable.getURI().getInstance(), mTable.getURI().getTable()).toBytes();
    if (null != mStartKey) {
      scanner.setStartKey(mStartKey.getHBaseRowKey());
    }
    if (null != mStopKey) {
      scanner.setStopKey(mStopKey.getHBaseRowKey());
    }
    mNextResults = getNextResult();

    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open KijiRowScanner instance in state %s.", oldState);
  }

  /**
   * Reopen the ResultScanner if mReopenScannerOnTimeout is true. The new ResultScanner should be
   * set to start where the old scanner left off.
   */
  private void reopenScanner() {
    if (mReopenScannerOnTimeout) {
      LOG.debug("Async scanner timed out: closing an reopening a new scanner.");
      final byte[] nextRow = (null == mNextResults)
          ? mStartKey.getHBaseRowKey()
          : leastGreaterThan(mNextResults.get(mRowCacheIndex).get(0).key());
      mScanner.close();
      mScanner = mHBClient.newScanner(mTableName);
      mScanner.setStartKey(nextRow);
      if (null != mStopKey) {
        mScanner.setStopKey(mStopKey.getHBaseRowKey());
      }
    } else {
      throw new KijiIOException("Async scanner timed out with automatic reopening disabled.");
    }
  }

  /**
   * Get the next results from the Scanner, reopening the scanner if necessary.
   *
   * @return the next results from the Scanner.
   */
  private ArrayList<ArrayList<KeyValue>> getNextResult() {
    for (int retries = 0; retries < MAX_RETRIES_ON_TIMEOUT; ++retries) {
      try {
        final ArrayList<ArrayList<KeyValue>> results = mScanner.nextRows().join();
        mRowCacheIndex = 0;
        return results;
      } catch (Exception e) {
        if (e instanceof UnknownScannerException) {
          reopenScanner();
        }
        else if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        } else if (e instanceof RuntimeException) {
          throw (RuntimeException) e;
        } else {
          throw new KijiIOException(e);
        }
      }
    }
    throw new KijiIOException(String.format(
        "Unable to get results from AsyncHBase scanner after %d attempts.", MAX_RETRIES_ON_TIMEOUT));
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    return null != mNextResults;
  }

  /** {@inheritDoc} */
  @Override
  public KijiResult<T> next() {
    final State oldState = mState.get();
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot get element from KijiResultScanner in state %s.", oldState);
    if (null == mNextResults) {
      throw new NoSuchElementException();
    }

    final ArrayList<KeyValue> next = mNextResults.get(mRowCacheIndex);
    if (null == next) {
      throw new NoSuchElementException();
    }
    mRowCacheIndex += 1;
    if (mRowCacheIndex >= mNextResults.size()) {
      mNextResults = getNextResult();
    }


    try {
      return AsyncKijiResult.create(
          mEidFactory.getEntityIdFromHBaseRowKey(next.get(0).key()),
          mRequest,
          next,
          mTable,
          mLayout,
          mColumnNameTranslator,
          mDecoderProvider);
    } catch (IOException e) {
      throw new KijiIOException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void remove() {
    throw new UnsupportedOperationException(
        getClass().getSimpleName() + " does not support remove().");
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close KijiResultScanner instance in state %s.", oldState);
    try {
      mScanner.close().join();
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }
  }
}
