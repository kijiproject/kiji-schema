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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.AsyncKijiResultScanner;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiFuture;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

/**
 * {@inheritDoc}
 *
 * AsyncHBase implementation of {@code AsyncKijiResultScanner}. This scanner internally caches
 * KijiResults up to the maximum number of rows set through
 * {@code KijiScannerOptions#setRowCaching()}. If no option is set, then it defaults to
 * {@link org.hbase.async.Scanner}'s default maximum number of rows.
 *
 * @param <T> type of {@code KijiCell} value returned by scanned {@code KijiResult}s.
 */
@ThreadSafe
public class AsyncHBaseAsyncKijiResultScanner<T> implements AsyncKijiResultScanner<T> {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncHBaseAsyncKijiResultScanner.class);

  private static final int DEFAULT_NUM_ROWS = 128;

  /** Possible states of the scanner. */
  private static enum State {
    UNINITIALIZED, OPEN, CLOSED
  }

  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);
  private final KijiDataRequest mRequest;
  private final AsyncHBaseKijiTable mTable;
  private final KijiTableLayout mLayout;
  private final CellDecoderProvider mDecoderProvider;
  private final HBaseColumnNameTranslator mColumnNameTranslator;
  private final EntityIdFactory mEidFactory;
  /** Replaced if the current scanner times out. */
  private Scanner mScanner;
  /** Replaced after the cache index exceeds the maximum number of rows. */
  private KijiFuture<ArrayList<ArrayList<KeyValue>>> mNextDeferredResults;
  private final AtomicBoolean mIsDoneScanning;
  private final AtomicInteger mRowCacheIndex;
  private final int mMaxNumRowCache;

  /**
   * Initialize a new AsyncKijiResultScanner.
   *
   * @param request The data request which will be applied to each row by this scanner.
   * @param table The kiji table from which to scan rows.
   * @param scanner The AsyncHBase scanner which will perform the scanning.
   * @param layout The layout of the Kiji table.
   * @param decoderProvider The provider for cell decoders with which to decode data from HBase.
   * @param columnNameTranslator The translator for Kiji columns with which to decode data from HBase.
   * @param reopenScannerOnTimeout Whether to reopen the underlying scanner if it times out.
   * @throws java.io.IOException in case of an error connecting to HBase.
   */
  public AsyncHBaseAsyncKijiResultScanner(
      final KijiDataRequest request,
      final AsyncHBaseKijiTable table,
      final Scanner scanner,
      final KijiScannerOptions scannerOptions,
      final KijiTableLayout layout,
      final CellDecoderProvider decoderProvider,
      final HBaseColumnNameTranslator columnNameTranslator,
      final boolean reopenScannerOnTimeout
  ) throws IOException {
    if (reopenScannerOnTimeout) {
      throw new UnsupportedOperationException(
          "Reopening the Scanner not supported by AsyncKijiResultScanner");
    }
    mIsDoneScanning = new AtomicBoolean(false);
    mRequest = request;
    mTable = table;
    mScanner = scanner;
    mLayout = layout;
    mDecoderProvider = decoderProvider;
    mColumnNameTranslator = columnNameTranslator;
    mEidFactory = EntityIdFactory.getFactory(mTable.getLayout());
    if (null != scannerOptions.getStartRow()) {
      scanner.setStartKey(scannerOptions.getStartRow().getHBaseRowKey());
    }
    if (null != scannerOptions.getStopRow()) {
      scanner.setStopKey(scannerOptions.getStopRow().getHBaseRowKey());
    }
    // Only explicitly set the max number of rows if row caching is greater than 0.
    if (scannerOptions.getRowCaching() > 0) {
      scanner.setMaxNumRows(scannerOptions.getRowCaching());
      mMaxNumRowCache = scannerOptions.getRowCaching();
    } else {
      mMaxNumRowCache = DEFAULT_NUM_ROWS;
    }
    mRowCacheIndex = new AtomicInteger(0);
    getNextResult();
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open KijiRowScanner instance in state %s.", oldState);
  }

  /**
   * Get the next results from the Scanner and reset the row cache index to 0.
   */
  private void getNextResult() {
    final Deferred<ArrayList<ArrayList<KeyValue>>> results = mScanner.nextRows();
    // Add a callback that handles the case where the result is null.
    results.addCallback(
        new Callback<ArrayList<ArrayList<KeyValue>>, ArrayList<ArrayList<KeyValue>>>() {
          @Override
          public ArrayList<ArrayList<KeyValue>> call(final ArrayList<ArrayList<KeyValue>> arg)
              throws Exception {
            if (null == arg) {
              mIsDoneScanning.set(true);
            }
            return arg;
          }
        });
    results.addErrback(
        new Callback<Object, Exception>() {
          @Override
          public Object call(final Exception e) throws Exception {
            throw new KijiIOException(e);
          }
        });
    mRowCacheIndex.set(0);
    mNextDeferredResults = AsyncHBaseKijiFuture.create(results);
  }

  /** {@inheritDoc}
   * <p>Note: If you call next() more than n times before the first future has returned a result,
   * where n is the maximum number of rows, then a KijiFuture will be returned with a
   * KijiIOException. You can handle this in the onFailure() method of a FutureCallback that you
   * can attach to the KijiFuture. This allows the scanner to always return a KijiFuture and never
   * block internally. The user of the scanner can choose if and when to block for the result if
   * they choose.</p>
   */
  @Override
  public KijiFuture<KijiResult<T>> next() {
    final State oldState = mState.get();
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot get element from KijiResultScanner in state %s.", oldState);
    if (mIsDoneScanning.get()) {
      final KijiResult<T> nullResult = null;
      return AsyncHBaseKijiFuture.create(Deferred.fromResult(nullResult));
    } else {
      if (mRowCacheIndex.get() >= mMaxNumRowCache) {
        if (!mNextDeferredResults.isDone()) {
          // We can't get the next result until the current result is done.
          // So we return a KijiFuture with an error that will result in
          // onFailure() being called in a callback.
          Deferred<KijiResult<T>> error = Deferred.fromError(new KijiIOException(
                  "You must wait for the previous KijiFuture to resolve before calling next()."));
          return AsyncHBaseKijiFuture.create(error);
        } else {
          getNextResult();
        }
      }
      // Grab the current row cache index and pass it to the transform through a final int
      final int localCacheIndex = mRowCacheIndex.get();
      KijiFuture<KijiResult<T>> futureResult = AsyncHBaseKijiFuture.createFromFuture(
          Futures.transform(
              mNextDeferredResults,
              new Function<ArrayList<ArrayList<KeyValue>>, KijiResult<T>>() {
                @Nullable
                @Override
                public KijiResult<T> apply(
                    @Nullable final ArrayList<ArrayList<KeyValue>> fullResults
                ) {
                  if (mIsDoneScanning.get() || null == fullResults) {
                      // The Scanner returned null, meaning there are no more results.
                      mIsDoneScanning.set(true);
                      return null;
                    } else {
                      final KijiResult<T> kijiResult;
                      if (localCacheIndex >= fullResults.size() ||
                          fullResults.get(localCacheIndex).isEmpty()) {
                        mIsDoneScanning.set(true);
                        return null;
                      } else {
                        try {
                          final ArrayList<KeyValue> rawResult = fullResults.get(localCacheIndex);
                          kijiResult = AsyncHBaseKijiResult.create(
                              mEidFactory.getEntityIdFromHBaseRowKey(rawResult.get(0).key()),
                              mRequest,
                              rawResult,
                              mTable,
                              mLayout,
                              mColumnNameTranslator,
                              mDecoderProvider);
                        } catch (IOException e) {
                          throw new InternalKijiError(e);
                        }
                      }
                      if (localCacheIndex + 1 >= fullResults.size() &&
                          fullResults.size() < mMaxNumRowCache) {
                          // Since fullResults contains less rows than requested, scanning is done.
                          mIsDoneScanning.set(true);
                        }
                      return kijiResult;
                    }
                  }
              })
      );
      mRowCacheIndex.addAndGet(1);
      return futureResult;
    }
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
