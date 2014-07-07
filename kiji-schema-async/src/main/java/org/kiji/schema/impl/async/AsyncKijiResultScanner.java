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
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiResultScanner;

/** Scanner across rows of a KijiTable which returns a KijiResult per row. */
public class AsyncKijiResultScanner implements KijiResultScanner {
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

  // TODO(gabe): Replace with asynchbase
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);
  //private final KijiDataRequest mRequest;
  //private final AsyncKijiTable mTable;
  //private final Scan mScan;
  //private final CellDecoderProvider mDecoderProvider;
  //private final HBaseColumnNameTranslator mColumnNameTranslator;
  //private final EntityIdFactory mEidFactory;
  //private final HTableInterface mHTable;
  //private final boolean mReopenScannerOnTimeout;
  /** Replaced if the current scanner times out. */
  //private ResultScanner mResultScanner;
  /** Replaced after every call to {@link #next()}. */
  //private Result mNextResult;

  /**
   * Initialize a new HBaseKijiResultScanner.
   *
   * @param request data request which will be applied to each row by this scanner.
   * @param table Kiji table from which to scan rows.
   * @param scan HBase Scan object with which defines the actual data to retrieve from HBase.
   * @param decoderProvider Provider for cell decoders with which to decode data from HBase.
   * @param columnNameTranslator Translator for Kiji columns with which to decode data from HBase.
   * @param reopenScannerOnTimeout Whether to reopen the underlying scanner if it times out.
   * @throws IOException in case of an error connecting to HBase.
   */
  // TODO(gabe): Replace this with asynchbase

  /*
public AsyncKijiResultScanner(
    final KijiDataRequest request,
    final AsyncKijiTable table,
    final Scan scan,
    final CellDecoderProvider decoderProvider,
    final HBaseColumnNameTranslator columnNameTranslator,
    final boolean reopenScannerOnTimeout
) throws IOException {
  mRequest = request;
  mTable = table;
  mScan = scan;
  mDecoderProvider = decoderProvider;
  mColumnNameTranslator = columnNameTranslator;
  mReopenScannerOnTimeout = reopenScannerOnTimeout;
  mEidFactory = EntityIdFactory.getFactory(mTable.getLayout());
  mHTable = mTable.openHTableConnection();
  try {
    mResultScanner = mHTable.getScanner(scan);
  } catch (IOException ioe) {
    mHTable.close();
    throw ioe;
  } catch (RuntimeException re) {
    mHTable.close();
    throw re;
  }
  mNextResult = getNextResult();

  final State oldState = mState.getAndSet(State.OPEN);
  Preconditions.checkState(oldState == State.UNINITIALIZED,
      "Cannot open KijiRowScanner instance in state %s.", oldState);
} */

/**
 * Reopen the ResultScanner if mReopenScannerOnTimeout is true. The new ResultScanner should be
 * set to start where the old scanner left off.
 */
  private void reopenScanner() {
    // TODO(gabe): Replace this with asynchbase
    throw new UnsupportedOperationException("Not yet implemented to work with AsyncHBase");

    /*
    if (mReopenScannerOnTimeout) {
      LOG.debug("HBase scanner timed out: closing an reopening a new scanner.");
      final byte[] nextRow = (null == mNextResult)
          ? mScan.getStartRow()
          : leastGreaterThan(mNextResult.getRow());
      mScan.setStartRow(nextRow);
      mResultScanner.close();
      try {
        mResultScanner = mHTable.getScanner(mScan);
      } catch (IOException ioe) {
        throw new KijiIOException(ioe);
      }
    } else {
      throw new KijiIOException("HBase scanner timed out with automatic reopening disabled.");
    }
    */
  }

  /**
   * Get the next HBase Result from the ResultScanner, reopening the scanner if necessary.
   *
   * @return the next HBase Result from the ResultScanner.
   */
  // TODO(gabe): Replace this with asynchbase

  /*
private Result getNextResult() {
  for (int retries = 0; retries < MAX_RETRIES_ON_TIMEOUT; ++retries) {
    try {
      return mResultScanner.next();
    } catch (LeaseException le) {
      reopenScanner();
    } catch (ScannerTimeoutException ste) {
      reopenScanner();
    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }
  }
  throw new KijiIOException(String.format(
      "Unable to get Result from HBase scanner after %d attempts.", MAX_RETRIES_ON_TIMEOUT));
} */

/** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    // TODO(gabe): Replace this with asynchbase
    throw new UnsupportedOperationException("Not yet implemented to work with AsyncHBase");

    /*
    return null != mNextResult;
    */
  }

  /** {@inheritDoc} */
  @Override
  public AsyncKijiResult next() {
    // TODO(gabe): Replace this with asynchbase
    throw new UnsupportedOperationException("Not yet implemented to work with AsyncHBase");

    /*
    final State oldState = mState.get();
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot get element from KijiResultScanner in state %s.", oldState);
    final Result next = mNextResult;
    if (null == next) {
      throw new NoSuchElementException();
    }
    mNextResult = getNextResult();
    return new AsyncKijiResult(
        mEidFactory.getEntityIdFromHBaseRowKey(next.getRow()),
        mRequest,
        next,
        mColumnNameTranslator,
        mDecoderProvider,
        mTable);
    */
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
    // TODO(gabe): Replace this with asynchbase
    throw new UnsupportedOperationException("Not yet implemented to work with AsyncHBase");

    /*
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close KijiResultScanner instance in state %s.", oldState);
    mResultScanner.close();
    mHTable.close();
    */
  }
}
