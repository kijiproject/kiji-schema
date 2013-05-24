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

package org.kiji.schema.impl;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.util.Debug;

/**
 * The internal implementation of KijiRowScanner that reads from HTables.
 */
@ApiAudience.Private
public class HBaseKijiRowScanner implements KijiRowScanner {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseKijiRowScanner.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger("cleanup." + HBaseKijiRowScanner.class.getName());

  /** The HBase result scanner. */
  private final ResultScanner mResultScanner;

  /** The request used to fetch the row data. */
  private final KijiDataRequest mDataRequest;

  /** The table being scanned. */
  private final HBaseKijiTable mTable;

  /** Provider for cell decoders. */
  private final CellDecoderProvider mCellDecoderProvider;

  /** Whether the scanner is open. */
  private final AtomicBoolean mIsOpen = new AtomicBoolean(false);

  /** For debugging finalize(). */
  private String mConstructorStack = "";

  /**
   * A class to encapsulate the various options the HBaseKijiRowScanner constructor requires.
   */
  public static class Options {
    private ResultScanner mHBaseResultScanner;
    private KijiDataRequest mDataRequest;
    private HBaseKijiTable mTable;
    private CellDecoderProvider mCellDecoderProvider;

    /**
     * Sets the HBase result scanner the KijiRowScanner will wrap.
     *
     * @param hbaseResultScanner An HBase result scanner.
     * @return This options instance.
     */
    public Options withHBaseResultScanner(ResultScanner hbaseResultScanner) {
      mHBaseResultScanner = hbaseResultScanner;
      return this;
    }

    /**
     * Sets the data request used to generate the KijiRowScanner.
     *
     * @param dataRequest A data request.
     * @return This options instance.
     */
    public Options withDataRequest(KijiDataRequest dataRequest) {
      mDataRequest = dataRequest;
      return this;
    }

    /**
     * Sets the table being scanned.
     *
     * @param table The table being scanned.
     * @return This options instance.
     */
    public Options withTable(HBaseKijiTable table) {
      mTable = table;
      return this;
    }

    /**
     * Sets a provider for cell decoders.
     *
     * @param cellDecoderProvider Provider for cell decoders.
     * @return This options instance.
     */
    public Options withCellDecoderProvider(CellDecoderProvider cellDecoderProvider) {
      mCellDecoderProvider = cellDecoderProvider;
      return this;
    }

    /**
     * Gets the HBase result scanner.
     *
     * @return The HBase result scanner.
     */
    public ResultScanner getHBaseResultScanner() {
      return mHBaseResultScanner;
    }

    /**
     * Gets the data request.
     *
     * @return The data request.
     */
    public KijiDataRequest getDataRequest() {
      return mDataRequest;
    }

    /**
     * Gets the table being scanned.
     *
     * @return The Kiji table.
     */
    public HBaseKijiTable getTable() {
      return mTable;
    }

    /**
     * Gets the provider for cell decoders.
     *
     * @return the provider for cell decoders.
     */
    public CellDecoderProvider getCellDecoderProvider() {
      return mCellDecoderProvider;
    }
  }

  /**
   * Creates a new <code>KijiRowScanner</code> instance.
   *
   * @param options The options for this scanner.
   */
  public HBaseKijiRowScanner(Options options) {
    if (CLEANUP_LOG.isDebugEnabled()) {
      mConstructorStack = Debug.getStackTrace();
    }

    mResultScanner = options.getHBaseResultScanner();
    mDataRequest = options.getDataRequest();
    mTable = options.getTable();
    mCellDecoderProvider = options.getCellDecoderProvider();

    mIsOpen.set(true);
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowIterator iterator() {
    return new KijiRowIterator(mResultScanner.iterator());
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    final boolean wasOpen = mIsOpen.getAndSet(false);
    if (!wasOpen) {
      LOG.warn("Called HBaseKijiRowScanner.close() more than once.");
      LOG.debug("Stacktrace of extra call to HBaseKijiRowScanner.close():\n{}",
          Debug.getStackTrace());
      return;
    }
    mResultScanner.close();
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    if (mIsOpen.get()) {
      CLEANUP_LOG.warn(
          "Closing HBaseKijiRowScanner in finalize() : please close it explicitly!\n"
          + "Call stack when the scanner was constructed:\n{}",
          mConstructorStack);
      close();
    }
    super.finalize();
  }

  /**
   * Class for iterating over a Kiji Table.
   */
  private class KijiRowIterator implements Iterator<KijiRowData> {
    /** The wrapped HBase results. */
    private final Iterator<Result> mResults;

    /**
     * Creates a new <code>KijiRowIterator</code> instance.
     *
     * @param results An Iterator of HBase results.
     */
    public KijiRowIterator(Iterator<Result> results) {
      mResults = Preconditions.checkNotNull(results);
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
      return mResults.hasNext();
    }

    /** {@inheritDoc} */
    @Override
    public KijiRowData next() {
      final Result result = mResults.next();
      if (null == result) {
        return null;
      }

      // Read the entity id from the HBase result.
      final EntityId entityId = EntityIdFactory.getFactory(mTable.getLayout())
          .getEntityIdFromHBaseRowKey(result.getRow());
      try {
        return new HBaseKijiRowData(mTable, mDataRequest, entityId, result, mCellDecoderProvider);
      } catch (IOException ioe) {
        throw new KijiIOException(ioe);
      }
    }

    /** {@inheritDoc} */
    @Override
    public void remove() {
      throw new UnsupportedOperationException("KijiRowIterator does not support remove().");
    }
  }
}
