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

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiCellDecoderFactory;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;


/**
 * The internal implementation of KijiRowScanner that reads from HTables.
 */
@ApiAudience.Private
public class HBaseKijiRowScanner implements KijiRowScanner {
  /** The HBase result scanner. */
  private final ResultScanner mResultScanner;

  /** The request used to fetch the row data. */
  private final KijiDataRequest mKijiDataRequest;

  /** The table being scanned. */
  private final HBaseKijiTable mTable;

  /** A cell decoder factory. */
  private final KijiCellDecoderFactory mCellDecoderFactory;

  /**
   * A class to encapsulate the various options the HBaseKijiRowScanner constructor requires.
   */
  public static class Options {
    private ResultScanner mHBaseResultScanner;
    private KijiDataRequest mDataRequest;
    private HBaseKijiTable mTable;
    private KijiCellDecoderFactory mCellDecoderFactory;

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
     * Sets the cell decoder factory to use when reading cells from the scanner.
     *
     * @param cellDecoderFactory A cell decoder factory.
     * @return This options instance.
     */
    public Options withCellDecoderFactory(KijiCellDecoderFactory cellDecoderFactory) {
      mCellDecoderFactory = cellDecoderFactory;
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
     * Gets the cell decoder factory.
     *
     * @return The cell decoder factory.
     */
    public KijiCellDecoderFactory getCellDecoderFactory() {
      return mCellDecoderFactory;
    }
  }

  /**
   * Creates a new <code>KijiRowScanner</code> instance.
   *
   * @param options The options for this scanner.
   */
  public HBaseKijiRowScanner(Options options) {
    mResultScanner = options.getHBaseResultScanner();
    mKijiDataRequest = options.getDataRequest();
    mTable = options.getTable();
    mCellDecoderFactory = options.getCellDecoderFactory();
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowIterator iterator() {
    return new KijiRowIterator(mResultScanner.iterator());
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    mResultScanner.close();
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    close();
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
      assert null != results;
      mResults = results;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
      return mResults.hasNext();
    }

    /** {@inheritDoc} */
    @Override
    public KijiRowData next() {
      Result hbaseResult = mResults.next();
      if (null == hbaseResult) {
        return null;
      }

      HBaseKijiRowData rowData = new HBaseKijiRowData(new HBaseKijiRowData.Options()
          .withHBaseResult(hbaseResult)
          .withDataRequest(mKijiDataRequest)
          .withTableLayout(mTable.getLayout())
          .withCellDecoderFactory(mCellDecoderFactory)
          .withHTable(mTable.getHTable()));
      return rowData;
    }

    /** {@inheritDoc} */
    @Override
    public void remove() {
      throw new UnsupportedOperationException("KijiRowIterator does not support remove().");
    }
  }
}
