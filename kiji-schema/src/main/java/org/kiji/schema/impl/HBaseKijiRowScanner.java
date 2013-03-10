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
import org.kiji.schema.util.Debug;
import org.kiji.schema.util.ResourceUtils;


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
  private final KijiDataRequest mKijiDataRequest;

  /** The table being scanned. */
  private final HBaseKijiTable mTable;

  /** Whether the writer is open. */
  private boolean mIsOpen;
  /** For debugging finalize(). */
  private String mConstructorStack = "";

  /**
   * A class to encapsulate the various options the HBaseKijiRowScanner constructor requires.
   */
  public static class Options {
    private ResultScanner mHBaseResultScanner;
    private KijiDataRequest mDataRequest;
    private HBaseKijiTable mTable;

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
  }

  /**
   * Creates a new <code>KijiRowScanner</code> instance.
   *
   * @param options The options for this scanner.
   */
  public HBaseKijiRowScanner(Options options) {
    mIsOpen = true;
    if (CLEANUP_LOG.isDebugEnabled()) {
      mConstructorStack = Debug.getStackTrace();
    }

    mResultScanner = options.getHBaseResultScanner();
    mKijiDataRequest = options.getDataRequest();
    mTable = options.getTable();
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowIterator iterator() {
    return new KijiRowIterator(mResultScanner.iterator());
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    if (!mIsOpen) {
      LOG.warn("Called close() on [HBase]KijiRowScanner more than once.");
    }

    mIsOpen = false;

    ResourceUtils.closeOrLog(mResultScanner);
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    if (mIsOpen) {
      CLEANUP_LOG.warn("Closing [HBase]KijiRowScanner in finalize().");
      CLEANUP_LOG.warn("You should close it explicitly.");
      CLEANUP_LOG.debug("Call stack when this scanner was constructed:");
      CLEANUP_LOG.debug(mConstructorStack);
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
        // TODO: Inject the cell decoder factory in the row data
        return new HBaseKijiRowData(entityId, mKijiDataRequest, mTable, result);
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
