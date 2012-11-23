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
import java.util.Map;
import java.util.NavigableMap;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.schema.HBaseColumnName;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiCounter;
import org.kiji.schema.KijiTable;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.layout.ColumnNameTranslator;
import org.kiji.schema.mapreduce.KijiDelete;
import org.kiji.schema.mapreduce.KijiIncrement;
import org.kiji.schema.mapreduce.KijiPut;

/**
 * Makes modifications to a Kiji table by sending requests directly to HBase from the local client.
 */
public class HBaseKijiTableWriter extends BaseKijiTableWriter {
  /** The kiji table instance. */
  private final HBaseKijiTable mTable;

  /** A column name translator. */
  private final ColumnNameTranslator mColumnNameTranslator;

  /** A kiji cell encoder. */
  private final KijiCellEncoder mCellEncoder;

  /**
   * Creates a non-buffered kiji table writer that sends modifications directly to Kiji.
   *
   * @param table A kiji table.
   * @throws IOException If there is an error creating the writer.
   */
  public HBaseKijiTableWriter(KijiTable table)
      throws IOException {
    mTable = HBaseKijiTable.downcast(table);
    mColumnNameTranslator = new ColumnNameTranslator(mTable.getLayout());
    mCellEncoder = new KijiCellEncoder(table.getKiji().getSchemaTable());
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void put(KijiPut put)
      throws IOException {
    // TODO(robert): Add buffering support back in.
    final Put hbasePut = put.toPut(mColumnNameTranslator, mCellEncoder);

    mTable.getHTable().put(hbasePut);
  }

  /** {@inheritDoc} */
  @Override
  public KijiCounter increment(KijiIncrement increment)
      throws IOException {
    final String family = increment.getFamily();
    final String qualifier = increment.getQualifier();
    verifyIsCounter(family, qualifier);

    // Translate the Kiji column name to an HBase column name.
    final HBaseColumnName hbaseColumnName = mColumnNameTranslator
        .toHBaseColumnName(new KijiColumnName(family, qualifier));
    final byte[] hFamily = hbaseColumnName.getFamily();
    final byte[] hQualifier = hbaseColumnName.getQualifier();

    // Send the increment to the HBase HTable.
    final Increment hbaseIncrement = increment.toIncrement(mColumnNameTranslator);
    final Result result = mTable.getHTable().increment(hbaseIncrement);

    // Verify that the increment result makes sense.
    final NavigableMap<Long, byte[]> counterEntries = result.getMap().get(hFamily).get(hQualifier);
    Preconditions.checkState(null != counterEntries);
    Preconditions.checkState(1 == counterEntries.size());

    // Return the new counter value.
    final Map.Entry<Long, byte[]> counterEntry = counterEntries.firstEntry();
    return new DefaultKijiCounter(counterEntry.getKey(), Bytes.toLong(counterEntry.getValue()));
  }

  /** {@inheritDoc} */
  @Override
  public void delete(KijiDelete delete)
      throws IOException {
    final Delete hbaseDelete = delete.toDelete(mColumnNameTranslator);

    // Send the delete to the HBase HTable.
    mTable.getHTable().delete(hbaseDelete);
  }

  @Override
  public void flush() throws IOException {
    mTable.getHTable().flushCommits();
  }

  @Override
  public void close() throws IOException {
    flush();
  }

  /**
   * Verifies that a column is a counter.
   *
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @throws IOException If the column is not a counter, or it does not exist.
   */
  private void verifyIsCounter(String family, String qualifier) throws IOException {
    final KijiColumnName column = new KijiColumnName(family, qualifier);
    if (mTable.getLayout().getCellSchema(column).getType() != SchemaType.COUNTER) {
      throw new IOException(String.format("Column '%s' is not a counter", column));
    }
  }
}
