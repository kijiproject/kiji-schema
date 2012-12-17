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

package org.kiji.schema;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import static org.kiji.schema.util.IncrementEquals.eqIncrement;
import static org.kiji.schema.util.PutEquals.eqPut;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.impl.HTableInterfaceFactory;
import org.kiji.schema.layout.ColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.impl.CellSpec;

public class TestHBaseKijiTableWriter extends KijiClientTest {
  private boolean mShouldVerifyMocks;
  private ColumnNameTranslator mColumnNameTranslator;
  private HTableInterface mHTable;
  private KijiTable mKijiTable;
  private KijiTableWriter mWriter;

  @Before
  public void setup() throws Exception {
    getKiji().getMetaTable()
        .updateTableLayout("user", KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));

    mColumnNameTranslator = new ColumnNameTranslator(
        getKiji().getMetaTable().getTableLayout("user"));
    mHTable = createMock(HTableInterface.class);
    mKijiTable = new HBaseKijiTable(getKiji(), "user", new HTableInterfaceFactory() {
      @Override
      public HTableInterface create(Configuration conf, String htabeTableName) throws IOException {
        return mHTable;
      }
    });
    mWriter = mKijiTable.openTableWriter();
  }

  @After
  public void cleanup() throws IOException {
    mWriter.close();
    mKijiTable.close();

    if (mShouldVerifyMocks) {
      verify(mHTable);
    }
  }

  @Test
  public void testPutWithTimestamp() throws Exception {
    // Set the expectations that the writer will execute on the HTable.
    Put expectedPut = new Put(mKijiTable.getEntityId("foo").getHBaseRowKey());
    final KijiColumnName column = new KijiColumnName("info", "name");
    final HBaseColumnName hbaseColumnName = mColumnNameTranslator.toHBaseColumnName(column);
    final KijiTableLayout layout = getKiji().getMetaTable().getTableLayout("user");
    final CellSpec cellSpec = layout.getCellSpec(column)
        .setSchemaTable(getKiji().getSchemaTable());
    final KijiCellEncoder encoder = DefaultKijiCellEncoderFactory.get().create(cellSpec);

    expectedPut.add(hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(), 123L,
        encoder.encode("baz"));
    mHTable.put(eqPut(expectedPut));
    mHTable.flushCommits();
    mHTable.close();
    replay(mHTable);
    mShouldVerifyMocks = true;

    mWriter.put(mKijiTable.getEntityId("foo"), "info", "name", 123L, "baz");
  }

  @Test
  public void testIncrement() throws Exception {
    // Set the expectations that the writer will execute on the HTable.
    final HBaseColumnName hbaseColumnName = mColumnNameTranslator.toHBaseColumnName(
        new KijiColumnName("info", "visits"));
    final Increment expectedIncrement =
        new Increment(mKijiTable.getEntityId("foo").getHBaseRowKey());
    expectedIncrement.addColumn(hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(), 5L);
    final Result cannedResult = new Result(new KeyValue[] {
        new KeyValue(mKijiTable.getEntityId("foo").getHBaseRowKey(),
            hbaseColumnName.getFamily(),
            hbaseColumnName.getQualifier(),
            123L,
            Bytes.toBytes(12L)),
    });
    expect(mHTable.increment(eqIncrement(expectedIncrement)))
        .andReturn(cannedResult);
    mHTable.flushCommits();
    mHTable.close();
    replay(mHTable);

    final KijiCounter kijiCounter =
        mWriter.increment(mKijiTable.getEntityId("foo"), "info", "visits", 5L);
    assertEquals(123L, kijiCounter.getTimestamp());
    assertEquals(12L, kijiCounter.getValue());
  }

  @Test(expected=IOException.class)
  public void testIncrementAColumnThatIsNotACounter() throws Exception {
    // This should throw an exception because we are attempting to increment a column that
    // isn't a counter.
    mWriter.increment(mKijiTable.getEntityId("foo"), "info", "name", 5L);
  }

  @Test
  public void testSetCounter() throws Exception {
    // Set the expectations that the writer will execute on the HTable.
    final HBaseColumnName hbaseColumnName = mColumnNameTranslator.toHBaseColumnName(
        new KijiColumnName("info", "visits"));
    final Put expectedPut = new Put(mKijiTable.getEntityId("foo").getHBaseRowKey());
    expectedPut.add(
        hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(), Bytes.toBytes(5L));
    mHTable.put(eqPut(expectedPut));
    mHTable.flushCommits();
    mHTable.close();
    replay(mHTable);

    mWriter.put(mKijiTable.getEntityId("foo"), "info", "visits", 5L);
  }
}
