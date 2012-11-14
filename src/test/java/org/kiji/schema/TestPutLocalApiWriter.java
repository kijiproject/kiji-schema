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
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import static org.kiji.schema.util.PutEquals.eqPut;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.impl.HTableFactory;
import org.kiji.schema.layout.ColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;


public class TestPutLocalApiWriter
    extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestPutLocalApiWriter.class);
  public static final int MAX_BUFFERED_WRITES = 20;
  private HTable mHTable;
  private KijiTable mKijiTable;
  private EntityIdFactory mEntityIdFactory;
  private PutLocalApiWriter mWriter;
  private ColumnNameTranslator mColumnNameTranslator;

  @Before
  public void setup() throws Exception {
    final KijiTableLayout layout = getKiji().getMetaTable()
        .updateTableLayout("user", KijiTableLayouts.getLayout(KijiTableLayouts.USER_TABLE));

    mColumnNameTranslator = new ColumnNameTranslator(layout);
    mHTable = createMock(HTable.class);
    mKijiTable = new HBaseKijiTable(getKiji(), "user", new HTableFactory() {
      @Override
      public HTable create(Configuration conf, String htabeTableName) throws IOException {
        return mHTable;
      }
    });
    mEntityIdFactory = mKijiTable.getEntityIdFactory();

    mWriter = new PutLocalApiWriter(new PutLocalApiWriter.Options()
        .withEntityIdFactory(EntityIdFactory.create(layout.getDesc().getKeysFormat()))
        .withMaxBufferedWrites(MAX_BUFFERED_WRITES)
        .withCellEncoder(new KijiCellEncoder(getKiji().getSchemaTable()))
        .withColumnNameTranslator(mColumnNameTranslator)
        .withKijiTable(mKijiTable)
        .withFamily("info"));
  }

  @After
  public void cleanup() throws IOException {
    mWriter.close();
    mKijiTable.close();

    verify(mHTable);
  }

  @Test
  public void testWrite() throws Exception {
    // Record expected behavior
    Put expectedPut = new Put(mEntityIdFactory.fromKijiRowKey("foo").getHBaseRowKey());
    KijiCellEncoder cellEncoder = new KijiCellEncoder(getKiji().getSchemaTable());
    HBaseColumnName hBaseColumnName = mColumnNameTranslator.toHBaseColumnName(
        new KijiColumnName("info", "name"));
    expectedPut.add(
        hBaseColumnName.getFamily(),
        hBaseColumnName.getQualifier(),
        123L,
        cellEncoder.encode(
            new KijiCell<CharSequence>(Schema.create(Schema.Type.STRING), "baz"),
            KijiCellFormat.HASH));
    expectedPut.add(
        hBaseColumnName.getFamily(),
        hBaseColumnName.getQualifier(),
        124L,
        cellEncoder.encode(
            new KijiCell<CharSequence>(Schema.create(Schema.Type.STRING), "foo"),
            KijiCellFormat.HASH));
    mHTable.put(eqPut(expectedPut));
    mHTable.flushCommits();
    mHTable.close();
    replay(mHTable);

    // Verify that the expected behavior performed as expected
    mWriter.write(mEntityIdFactory.fromKijiRowKey("foo"), "name", "baz");
    assertEquals(1, mWriter.getNumBufferedElements());
    mWriter.write(mEntityIdFactory.fromKijiRowKey("foo"), "name", "foo");
    assertEquals(2, mWriter.getNumBufferedElements());
  }

  @Test
  public void testBuffering() throws Exception {
    // Record expectation
    Put expectedPut1 = new Put(mEntityIdFactory.fromKijiRowKey("foo2").getHBaseRowKey());
    Put expectedPut2 = new Put(mEntityIdFactory.fromKijiRowKey("bar2").getHBaseRowKey());
    Put expectedPut3 = new Put(mEntityIdFactory.fromKijiRowKey("bar2").getHBaseRowKey());
    KijiCellEncoder cellEncoder = new KijiCellEncoder(getKiji().getSchemaTable());
    HBaseColumnName hBaseColumnName = mColumnNameTranslator.toHBaseColumnName(
        new KijiColumnName("info", "name"));
    for (long i = 0; i < 10; i++) {
      expectedPut1.add(
          hBaseColumnName.getFamily(),
          hBaseColumnName.getQualifier(),
          i,
          cellEncoder.encode(
              new KijiCell<CharSequence>(Schema.create(Schema.Type.STRING), "Robert Chu " + i),
              KijiCellFormat.HASH));
    }
    for (long i = 0; i < 10; i++) {
      expectedPut2.add(
          hBaseColumnName.getFamily(),
          hBaseColumnName.getQualifier(),
          i,
          cellEncoder.encode(
              new KijiCell<CharSequence>(Schema.create(Schema.Type.STRING), "Robert Chiu " + i),
              KijiCellFormat.HASH));
    }
    mHTable.put(eqPut(expectedPut1));
    mHTable.put(eqPut(expectedPut2));
    expectedPut3.add(
        hBaseColumnName.getFamily(),
        hBaseColumnName.getQualifier(),
        10L,
        cellEncoder.encode(
            new KijiCell<CharSequence>(Schema.create(Schema.Type.STRING), "Robert Chiu 10"),
            KijiCellFormat.HASH));
    mHTable.flushCommits();
    mHTable.put(eqPut(expectedPut3));
    mHTable.flushCommits();
    mHTable.close();
    replay(mHTable);

    // Verify that the expected behavior performed as expected
    for (long i = 0; i < 10; i++) {
      mWriter.write(mEntityIdFactory.fromKijiRowKey("foo2"), "name", "Robert Chu " + i);
    }
    for (long i = 0; i < 10; i++) {
      mWriter.write(mEntityIdFactory.fromKijiRowKey("bar2"), "name", "Robert Chiu " + i);
    }
    assertEquals(0, mWriter.getNumBufferedElements());
    mWriter.write(mEntityIdFactory.fromKijiRowKey("bar2"), "name", "Robert Chiu 10");
    assertEquals(1, mWriter.getNumBufferedElements());
  }
}
