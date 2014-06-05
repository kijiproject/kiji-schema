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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hbase.HConstants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.avro.EmptyRecord;
import org.kiji.schema.avro.TestRecord1;
import org.kiji.schema.avro.TestRecord2;
import org.kiji.schema.avro.TestRecord3;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestReaderSchema extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestReaderSchema.class);

  private static final String TABLE_NAME = "table";

  /** KijiTable used for the test (named TABLE_NAME). */
  private KijiTable mTable;

  /** Requests all columns in family "family". */
  private static final KijiDataRequest DATA_REQUEST = KijiDataRequest.builder()
      .addColumns(ColumnsDef.create().withMaxVersions(HConstants.ALL_VERSIONS).addFamily("family"))
      .build();

  @Before
  public final void setupTestHBaseKijiRowData() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.READER_SCHEMA_TEST));
    mTable = getKiji().openTable(TABLE_NAME);

    final EmptyRecord emptyRecord = EmptyRecord.newBuilder().build();
    final TestRecord1 record1 = TestRecord1.newBuilder().setInteger(1).build();
    final TestRecord2 record2 =
        TestRecord2.newBuilder().setInteger(2).setText("record2").build();
    final TestRecord3 record3 =
        TestRecord3.newBuilder().setInteger(3).setAnotherText("record3").build();

    final EntityId eid = mTable.getEntityId("eid");

    final KijiTableWriter writer = mTable.openTableWriter();
    try {
      writer.put(eid, "family", "integer", 1L, 314);
      writer.put(eid, "family", "empty", 1L, emptyRecord);
      writer.put(eid, "family", "record1", 1L, record1);
      writer.put(eid, "family", "record2", 1L, record2);
      writer.put(eid, "family", "record3", 1L, record3);

      // TODO(): For now, we cannot write a record whose schema does not match exactly:
      // writer.put(eid, "family", "records", 1L, record1);
      // writer.put(eid, "family", "records", 2L, record2);
      writer.put(eid, "family", "records", 3L, record3);

    } finally {
      writer.close();
    }
  }

  @After
  public final void teardownTestHBaseKijiRowData() throws Exception {
    mTable.release();
    mTable = null;
  }

  // -----------------------------------------------------------------------------------------------

  /** Decode an EmptyRecord as a specific TestRecord1. */
  @Test
  public void testDecodeEmptyAsRecord1() throws Exception {
    final EntityId eid = mTable.getEntityId("eid");

    final KijiColumnName colEmpty = KijiColumnName.create("family", "empty");
    final Map<KijiColumnName, CellSpec> overrides =
        ImmutableMap.<KijiColumnName, CellSpec>builder()
            .put(colEmpty, mTable.getLayout().getCellSpec(colEmpty)
                .setReaderSchema(TestRecord1.SCHEMA$))
            .build();
    final KijiTableReader reader = mTable.getReaderFactory().openTableReader(overrides);
    try {
      final KijiRowData row = reader.get(eid, DATA_REQUEST);
      final TestRecord1 read = row.getMostRecentValue("family", "empty");
      // Field 'integer' does not exist in EmptyRecord and must be decoded as its default value:
      assertEquals(-1, (int) read.getInteger());
    } finally {
      reader.close();
    }
  }

  /** Decode a TestRecord1 as a specific TestRecord2 using CellSpec.setSpecificRecord(Class). */
  @Test
  public void testDecodeRecord1AsRecord2() throws Exception {
    final EntityId eid = mTable.getEntityId("eid");

    final KijiColumnName colEmpty = KijiColumnName.create("family", "record1");
    final Map<KijiColumnName, CellSpec> overrides =
        ImmutableMap.<KijiColumnName, CellSpec>builder()
            .put(colEmpty, mTable.getLayout().getCellSpec(colEmpty)
                .setSpecificRecord(TestRecord2.class))
            .build();
    final KijiTableReader reader = mTable.getReaderFactory().openTableReader(overrides);
    try {
      final KijiRowData row = reader.get(eid, DATA_REQUEST);
      final TestRecord2 read = row.getMostRecentValue("family", "record1");
      assertEquals(1, (int) read.getInteger());

      // Field 'text' does not exist in record1, so must be decoded using its default value:
      assertEquals("record2", read.getText());
    } finally {
      reader.close();
    }
  }

  /** Decode a TestRecord2 as a specific TestRecord1 using CellSpec.setReaderSchema(Schema). */
  @Test
  public void testDecodeRecord2AsRecord1() throws Exception {
    final EntityId eid = mTable.getEntityId("eid");

    final KijiColumnName colEmpty = KijiColumnName.create("family", "record2");
    final Map<KijiColumnName, CellSpec> overrides =
        ImmutableMap.<KijiColumnName, CellSpec>builder()
            .put(colEmpty, mTable.getLayout().getCellSpec(colEmpty)
                .setReaderSchema(TestRecord1.SCHEMA$))
            .build();
    final KijiTableReader reader = mTable.getReaderFactory().openTableReader(overrides);
    try {
      final KijiRowData row = reader.get(eid, DATA_REQUEST);
      final TestRecord1 read = row.getMostRecentValue("family", "record2");
      assertEquals(2, (int) read.getInteger());
    } finally {
      reader.close();
    }
  }

  /** Decode a TestRecord2 as a specific TestRecord3. */
  @Test
  public void testDecodeRecord2AsRecord3() throws Exception {
    final EntityId eid = mTable.getEntityId("eid");

    final KijiColumnName colEmpty = KijiColumnName.create("family", "record2");
    final Map<KijiColumnName, CellSpec> overrides =
        ImmutableMap.<KijiColumnName, CellSpec>builder()
            .put(colEmpty, mTable.getLayout().getCellSpec(colEmpty)
                .setSpecificRecord(TestRecord3.class))
            .build();
    final KijiTableReader reader = mTable.getReaderFactory().openTableReader(overrides);
    try {
      final KijiRowData row = reader.get(eid, DATA_REQUEST);
      final TestRecord3 read = row.getMostRecentValue("family", "record2");
      assertEquals(2, (int) read.getInteger());

      // Field 'text' from record2 is discarded, field 'another_text' has its default value:
      assertEquals("record3", read.getAnotherText());
    } finally {
      reader.close();
    }
  }

  /** Decode a TestRecord2 as a generic TestRecord3. */
  @Test
  public void testDecodeRecord2AsRecord3Generic() throws Exception {
    final EntityId eid = mTable.getEntityId("eid");

    final KijiColumnName colEmpty = KijiColumnName.create("family", "record2");
    final Map<KijiColumnName, CellSpec> overrides =
        ImmutableMap.<KijiColumnName, CellSpec>builder()
            .put(colEmpty, mTable.getLayout().getCellSpec(colEmpty)
                .setReaderSchema(TestRecord3.SCHEMA$)
                .setDecoderFactory(GenericCellDecoderFactory.get()))
            .build();
    final KijiTableReader reader = mTable.getReaderFactory().openTableReader(overrides);
    try {
      final KijiRowData row = reader.get(eid, DATA_REQUEST);
      final GenericData.Record read = row.getMostRecentValue("family", "record2");
      assertEquals(2, read.get("integer"));

      // Field 'text' from record2 is discarded, field 'another_text' has its default value:
      assertEquals("record3", read.get("another_text"));
    } finally {
      reader.close();
    }
  }

  /** Decode an integer as a long. */
  @Test
  public void testDecodeIntAsLong() throws Exception {
    final EntityId eid = mTable.getEntityId("eid");

    final KijiColumnName colEmpty = KijiColumnName.create("family", "integer");
    final Map<KijiColumnName, CellSpec> overrides =
        ImmutableMap.<KijiColumnName, CellSpec>builder()
            .put(colEmpty, mTable.getLayout().getCellSpec(colEmpty)
                .setReaderSchema(Schema.create(Schema.Type.LONG)))
            .build();
    final KijiTableReader reader = mTable.getReaderFactory().openTableReader(overrides);
    try {
      final KijiRowData row = reader.get(eid, DATA_REQUEST);
      final Long read = row.getMostRecentValue("family", "integer");
      assertEquals(314L, (long) read);

    } finally {
      reader.close();
    }
  }

  /** Decode an integer as a string should fail. */
  @Test
  public void testDecodeIntAsStringFails() throws Exception {
    final EntityId eid = mTable.getEntityId("eid");

    final KijiColumnName colEmpty = KijiColumnName.create("family", "integer");
    final Map<KijiColumnName, CellSpec> overrides =
        ImmutableMap.<KijiColumnName, CellSpec>builder()
            .put(colEmpty, mTable.getLayout().getCellSpec(colEmpty)
                .setReaderSchema(Schema.create(Schema.Type.STRING)))
            .build();
    final KijiTableReader reader = mTable.getReaderFactory().openTableReader(overrides);
    try {
      final KijiRowData row = reader.get(eid, DATA_REQUEST);
      try {
        final String read = row.getMostRecentValue("family", "integer");
        Assert.fail("Converting int to string must fail: got " + read);
      } catch (AvroTypeException ate) {
        // Expected
        assertTrue(ate.getMessage(),
            ate.getMessage().contains("Found int, expecting string"));
      }

    } finally {
      reader.close();
    }
  }

  /** Decode an int as TestRecord2 should fail. */
  @Test
  public void testDecodeIntAsRecord2Fails() throws Exception {
    final EntityId eid = mTable.getEntityId("eid");

    final KijiColumnName colEmpty = KijiColumnName.create("family", "integer");
    final Map<KijiColumnName, CellSpec> overrides =
        ImmutableMap.<KijiColumnName, CellSpec>builder()
            .put(colEmpty, mTable.getLayout().getCellSpec(colEmpty)
                .setReaderSchema(TestRecord2.SCHEMA$))
            .build();
    final KijiTableReader reader = mTable.getReaderFactory().openTableReader(overrides);
    try {
      final KijiRowData row = reader.get(eid, DATA_REQUEST);
      try {
        final TestRecord2 read = row.getMostRecentValue("family", "integer");
        Assert.fail("Converting int to string must fail: got " + read);
      } catch (AvroTypeException ate) {
        // Expected
        assertTrue(ate.getMessage(),
            ate.getMessage().contains("Found int, expecting org.kiji.schema.avro.TestRecord2"));
      }

    } finally {
      reader.close();
    }
  }

  /** Decode using the writer schema (this forces generic records). */
  @Test
  public void testDecodeWithWriterSchema() throws Exception {
    final EntityId eid = mTable.getEntityId("eid");

    final KijiColumnName colEmpty = KijiColumnName.create("family", "records");
    final Map<KijiColumnName, CellSpec> overrides =
        ImmutableMap.<KijiColumnName, CellSpec>builder()
            .put(colEmpty, mTable.getLayout().getCellSpec(colEmpty)
                .setUseWriterSchema())
            .build();
    final KijiTableReader reader = mTable.getReaderFactory().openTableReader(overrides);
    try {
      final KijiRowData row = reader.get(eid, DATA_REQUEST);
      final GenericData.Record read = row.getMostRecentValue("family", "records");
      assertEquals((Integer) 3, (Integer) read.get("integer"));

    } finally {
      reader.close();
    }
  }
}
