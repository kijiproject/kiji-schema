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

package org.kiji.schema.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestCassandraKijiRowDataModifyLayout {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestCassandraKijiRowDataModifyLayout.class);

  /** Test layout. */
  public static final String TEST_LAYOUT_V1 =
      "org/kiji/schema/layout/TestHBaseKijiRowData.test-layout-v1.json";

  /** Update for TEST_LAYOUT, with Test layout with column "family:qual0" removed. */
  public static final String TEST_LAYOUT_V2 =
      "org/kiji/schema/layout/TestHBaseKijiRowData.test-layout-v2.json";

  /** Layout for table 'writer_schema' to test when a column class is not found. */
  public static final String WRITER_SCHEMA_TEST =
      "org/kiji/schema/layout/TestHBaseKijiRowData.writer-schema.json";

  /** Test layout with version layout-1.3. */
  public static final String TEST_LAYOUT_V1_3 =
      "org/kiji/schema/layout/TestHBaseKijiRowData.layout-v1.3.json";

  private static final String TABLE_NAME = "row_data_test_table";

  private static final CassandraKijiClientTest CLIENT_TEST_DELEGATE = new CassandraKijiClientTest();
  private static Kiji mKiji;

  // Create a shared Kiji for everyone.
  @BeforeClass
  public static void initShared() throws Exception {
    CLIENT_TEST_DELEGATE.setupKijiTest();
    mKiji = CLIENT_TEST_DELEGATE.getKiji();
  }

  @AfterClass
  public static void tearDownShared() throws Exception {
    CLIENT_TEST_DELEGATE.tearDownKijiTest();
  }

  // Tests for KijiRowData.getReaderSchema() with layout-1.3 tables.
  @Test
  public void testGetReaderSchemaLayout13() throws Exception {
    final Kiji kiji = new InstanceBuilder(mKiji)
        .withTable(KijiTableLayouts.getLayout(TEST_LAYOUT_V1_3))
        .build();
    final KijiTable table = kiji.openTable("table");
    try {
      final KijiTableReader reader = table.getReaderFactory().openTableReader();
      try {
        final EntityId eid = table.getEntityId("row");
        final KijiDataRequest dataRequest = KijiDataRequest.builder()
            .addColumns(ColumnsDef.create().addFamily("family"))
            .build();
        final KijiRowData row = reader.get(eid, dataRequest);
        assertEquals(
            Schema.Type.STRING,
            row.getReaderSchema("family", "qual0").getType());

      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }

  // Tests that reading an entire family with a column that has been deleted works.
  @Test
  public void testReadDeletedColumns() throws Exception {
    // Create a separate Kiji here to avoid stepping on the one used elsewhere.
    mKiji.createTable(KijiTableLayouts.getLayout(TEST_LAYOUT_V1));
    KijiTable table = mKiji.openTable(TABLE_NAME);
    try {
      new InstanceBuilder(mKiji)
          .withTable(table)
          .withRow("row1")
          .withFamily("family")
          .withQualifier("qual0").withValue(1L, "string1")
          .withQualifier("qual0").withValue(2L, "string2")
          .build();

      final TableLayoutDesc update = KijiTableLayouts.getLayout(TEST_LAYOUT_V2);
      update.setReferenceLayout(table.getLayout().getDesc().getLayoutId());
      mKiji.modifyTableLayout(update);

      final KijiDataRequest dataRequest = KijiDataRequest.builder()
          .addColumns(ColumnsDef.create().addFamily("family"))
          .build();

      final KijiTableReader reader = table.openTableReader();
      try {
        final KijiRowData row1 = reader.get(table.getEntityId("row1"), dataRequest);
        assertTrue(row1.getValues("family", "qual0").isEmpty());
      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }

  //    Tests that we can read a record using the writer schema.
  //    This tests the case when a specific record class is not found on the classpath.
  //    However, this behavior is bogus. The reader schema should not be tied to the classes
  //    available on the classpath.
  //
  //    TODO(SCHEMA-295) the user may force using the writer schemas by overriding the
  //        declared reader schemas. This test will be updated accordingly.
  @Test
  public void testWSchemaWhenSpecRecClassNF() throws Exception {
    final Kiji kiji = mKiji;
    kiji.createTable(KijiTableLayouts.getLayout(WRITER_SCHEMA_TEST));
    final KijiTable table = kiji.openTable("writer_schema");
    try {
      // Write a (generic) record:
      final Schema writerSchema = Schema.createRecord("Found", null, "class.not", false);
      writerSchema.setFields(Lists.newArrayList(
          new Field("field", Schema.create(Schema.Type.STRING), null, null)));

      final KijiTableWriter writer = table.openTableWriter();
      try {
        final GenericData.Record record = new GenericRecordBuilder(writerSchema)
            .set("field", "value")
            .build();
        writer.put(table.getEntityId("eid"), "family", "qualifier", 1L, record);

      } finally {
        writer.close();
      }

      // Read the record back (should be a generic record):
      final KijiTableReader reader = table.openTableReader();
      try {
        final KijiDataRequest dataRequest = KijiDataRequest.builder()
            .addColumns(ColumnsDef.create().add("family", "qualifier"))
            .build();
        final KijiRowData row = reader.get(table.getEntityId("eid"), dataRequest);
        final GenericData.Record record = row.getValue("family", "qualifier", 1L);
        assertEquals(writerSchema, record.getSchema());
        assertEquals("value", record.get("field").toString());
      } finally {
        reader.close();
      }

    } finally {
      table.release();
    }
  }
}
