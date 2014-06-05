/**
 * (c) Copyright 2013 WibiData, Inc.
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

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.TextNode;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.avro.AvroSchema;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.avro.TestRecord1;
import org.kiji.schema.layout.InvalidLayoutSchemaException;
import org.kiji.schema.layout.KijiTableLayouts;

/**
 * Tests layout validation in developer mode.
 */
public class TestDeveloperValidation extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestDeveloperValidation.class);

  public static final String LAYOUT_DEVELOPER =
      "org/kiji/schema/layout/layout-developer.json";

  private static final Schema SCHEMA_STRING = Schema.create(Type.STRING);
  private static final Schema INT_SCHEMA = Schema.create(Type.INT);
  private static final Schema SCHEMA_LONG = Schema.create(Type.LONG);

  private static final Schema EMPTY_RECORD1 =
      Schema.createRecord("Record1", null, null, false);
  private static final Schema INT_RECORD1 =
      Schema.createRecord("Record1", null, null, false);
  private static final Schema INT_TEXT_RECORD1 =
      Schema.createRecord("Record1", null, null, false);
  private static final Schema INT_ANOTHER_TEXT_RECORD1 =
      Schema.createRecord("Record1", null, null, false);
  static {
    EMPTY_RECORD1.setFields(Collections.<Field>emptyList());
    INT_RECORD1.setFields(Lists.newArrayList(
        new Field("integer", INT_SCHEMA, null, IntNode.valueOf(-1))));
    INT_TEXT_RECORD1.setFields(Lists.newArrayList(
        new Field("integer", INT_SCHEMA, null, IntNode.valueOf(-2)),
        new Field("text", SCHEMA_STRING, null, TextNode.valueOf("record2"))));
    INT_ANOTHER_TEXT_RECORD1.setFields(Lists.newArrayList(
        new Field("integer", INT_SCHEMA, null, IntNode.valueOf(-3)),
        new Field("another_text", SCHEMA_STRING, null, TextNode.valueOf("record3"))));
  }

  /** Tests writer schema registration with no reader schema, ie. no constraint. */
  @Test
  public void testNoReaderSchema() throws Exception {
    final Kiji kiji = getKiji();
    kiji.createTable(KijiTableLayouts.getLayout(LAYOUT_DEVELOPER));
    final KijiTable table = kiji.openTable("dev");
    try {
      final EntityId eid = table.getEntityId("row");
      final KijiTableWriter writer = table.getWriterFactory().openTableWriter();
      try {
        // Registers writer schema 'long':
        writer.put(eid, "info", "user_id", (Long) 1L);

        // Should not need to register writer schema 'long':
        {
          final String layoutIdBefore = table.getLayout().getDesc().getLayoutId();
          writer.put(eid, "info", "user_id", (Long) 2L);
          final String layoutIdAfter = table.getLayout().getDesc().getLayoutId();
          Assert.assertEquals(layoutIdBefore, layoutIdAfter);
        }

        // Register writer schema 'string':
        writer.put(eid, "info", "user_id", "string");

        // Register writer schema for TestRecord1:
        final TestRecord1 record1 = TestRecord1.newBuilder().setInteger(314).build();
        writer.put(eid, "info", "user_id", record1);
      } finally {
        writer.close();
      }

      final List<AvroSchema> expectedIds = Lists.newArrayList(
          AvroSchema.newBuilder()
              .setUid(kiji.getSchemaTable().getOrCreateSchemaId(SCHEMA_LONG))
              .build(),
          AvroSchema.newBuilder()
              .setUid(kiji.getSchemaTable().getOrCreateSchemaId(SCHEMA_STRING))
              .build(),
          AvroSchema.newBuilder()
              .setUid(kiji.getSchemaTable().getOrCreateSchemaId(TestRecord1.SCHEMA$))
              .build());

      final List<AvroSchema> writerSchemaIds =
          table.getLayout().getCellSchema(KijiColumnName.create("info:user_id")).getWriters();
      Assert.assertEquals(expectedIds, writerSchemaIds);

      final List<AvroSchema> writtenSchemaIds =
          table.getLayout().getCellSchema(KijiColumnName.create("info:user_id")).getWritten();
      Assert.assertEquals(expectedIds, writtenSchemaIds);

    } finally {
      table.release();
    }
  }

  /** Tests writer schema compatibility with a LONG reader schema. */
  @Test
  public void testReaderSchemaLong() throws Exception {
    final Kiji kiji = getKiji();
    final TableLayoutDesc desc = KijiTableLayouts.getLayout(LAYOUT_DEVELOPER);
    final CellSchema cellSchema = desc.getLocalityGroups().get(0)
        .getFamilies().get(0)
        .getColumns().get(0)
        .getColumnSchema();
    cellSchema.setReaders(Lists.newArrayList(
        AvroSchema.newBuilder()
            .setUid(kiji.getSchemaTable().getOrCreateSchemaId(SCHEMA_LONG))
            .build()));

    kiji.createTable(desc);
    final KijiTable table = kiji.openTable("dev");
    try {
      final EntityId eid = table.getEntityId("row");
      final KijiTableWriter writer = table.getWriterFactory().openTableWriter();
      try {
        // Registering 'long' as a writer schema: compatible with 'long' reader schema:
        writer.put(eid, "info", "user_id", (Long) 1L);

        // Should not require additional registration:
        {
          final String layoutIdBefore = table.getLayout().getDesc().getLayoutId();
          writer.put(eid, "info", "user_id", (Long) 2L);
          final String layoutIdAfter = table.getLayout().getDesc().getLayoutId();
          Assert.assertEquals(layoutIdBefore, layoutIdAfter);
        }

        // Registering 'int' as a writer schema: compatible with 'long' reader schema:
        writer.put(eid, "info", "user_id", (Integer) 1);

        // Register writer schema 'string' must fail: incompatible with 'long':
        try {
          writer.put(eid, "info", "user_id", "1");
          Assert.fail("Registering writer schema 'string' should fail.");
        } catch (InvalidLayoutSchemaException ilse) {
          LOG.info("Expected error: {}", ilse.toString());
          Assert.assertTrue(ilse.toString().contains(
              "column: 'info:user_id' "
              + "Reader schema: \"long\" is incompatible with writer schema: \"string\""));
        }
      } finally {
        writer.close();
      }

    } finally {
      table.release();
    }
  }

  /** Tests writer schema compatibility with record schemas. */
  @Test
  public void testReaderSchemaTestRecord2() throws Exception {
    final Kiji kiji = getKiji();
    final TableLayoutDesc desc = KijiTableLayouts.getLayout(LAYOUT_DEVELOPER);
    final CellSchema cellSchema = desc.getLocalityGroups().get(0)
        .getFamilies().get(0)
        .getColumns().get(0)
        .getColumnSchema();
    cellSchema.setReaders(Lists.newArrayList(
        AvroSchema.newBuilder()
            .setUid(kiji.getSchemaTable().getOrCreateSchemaId(INT_TEXT_RECORD1))
            .build()));

    kiji.createTable(desc);
    final KijiTable table = kiji.openTable("dev");
    try {
      final EntityId eid = table.getEntityId("row");
      final KijiTableWriter writer = table.getWriterFactory().openTableWriter();
      try {
        // Register writer schema TestRecord1, compatible with reader TestRecord2:
        final GenericData.Record record1 = new GenericData.Record(INT_RECORD1);
        record1.put("integer", 314);
        writer.put(eid, "info", "user_id", record1);

        // Register writer schema TestRecord2, is exactly reader TestRecord2:
        final GenericData.Record record2 = new GenericData.Record(INT_TEXT_RECORD1);
        record2.put("integer", 314);
        record2.put("text", "text");
        writer.put(eid, "info", "user_id", record2);

        // Register writer schema TestRecord3, compatible with reader TestRecord2:
        final GenericData.Record record3 = new GenericData.Record(INT_ANOTHER_TEXT_RECORD1);
        record3.put("integer", 314);
        record3.put("another_text", "text");
        writer.put(eid, "info", "user_id", record3);

        // Any primitive type is incompatible with reader schema TestRecord1:
        try {
          writer.put(eid, "info", "user_id", "1");
          Assert.fail("Registering writer schema 'string' should fail.");
        } catch (InvalidLayoutSchemaException ilse) {
          LOG.info("Expected error: {}", ilse.toString());
          Assert.assertTrue(
              ilse.getMessage(),
              ilse.getMessage().contains("In column: 'info:user_id'"));
          Assert.assertTrue(
              ilse.getMessage(),
              ilse.getMessage().contains("is incompatible with writer schema"));
        }

      } finally {
        writer.close();
      }

    } finally {
      table.release();
    }
  }
}
