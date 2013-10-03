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
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
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
import org.kiji.schema.avro.TestRecord2;
import org.kiji.schema.avro.TestRecord3;
import org.kiji.schema.layout.InvalidLayoutSchemaException;
import org.kiji.schema.layout.KijiTableLayouts;

/**
 * Tests layout validation in developer mode.
 */
public class TestDeveloperValidation extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestDeveloperValidation.class);

  public static final String LAYOUT_DEVELOPER =
      "org/kiji/schema/layout/layout-developer.json";

  private static final Schema SCHEMA_STRING = Schema.create(Schema.Type.STRING);
  private static final Schema SCHEMA_BYTES = Schema.create(Schema.Type.BYTES);
  private static final Schema SCHEMA_INT = Schema.create(Schema.Type.INT);
  private static final Schema SCHEMA_LONG = Schema.create(Schema.Type.LONG);
  private static final Schema SCHEMA_FLOAT = Schema.create(Schema.Type.FLOAT);
  private static final Schema SCHEMA_DOUBLE = Schema.create(Schema.Type.DOUBLE);
  private static final Schema SCHEMA_BOOLEAN = Schema.create(Schema.Type.BOOLEAN);
  private static final Schema SCHEMA_NULL = Schema.create(Schema.Type.NULL);

  private static final Schema TEST_SCHEMA_A = Schema.createMap(SCHEMA_STRING);
  private static final Schema TEST_SCHEMA_B = Schema.createArray(SCHEMA_STRING);

  private Kiji openSystem2Kiji() throws IOException {
    final Kiji kiji = getKiji();
    kiji.getSystemTable().setDataVersion(Versions.SYSTEM_2_0);
    return Kiji.Factory.open(kiji.getURI());
  }

  /** Tests writer schema registration with no reader schema, ie. no constraint. */
  @Test
  public void testNoReaderSchema() throws Exception {
    final Kiji kiji = openSystem2Kiji();
    try {
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
            table.getLayout().getCellSchema(new KijiColumnName("info:user_id")).getWriters();
        Assert.assertEquals(expectedIds, writerSchemaIds);

        final List<AvroSchema> writtenSchemaIds =
            table.getLayout().getCellSchema(new KijiColumnName("info:user_id")).getWritten();
        Assert.assertEquals(expectedIds, writtenSchemaIds);

      } finally {
        table.release();
      }
    } finally {
      kiji.release();
    }
  }

  /** Tests writer schema compatibility with a LONG reader schema. */
  @Test
  public void testReaderSchemaLong() throws Exception {
    final Kiji kiji = openSystem2Kiji();
    try {
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
    } finally {
      kiji.release();
    }
  }

  /** Tests writer schema compatibility with record schemas. */
  @Test
  public void testReaderSchemaTestRecord2() throws Exception {
    final Kiji kiji = openSystem2Kiji();
    try {
      final TableLayoutDesc desc = KijiTableLayouts.getLayout(LAYOUT_DEVELOPER);
      final CellSchema cellSchema = desc.getLocalityGroups().get(0)
          .getFamilies().get(0)
          .getColumns().get(0)
          .getColumnSchema();
      cellSchema.setReaders(Lists.newArrayList(
          AvroSchema.newBuilder()
              .setUid(kiji.getSchemaTable().getOrCreateSchemaId(TestRecord2.SCHEMA$))
              .build()));

      kiji.createTable(desc);
      final KijiTable table = kiji.openTable("dev");
      try {
        final EntityId eid = table.getEntityId("row");
        final KijiTableWriter writer = table.getWriterFactory().openTableWriter();
        try {
          // Register writer schema TestRecord1, compatible with reader TestRecord2:
          final TestRecord1 record1 = TestRecord1.newBuilder().setInteger(314).build();
          writer.put(eid, "info", "user_id", record1);

          // Register writer schema TestRecord2, is exactly reader TestRecord2:
          final TestRecord2 record2 = TestRecord2.newBuilder()
              .setInteger(314)
              .setText("text")
              .build();
          writer.put(eid, "info", "user_id", record2);

          // Register writer schema TestRecord3, compatible with reader TestRecord2:
          final TestRecord3 record3 = TestRecord3.newBuilder()
              .setInteger(314)
              .setAnotherText("text")
              .build();
          writer.put(eid, "info", "user_id", record3);

          // Any primitive type is incompatible with reader schema TestRecord1:
          try {
            writer.put(eid, "info", "user_id", "1");
            Assert.fail("Registering writer schema 'string' should fail.");
          } catch (InvalidLayoutSchemaException ilse) {
            LOG.info("Expected error: {}", ilse.toString());
            Assert.assertTrue(ilse.toString().contains(
                "is incompatible with writer schema: \"string\""));
          }

        } finally {
          writer.close();
        }

      } finally {
        table.release();
      }
    } finally {
      kiji.release();
    }
  }

}
