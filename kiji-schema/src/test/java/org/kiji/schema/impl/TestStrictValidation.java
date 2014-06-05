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

import java.util.List;

import com.google.common.collect.ImmutableList;
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
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.InvalidLayoutSchemaException;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.TableLayoutBuilder;

/**
 * Tests layout validation in strict mode.
 */
public class TestStrictValidation extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestStrictValidation.class);

  public static final String LAYOUT_UNION_WRITER =
      "org/kiji/schema/layout/TestStrictValidation.union-writer.json";

  public static final String LAYOUT_ENUM =
      "org/kiji/schema/layout/TestStrictValidation.enum.json";

  private static final String TABLE_NAME = "table";

  private static final Schema SCHEMA_STRING = Schema.create(Schema.Type.STRING);
  private static final Schema SCHEMA_NULL = Schema.create(Schema.Type.NULL);

  /** Tests expansion of a writer schema union { null, string }. */
  @Test
  public void testUnionWriterSchema() throws Exception {
    final Kiji kiji = getKiji();
    kiji.createTable(KijiTableLayouts.getLayout(LAYOUT_UNION_WRITER));
    final KijiTable table = kiji.openTable(TABLE_NAME);
    try {
      final EntityId eid = table.getEntityId("row");
      final KijiTableWriter writer = table.getWriterFactory().openTableWriter();
      try {
        // Writing naked type "null" should work:
        writer.put(eid, "info", "user", null);

        // Writing naked type "string" should work too:
        writer.put(eid, "info", "user", "a string");

        final Schema unionNullString =
            Schema.createUnion(Lists.newArrayList(SCHEMA_NULL, SCHEMA_STRING));
        final List<AvroSchema> expectedIds = Lists.newArrayList(
            AvroSchema.newBuilder()
                .setUid(kiji.getSchemaTable().getOrCreateSchemaId(unionNullString))
                .build());

        final List<AvroSchema> writerSchemaIds =
            table.getLayout().getCellSchema(KijiColumnName.create("info:user")).getWriters();
        Assert.assertEquals(expectedIds, writerSchemaIds);

        final List<AvroSchema> writtenSchemaIds =
            table.getLayout().getCellSchema(KijiColumnName.create("info:user")).getWritten();
        Assert.assertEquals(expectedIds, writtenSchemaIds);

      } finally {
        writer.close();
      }
    } finally {
      table.release();
    }
  }

  /** Tests behavior of Avro enums. */
  @Test
  public void testEnumSchema() throws Exception {
    final Kiji kiji = getKiji();
    kiji.createTable(KijiTableLayouts.getLayout(LAYOUT_ENUM));

    final Schema readerEnum =
        Schema.createEnum("Gender", null, null, ImmutableList.of("MALE", "FEMALE"));
    final Schema writerEnum =
        Schema.createEnum("Gender", null, null, ImmutableList.of("MALE", "FEMALE", "BOTH"));

    final KijiTable table = kiji.openTable(TABLE_NAME);
    try {

      final TableLayoutDesc updateWithReader =
          new TableLayoutBuilder(table.getLayout().getDesc(), kiji)
              .withReader(KijiColumnName.create("info",  "gender"), readerEnum)
              .build();
      LOG.debug("Applying update with enum reader: %s", updateWithReader);
      kiji.modifyTableLayout(updateWithReader);

      final TableLayoutDesc updateWithWriter=
          new TableLayoutBuilder(table.getLayout().getDesc(), kiji)
              .withWriter(KijiColumnName.create("info",  "gender"), writerEnum)
              .build();
      LOG.debug("Applying update with enum writer: %s", updateWithWriter);
      try {
        kiji.modifyTableLayout(updateWithWriter);
        Assert.fail();
      } catch (InvalidLayoutSchemaException ilse) {
        LOG.debug("Expected error: {}", ilse.getMessage());
        Assert.assertTrue(
            ilse.getMessage(),
            ilse.getMessage().contains("In column: 'info:gender'"));
        Assert.assertTrue(
            ilse.getMessage(),
            ilse.getMessage().contains("is incompatible with writer schema"));
      }

    } finally {
      table.release();
    }
  }

}
