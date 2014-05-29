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

package org.kiji.schema.impl.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.impl.hbase.HBaseSchemaTable.PreRegisteredSchema;
import org.kiji.schema.util.BytesKey;

/** Tests for HBaseSchemaTable. */
public class TestHBaseSchemaTable extends KijiClientTest {
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

  /** Tests the basic functionalities and properties of the schema table. */
  @Test
  public void testBasicSchemaTableFunctions() throws Exception {
    final Kiji kiji = getKiji();
    final KijiSchemaTable schemaTable = kiji.getSchemaTable();

    // Schema ID 0 must be primitive type STRING.
    assertEquals(Schema.Type.STRING, schemaTable.getSchema(0L).getType());

    // Test looking up a schema by ID:
    assertEquals(SCHEMA_STRING, schemaTable.getSchema(0L));
    assertEquals(SCHEMA_BYTES, schemaTable.getSchema(1L));
    assertEquals(SCHEMA_INT, schemaTable.getSchema(2L));
    assertEquals(SCHEMA_LONG, schemaTable.getSchema(3L));
    assertEquals(SCHEMA_FLOAT, schemaTable.getSchema(4L));
    assertEquals(SCHEMA_DOUBLE, schemaTable.getSchema(5L));
    assertEquals(SCHEMA_BOOLEAN, schemaTable.getSchema(6L));
    assertEquals(SCHEMA_NULL, schemaTable.getSchema(7L));
    assertNull(schemaTable.getSchema(8L));

    // Test looking up a schema by hash:
    assertEquals(SCHEMA_STRING, schemaTable.getSchema(schemaTable.getSchemaHash(SCHEMA_STRING)));
    assertEquals(SCHEMA_BYTES, schemaTable.getSchema(schemaTable.getSchemaHash(SCHEMA_BYTES)));
    assertEquals(SCHEMA_INT, schemaTable.getSchema(schemaTable.getSchemaHash(SCHEMA_INT)));
    assertEquals(SCHEMA_LONG, schemaTable.getSchema(schemaTable.getSchemaHash(SCHEMA_LONG)));
    assertEquals(SCHEMA_FLOAT, schemaTable.getSchema(schemaTable.getSchemaHash(SCHEMA_FLOAT)));
    assertEquals(SCHEMA_DOUBLE, schemaTable.getSchema(schemaTable.getSchemaHash(SCHEMA_DOUBLE)));
    assertEquals(SCHEMA_BOOLEAN, schemaTable.getSchema(schemaTable.getSchemaHash(SCHEMA_BOOLEAN)));
    assertEquals(SCHEMA_NULL, schemaTable.getSchema(schemaTable.getSchemaHash(SCHEMA_NULL)));

    // There is no hash composed of a single byte 0:
    assertNull(schemaTable.getSchema(new BytesKey(new byte[]{0})));

    // Re-creating existing schemas are no-ops:
    assertEquals(
        PreRegisteredSchema.NULL.getSchemaId(),
        schemaTable.getOrCreateSchemaId(SCHEMA_NULL));
    assertEquals(
        PreRegisteredSchema.BOOLEAN.getSchemaId(),
        schemaTable.getOrCreateSchemaId(SCHEMA_BOOLEAN));
    assertEquals(
        PreRegisteredSchema.DOUBLE.getSchemaId(),
        schemaTable.getOrCreateSchemaId(SCHEMA_DOUBLE));
    assertEquals(
        PreRegisteredSchema.FLOAT.getSchemaId(),
        schemaTable.getOrCreateSchemaId(SCHEMA_FLOAT));
    assertEquals(
        PreRegisteredSchema.LONG.getSchemaId(),
        schemaTable.getOrCreateSchemaId(SCHEMA_LONG));
    assertEquals(
        PreRegisteredSchema.INT.getSchemaId(),
        schemaTable.getOrCreateSchemaId(SCHEMA_INT));
    assertEquals(
        PreRegisteredSchema.BYTES.getSchemaId(),
        schemaTable.getOrCreateSchemaId(SCHEMA_BYTES));
    assertEquals(
        PreRegisteredSchema.STRING.getSchemaId(),
        schemaTable.getOrCreateSchemaId(SCHEMA_STRING));

    // Check that none of the test schema are registered already:
    assertEquals(null, schemaTable.getSchema(schemaTable.getSchemaHash(TEST_SCHEMA_A)));
    assertEquals(null, schemaTable.getSchema(schemaTable.getSchemaHash(TEST_SCHEMA_B)));

    final long testSchemaAId = schemaTable.getOrCreateSchemaId(TEST_SCHEMA_A);
    Assert.assertEquals(HBaseSchemaTable.PRE_REGISTERED_SCHEMA_COUNT, testSchemaAId);
    assertEquals(testSchemaAId, schemaTable.getOrCreateSchemaId(TEST_SCHEMA_A));
    assertEquals(TEST_SCHEMA_A, schemaTable.getSchema(testSchemaAId));

    final long testSchemaBId = schemaTable.getOrCreateSchemaId(TEST_SCHEMA_B);
    assertEquals(testSchemaBId, schemaTable.getOrCreateSchemaId(TEST_SCHEMA_B));
    assertEquals(TEST_SCHEMA_B, schemaTable.getSchema(testSchemaBId));
    assertEquals(testSchemaAId + 1, testSchemaBId);
  }
}
