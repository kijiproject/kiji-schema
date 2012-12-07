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

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import org.kiji.schema.testutil.AbstractKijiIntegrationTest;

/** Tests for the HBase table implementation of KijiSchemaTable. */
public class IntegrationTestHBaseSchemaTable extends AbstractKijiIntegrationTest {

  private static final Schema SCHEMA_STRING = Schema.create(Schema.Type.STRING);
  private static final Schema SCHEMA_BYTES = Schema.create(Schema.Type.BYTES);
  private static final Schema SCHEMA_INT = Schema.create(Schema.Type.INT);
  private static final Schema SCHEMA_LONG = Schema.create(Schema.Type.LONG);
  private static final Schema SCHEMA_FLOAT = Schema.create(Schema.Type.FLOAT);
  private static final Schema SCHEMA_DOUBLE = Schema.create(Schema.Type.DOUBLE);
  private static final Schema SCHEMA_BOOLEAN = Schema.create(Schema.Type.BOOLEAN);

  private static final Schema TEST_SCHEMA_A = Schema.createMap(SCHEMA_STRING);
  private static final Schema TEST_SCHEMA_B = Schema.createArray(SCHEMA_STRING);

  /** Tests the basic functionalities and properties of the schema table. */
  @Test
  public void testBasicSchemaTableFunctions() throws IOException {
    final Kiji kiji = new Kiji(getKijiConfiguration());
    final KijiSchemaTable schemaTable = kiji.getSchemaTable();

    // Schema ID 0 must be primitive type STRING.
    assertEquals(Schema.Type.STRING, schemaTable.getSchema(0).getType());

    assertEquals(6, schemaTable.getOrCreateSchemaId(SCHEMA_BOOLEAN));
    assertEquals(5, schemaTable.getOrCreateSchemaId(SCHEMA_DOUBLE));
    assertEquals(4, schemaTable.getOrCreateSchemaId(SCHEMA_FLOAT));
    assertEquals(3, schemaTable.getOrCreateSchemaId(SCHEMA_LONG));
    assertEquals(2, schemaTable.getOrCreateSchemaId(SCHEMA_INT));
    assertEquals(1, schemaTable.getOrCreateSchemaId(SCHEMA_BYTES));
    assertEquals(0, schemaTable.getOrCreateSchemaId(SCHEMA_STRING));

    // Check that none of the test schema are registered already:
    assertEquals(null, schemaTable.getSchema(schemaTable.getSchemaHash(TEST_SCHEMA_A)));
    assertEquals(null, schemaTable.getSchema(schemaTable.getSchemaHash(TEST_SCHEMA_B)));

    final long testSchemaAId = schemaTable.getOrCreateSchemaId(TEST_SCHEMA_A);
    assertEquals(testSchemaAId, schemaTable.getOrCreateSchemaId(TEST_SCHEMA_A));
    assertEquals(TEST_SCHEMA_A, schemaTable.getSchema(testSchemaAId));

    final long testSchemaBId = schemaTable.getOrCreateSchemaId(TEST_SCHEMA_B);
    assertEquals(testSchemaBId, schemaTable.getOrCreateSchemaId(TEST_SCHEMA_B));
    assertEquals(TEST_SCHEMA_B, schemaTable.getSchema(testSchemaBId));
    assertEquals(testSchemaAId + 1, testSchemaBId);

    schemaTable.close();
    IOUtils.closeQuietly(kiji);
  }
}
