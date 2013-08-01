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

package org.kiji.schema.layout.impl;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.junit.Test;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.avro.AvroValidationPolicy;
import org.kiji.schema.avro.EmptyRecord;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.avro.TestRecord1;
import org.kiji.schema.layout.InvalidLayoutSchemaException;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.TableLayoutBuilder;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;
import org.kiji.schema.util.ProtocolVersion;

public class IntegrationTestTableLayoutUpdateValidator extends AbstractKijiIntegrationTest {

  /** Schema containing a single String field. */
  private static final Schema STRING_SCHEMA = Schema.create(Type.STRING);
  /**
   * Schema containing a single Int field. This will not be readable by the registered String
   * schema from the previous layout because Int and String are incompatible types.
   */
  private static final Schema INT_SCHEMA = Schema.create(Type.INT);
  /** Schema containing no fields. */
  private static final Schema EMPTY_SCHEMA = EmptyRecord.SCHEMA$;
  /**
   * Schema containing an optional Int field. This will be readable by the registered Empty schema
   * from the previous layout because all fields are optional.  The old reader schema will read new
   * records written with this schema as empty.
   */
  private static final Schema OPTIONAL_INT_SCHEMA = TestRecord1.SCHEMA$;

  private void setSystemVersion2() throws IOException {
    final Kiji kiji = Kiji.Factory.open(getKijiURI());
    try {
      kiji.getSystemTable().setDataVersion(ProtocolVersion.parse("system-2.0"));
    } finally {
      kiji.release();
    }
  }

  /**
   * Creates a table with a string reader schema and the given validation policy
   * then builds a new TableLayoutDesc with an int writer schema.
   *
   * @param kiji the Kiji instance in which to create the table.
   * @param policy the AvroValidationPolicy to enforce on the test column.
   * @return a new TableLayoutDesc with with its reference layout set to the old string-reader
   *     layout and a new int-writer schema.
   * @throws IOException
   */
  private TableLayoutDesc prepareNewDesc(Kiji kiji, AvroValidationPolicy policy)
      throws IOException {

    final TableLayoutDesc desc = KijiTableLayouts.getLayout(KijiTableLayouts.SCHEMA_REG_TEST);
    desc.setVersion("layout-1.3.0");

    final TableLayoutDesc originalDesc = new TableLayoutBuilder(desc, kiji)
        .withAvroValidationPolicy(
            new KijiColumnName("info:fullname"), policy)
        .withReader(new KijiColumnName("info:fullname"), STRING_SCHEMA)
        .build();

    kiji.createTable(originalDesc);

    final TableLayoutDesc newDesc = new TableLayoutBuilder(originalDesc, kiji)
        .withWriter(new KijiColumnName("info:fullname"), INT_SCHEMA).build();
    newDesc.setReferenceLayout("1");
    newDesc.setLayoutId("2");

    return newDesc;
  }

  @Test
  public void testValidLayoutUpdate() throws IOException {
    setSystemVersion2();

    final Kiji kiji = Kiji.Factory.open(getKijiURI());
    try {
      final TableLayoutDesc desc = KijiTableLayouts.getLayout(KijiTableLayouts.SCHEMA_REG_TEST);
      desc.setVersion("layout-1.3.0");


      final TableLayoutDesc originalDesc = new TableLayoutBuilder(desc, kiji)
          .withAvroValidationPolicy(
              new KijiColumnName("info:fullname"), AvroValidationPolicy.STRICT)
          .withReader(new KijiColumnName("info:fullname"), EMPTY_SCHEMA)
          .build();

      kiji.createTable(originalDesc);

      final TableLayoutDesc newDesc = new TableLayoutBuilder(originalDesc, kiji)
          .withWriter(new KijiColumnName("info:fullname"), OPTIONAL_INT_SCHEMA)
          .build();
      newDesc.setReferenceLayout("1");
      newDesc.setLayoutId("2");

      kiji.modifyTableLayout(newDesc);
    } finally {
      kiji.release();
    }
  }

  @Test
  public void testStrictValidation() throws IOException {
    setSystemVersion2();

    Kiji kiji = Kiji.Factory.open(getKijiURI());
    try {
      // Strict validation should fail.
      final TableLayoutDesc strictDesc = prepareNewDesc(kiji, AvroValidationPolicy.STRICT);
      try {
        kiji.modifyTableLayout(strictDesc);
        fail("Should have thrown an InvalidLayoutSchemaException because int and string are "
            + "incompatible.");
      } catch (InvalidLayoutSchemaException ilse) {
        assertTrue(ilse.getReasons().contains(
            "In column: 'info:fullname' Reader schema: \"string\" is incompatible with "
            + "writer schema: \"int\"."));
        assertTrue(ilse.getReasons().size() == 1);
      }
    } finally {
      kiji.release();
    }
  }

  @Test
  public void testDeveloperValidation() throws IOException {
    setSystemVersion2();

    Kiji kiji = Kiji.Factory.open(getKijiURI());
    try {
      // Developer validation should fail.
      final TableLayoutDesc developerDesc = prepareNewDesc(kiji, AvroValidationPolicy.DEVELOPER);
      try {
        kiji.modifyTableLayout(developerDesc);
        fail("Should have thrown an InvalidLayoutSchemaException because int and string are "
            + "incompatible.");
      } catch (InvalidLayoutSchemaException ilse) {
        assertTrue(ilse.getReasons().contains(
            "In column: 'info:fullname' Reader schema: \"string\" is incompatible with "
            + "writer schema: \"int\"."));
        assertTrue(ilse.getReasons().size() == 1);
      }
    } finally {
      kiji.release();
    }
  }

  @Test
  public void testNoneValidation() throws IOException {
    setSystemVersion2();

    Kiji kiji = Kiji.Factory.open(getKijiURI());
    try {
      // None validation should pass despite the incompatible change.
      final TableLayoutDesc noneDesc = prepareNewDesc(kiji, AvroValidationPolicy.NONE);
      kiji.modifyTableLayout(noneDesc);
    } finally {
      kiji.release();
    }
  }

  @Test
  public void testSchema10Validation() throws IOException {
    setSystemVersion2();

    Kiji kiji = Kiji.Factory.open(getKijiURI());
    try {
      // Schema-1.0 validation should pass despite incompatible changes.
      final TableLayoutDesc schema10Desc = prepareNewDesc(kiji, AvroValidationPolicy.SCHEMA_1_0);
      kiji.modifyTableLayout(schema10Desc);
    } finally {
      kiji.release();
    }
  }
}
