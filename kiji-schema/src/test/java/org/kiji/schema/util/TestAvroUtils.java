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

package org.kiji.schema.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.codehaus.jackson.node.IntNode;
import org.junit.Test;

import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.avro.AvroSchema;

public class TestAvroUtils extends KijiClientTest {
  private static final Schema INT_SCHEMA = Schema.create(Schema.Type.INT);
  private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);
  private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);
  private static final Schema ARRAY_SCHEMA = Schema.createArray(STRING_SCHEMA);

  private static final Schema WRITER_SCHEMA = Schema.createRecord(Lists.newArrayList(
      new Schema.Field("oldfield1", INT_SCHEMA, null, null),
      new Schema.Field("oldfield2", STRING_SCHEMA, null, null)));

  @Test
  public void testGetOptionalType() throws Exception {
    final List<Schema> unionSchemas = Lists.newArrayList(
        INT_SCHEMA,
        NULL_SCHEMA);
    final Schema optionalSchema = Schema.createUnion(unionSchemas);
    final Schema optionalReverseSchema = Schema.createUnion(Lists.reverse(unionSchemas));

    // Ensure that the optional type is retrievable.
    assertEquals(INT_SCHEMA, AvroUtils.getOptionalType(optionalSchema));
    assertEquals(INT_SCHEMA, AvroUtils.getOptionalType(optionalReverseSchema));
  }

  @Test
  public void testGetNonOptionalType() throws Exception {
    final List<Schema> unionSchemas = Lists.newArrayList(
        INT_SCHEMA,
        STRING_SCHEMA,
        NULL_SCHEMA);
    final Schema nonOptionalSchema = Schema.createUnion(unionSchemas);

    // Ensure that null gets returned when the schema provided isn't an optional type.
    assertEquals(null, AvroUtils.getOptionalType(nonOptionalSchema));
  }

  @Test
  public void testValidateSchemaPairMissingField() throws Exception {
    final List<Schema.Field> readerFields = Lists.newArrayList(
        new Schema.Field("oldfield1", INT_SCHEMA, null, null));
    final Schema reader = Schema.createRecord(readerFields);
    final AvroUtils.SchemaPairCompatibility expectedResult =
        new AvroUtils.SchemaPairCompatibility(
            AvroUtils.SchemaCompatibilityType.COMPATIBLE,
            reader,
            WRITER_SCHEMA,
            "Schemas match");

    // Test omitting a field.
    assertEquals(expectedResult, AvroUtils.checkReaderWriterCompatibility(reader, WRITER_SCHEMA));
  }

  @Test
  public void testValidateSchemaPairMissingSecondField() throws Exception {
    final List<Schema.Field> readerFields = Lists.newArrayList(
        new Schema.Field("oldfield2", STRING_SCHEMA, null, null));
    final Schema reader = Schema.createRecord(readerFields);
    final AvroUtils.SchemaPairCompatibility expectedResult =
        new AvroUtils.SchemaPairCompatibility(
            AvroUtils.SchemaCompatibilityType.COMPATIBLE,
            reader,
            WRITER_SCHEMA,
            "Schemas match");

    // Test omitting other field.
    assertEquals(expectedResult, AvroUtils.checkReaderWriterCompatibility(reader, WRITER_SCHEMA));
  }

  @Test
  public void testValidateSchemaPairAllFields() throws Exception {
    final List<Schema.Field> readerFields = Lists.newArrayList(
        new Schema.Field("oldfield1", INT_SCHEMA, null, null),
        new Schema.Field("oldfield2", STRING_SCHEMA, null, null));
    final Schema reader = Schema.createRecord(readerFields);
    final AvroUtils.SchemaPairCompatibility expectedResult =
        new AvroUtils.SchemaPairCompatibility(
            AvroUtils.SchemaCompatibilityType.COMPATIBLE,
            reader,
            WRITER_SCHEMA,
            "Schemas match");

    // Test with all fields.
    assertEquals(expectedResult, AvroUtils.checkReaderWriterCompatibility(reader, WRITER_SCHEMA));
  }

  @Test
  public void testValidateSchemaNewFieldWithDefault() throws Exception {
    final List<Schema.Field> readerFields = Lists.newArrayList(
        new Schema.Field("oldfield1", INT_SCHEMA, null, null),
        new Schema.Field("newfield1", INT_SCHEMA, null, IntNode.valueOf(42)));
    final Schema reader = Schema.createRecord(readerFields);
    final AvroUtils.SchemaPairCompatibility expectedResult =
        new AvroUtils.SchemaPairCompatibility(
            AvroUtils.SchemaCompatibilityType.COMPATIBLE,
            reader,
            WRITER_SCHEMA,
            "Schemas match");

    // Test new field with default value.
    assertEquals(expectedResult, AvroUtils.checkReaderWriterCompatibility(reader, WRITER_SCHEMA));
  }

  @Test
  public void testValidateSchemaNewField() throws Exception {
    final List<Schema.Field> readerFields = Lists.newArrayList(
        new Schema.Field("oldfield1", INT_SCHEMA, null, null),
        new Schema.Field("newfield1", INT_SCHEMA, null, null));
    final Schema reader = Schema.createRecord(readerFields);
    final AvroUtils.SchemaPairCompatibility expectedResult =
        new AvroUtils.SchemaPairCompatibility(
            AvroUtils.SchemaCompatibilityType.INCOMPATIBLE,
            reader,
            WRITER_SCHEMA,
            String.format("Cannot use reader schema %s to decode data with writer schema %s.",
                reader.toString(true),
                WRITER_SCHEMA.toString(true)));

    // Test new field without default value.
    assertEquals(expectedResult, AvroUtils.checkReaderWriterCompatibility(reader, WRITER_SCHEMA));
  }

  @Test
  public void testValidateArrayWriterSchema() throws Exception {
    final Schema validReader = Schema.createArray(STRING_SCHEMA);
    final Schema invalidReader = Schema.createMap(STRING_SCHEMA);
    final AvroUtils.SchemaPairCompatibility validResult =
        new AvroUtils.SchemaPairCompatibility(
            AvroUtils.SchemaCompatibilityType.COMPATIBLE,
            validReader,
            ARRAY_SCHEMA,
            "Schemas match");
    final AvroUtils.SchemaPairCompatibility invalidResult =
        new AvroUtils.SchemaPairCompatibility(
            AvroUtils.SchemaCompatibilityType.INCOMPATIBLE,
            invalidReader,
            ARRAY_SCHEMA,
            String.format("Cannot use reader schema %s to decode data with writer schema %s.",
                invalidReader.toString(true),
                ARRAY_SCHEMA.toString(true)));

    assertEquals(
        validResult,
        AvroUtils.checkReaderWriterCompatibility(validReader, ARRAY_SCHEMA));
    assertEquals(
        invalidResult,
        AvroUtils.checkReaderWriterCompatibility(invalidReader, ARRAY_SCHEMA));
  }

  @Test
  public void testValidatePrimitiveWriterSchema() throws Exception {
    final Schema validReader = Schema.create(Schema.Type.STRING);
    final AvroUtils.SchemaPairCompatibility validResult =
        new AvroUtils.SchemaPairCompatibility(
            AvroUtils.SchemaCompatibilityType.COMPATIBLE,
            validReader,
            STRING_SCHEMA,
            "Schemas match");
    final AvroUtils.SchemaPairCompatibility invalidResult =
        new AvroUtils.SchemaPairCompatibility(
            AvroUtils.SchemaCompatibilityType.INCOMPATIBLE,
            INT_SCHEMA,
            STRING_SCHEMA,
            String.format("Cannot use reader schema %s to decode data with writer schema %s.",
                INT_SCHEMA.toString(true),
                STRING_SCHEMA.toString(true)));

    assertEquals(
        validResult,
        AvroUtils.checkReaderWriterCompatibility(validReader, STRING_SCHEMA));
    assertEquals(
        invalidResult,
        AvroUtils.checkReaderWriterCompatibility(INT_SCHEMA, STRING_SCHEMA));
  }

  @Test
  public void testCheckWriterCompatibility() throws Exception {
    // Setup schema fields.
    final List<Schema.Field> writerFields = Lists.newArrayList(
        new Schema.Field("oldfield1", INT_SCHEMA, null, null),
        new Schema.Field("oldfield2", STRING_SCHEMA, null, null));
    final List<Schema.Field> readerFields1 = Lists.newArrayList(
        new Schema.Field("oldfield1", INT_SCHEMA, null, null),
        new Schema.Field("newfield1", INT_SCHEMA, null, IntNode.valueOf(42)));
    final List<Schema.Field> readerFields2 = Lists.newArrayList(
        new Schema.Field("oldfield1", INT_SCHEMA, null, null),
        new Schema.Field("oldfield2", STRING_SCHEMA, null, null),
        new Schema.Field("newfield1", INT_SCHEMA, null, IntNode.valueOf(42)));
    final List<Schema.Field> readerFields3 = Lists.newArrayList(
        new Schema.Field("oldfield1", INT_SCHEMA, null, null),
        new Schema.Field("oldfield2", STRING_SCHEMA, null, null));
    final List<Schema.Field> readerFields4 = Lists.newArrayList(
        new Schema.Field("oldfield1", INT_SCHEMA, null, null),
        new Schema.Field("newfield1", INT_SCHEMA, null, null));

    // Setup schemas.
    final Schema writer = Schema.createRecord(writerFields);
    final Schema reader1 = Schema.createRecord(readerFields1);
    final Schema reader2 = Schema.createRecord(readerFields2);
    final Schema reader3 = Schema.createRecord(readerFields3);
    final Schema reader4 = Schema.createRecord(readerFields4);
    final Set<Schema> readers = Sets.newHashSet(
        reader1,
        reader2,
        reader3,
        reader4);

    // Setup expectations.
    final AvroUtils.SchemaPairCompatibility result1 =
        new AvroUtils.SchemaPairCompatibility(
            AvroUtils.SchemaCompatibilityType.COMPATIBLE,
            reader1,
            writer,
            "Schemas match");
    final AvroUtils.SchemaPairCompatibility result2 =
        new AvroUtils.SchemaPairCompatibility(
            AvroUtils.SchemaCompatibilityType.COMPATIBLE,
            reader2,
            writer,
            "Schemas match");
    final AvroUtils.SchemaPairCompatibility result3 =
        new AvroUtils.SchemaPairCompatibility(
            AvroUtils.SchemaCompatibilityType.COMPATIBLE,
            reader3,
            writer,
            "Schemas match");
    final AvroUtils.SchemaPairCompatibility result4 =
        new AvroUtils.SchemaPairCompatibility(
            AvroUtils.SchemaCompatibilityType.INCOMPATIBLE,
            reader4,
            writer,
            String.format("Cannot use reader schema %s to decode data with writer schema %s.",
                reader4.toString(true),
                writer.toString(true)));

    // Perform the check.
    final AvroUtils.SchemaSetCompatibility results = AvroUtils
        .checkWriterCompatibility(readers.iterator(), writer);

    // Ensure that the results contain the expected values.
    assertEquals(AvroUtils.SchemaCompatibilityType.INCOMPATIBLE, results.getType());
    assertTrue(results.getCauses().contains(result1));
    assertTrue(results.getCauses().contains(result2));
    assertTrue(results.getCauses().contains(result3));
    assertTrue(results.getCauses().contains(result4));
  }

  @Test
  public void testCheckReaderCompatibility() throws Exception {
    // Setup schema fields.
    final List<Schema.Field> writerFields1 = Lists.newArrayList(
        new Schema.Field("oldfield1", INT_SCHEMA, null, null),
        new Schema.Field("oldfield2", STRING_SCHEMA, null, null));
    final List<Schema.Field> writerFields2 = Lists.newArrayList(
        new Schema.Field("oldfield1", INT_SCHEMA, null, null),
        new Schema.Field("oldfield2", STRING_SCHEMA, null, null),
        new Schema.Field("newfield1", INT_SCHEMA, null, null));
    final List<Schema.Field> writerFields3 = Lists.newArrayList(
        new Schema.Field("oldfield1", INT_SCHEMA, null, null),
        new Schema.Field("newfield1", INT_SCHEMA, null, null));
    final List<Schema.Field> readerFields = Lists.newArrayList(
        new Schema.Field("oldfield1", INT_SCHEMA, null, null),
        new Schema.Field("oldfield2", STRING_SCHEMA, null, null));

    // Setup schemas.
    final Schema writer1 = Schema.createRecord(writerFields1);
    final Schema writer2 = Schema.createRecord(writerFields2);
    final Schema writer3 = Schema.createRecord(writerFields3);
    final Schema reader = Schema.createRecord(readerFields);
    final Set<Schema> written = Sets.newHashSet(writer1);
    final Set<Schema> writers = Sets.newHashSet(writer2, writer3);

    // Setup expectations.
    final AvroUtils.SchemaPairCompatibility result1 =
        new AvroUtils.SchemaPairCompatibility(
            AvroUtils.SchemaCompatibilityType.COMPATIBLE,
            reader,
            writer1,
            "Schemas match");
    final AvroUtils.SchemaPairCompatibility result2 =
        new AvroUtils.SchemaPairCompatibility(
            AvroUtils.SchemaCompatibilityType.COMPATIBLE,
            reader,
            writer2,
            "Schemas match");
    final AvroUtils.SchemaPairCompatibility result3 =
        new AvroUtils.SchemaPairCompatibility(
            AvroUtils.SchemaCompatibilityType.INCOMPATIBLE,
            reader,
            writer3,
            String.format("Cannot use reader schema %s to decode data with writer schema %s.",
                reader.toString(true),
                writer3.toString(true)));

    // Perform the check.
    final AvroUtils.SchemaSetCompatibility results = AvroUtils
        .checkReaderCompatibility(reader, Iterators.concat(written.iterator(), writers.iterator()));

    // Ensure that the results contain the expected values.
    assertEquals(AvroUtils.SchemaCompatibilityType.INCOMPATIBLE, results.getType());
    assertTrue(results.getCauses().contains(result1));
    assertTrue(results.getCauses().contains(result2));
    assertTrue(results.getCauses().contains(result3));
  }

  @Test
  public void testAvroSchemaEquals() throws IOException {
    final KijiSchemaTable schemaTable = getKiji().getSchemaTable();

    final long stringUID = schemaTable.getOrCreateSchemaId(STRING_SCHEMA);
    final long intUID = schemaTable.getOrCreateSchemaId(INT_SCHEMA);
    final String stringJSON = STRING_SCHEMA.toString();
    final String intJSON = INT_SCHEMA.toString();

    final AvroSchema stringUIDAS = AvroSchema.newBuilder().setUid(stringUID).build();
    final AvroSchema stringJSONAS = AvroSchema.newBuilder().setJson(stringJSON).build();
    final AvroSchema intUIDAS = AvroSchema.newBuilder().setUid(intUID).build();
    final AvroSchema intJSONAS = AvroSchema.newBuilder().setJson(intJSON).build();

    assertTrue(AvroUtils.avroSchemaEquals(schemaTable, stringUIDAS, stringUIDAS));
    assertTrue(AvroUtils.avroSchemaEquals(schemaTable, stringUIDAS, stringJSONAS));
    assertTrue(AvroUtils.avroSchemaEquals(schemaTable, stringJSONAS, stringUIDAS));
    assertTrue(AvroUtils.avroSchemaEquals(schemaTable, intUIDAS, intUIDAS));
    assertTrue(AvroUtils.avroSchemaEquals(schemaTable, intUIDAS, intJSONAS));
    assertTrue(AvroUtils.avroSchemaEquals(schemaTable, intJSONAS, intUIDAS));

    assertFalse(AvroUtils.avroSchemaEquals(schemaTable, stringUIDAS, intUIDAS));
    assertFalse(AvroUtils.avroSchemaEquals(schemaTable, stringUIDAS, intJSONAS));
    assertFalse(AvroUtils.avroSchemaEquals(schemaTable, stringJSONAS, intJSONAS));
    assertFalse(AvroUtils.avroSchemaEquals(schemaTable, stringJSONAS, intUIDAS));
  }

  @Test
  public void testAvroSchemaListContains() throws IOException {
    final KijiSchemaTable schemaTable = getKiji().getSchemaTable();

    final long stringUID = schemaTable.getOrCreateSchemaId(STRING_SCHEMA);
    final long intUID = schemaTable.getOrCreateSchemaId(INT_SCHEMA);
    final String stringJSON = STRING_SCHEMA.toString();
    final String intJSON = INT_SCHEMA.toString();

    final AvroSchema stringUIDAS = AvroSchema.newBuilder().setUid(stringUID).build();
    final AvroSchema stringJSONAS = AvroSchema.newBuilder().setJson(stringJSON).build();
    final AvroSchema intUIDAS = AvroSchema.newBuilder().setUid(intUID).build();
    final AvroSchema intJSONAS = AvroSchema.newBuilder().setJson(intJSON).build();

    final List<AvroSchema> stringList = Lists.newArrayList(stringJSONAS, stringUIDAS);
    final List<AvroSchema> intList = Lists.newArrayList(intJSONAS, intUIDAS);
    final List<AvroSchema> bothList = Lists.newArrayList(stringJSONAS, intUIDAS);

    assertTrue(AvroUtils.avroSchemaCollectionContains(schemaTable, stringList, stringJSONAS));
    assertTrue(AvroUtils.avroSchemaCollectionContains(schemaTable, stringList, stringUIDAS));
    assertFalse(AvroUtils.avroSchemaCollectionContains(schemaTable, stringList, intUIDAS));
    assertTrue(AvroUtils.avroSchemaCollectionContains(schemaTable, intList, intJSONAS));
    assertTrue(AvroUtils.avroSchemaCollectionContains(schemaTable, intList, intUIDAS));
    assertFalse(AvroUtils.avroSchemaCollectionContains(schemaTable, intList, stringUIDAS));
    assertTrue(AvroUtils.avroSchemaCollectionContains(schemaTable, bothList, stringJSONAS));
    assertTrue(AvroUtils.avroSchemaCollectionContains(schemaTable, bothList, intUIDAS));
  }
}
