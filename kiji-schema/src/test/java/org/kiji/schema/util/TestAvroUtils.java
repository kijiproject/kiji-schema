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

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.codehaus.jackson.node.IntNode;
import org.junit.Test;

public class TestAvroUtils {
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
    final AvroUtils.ReaderWriterCompatibilityResult expectedResult =
        new AvroUtils.ReaderWriterCompatibilityResult(
            AvroUtils.ReaderWriterCompatibility.COMPATIBLE,
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
    final AvroUtils.ReaderWriterCompatibilityResult expectedResult =
        new AvroUtils.ReaderWriterCompatibilityResult(
            AvroUtils.ReaderWriterCompatibility.COMPATIBLE,
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
    final AvroUtils.ReaderWriterCompatibilityResult expectedResult =
        new AvroUtils.ReaderWriterCompatibilityResult(
            AvroUtils.ReaderWriterCompatibility.COMPATIBLE,
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
    final AvroUtils.ReaderWriterCompatibilityResult expectedResult =
        new AvroUtils.ReaderWriterCompatibilityResult(
            AvroUtils.ReaderWriterCompatibility.COMPATIBLE,
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
    final AvroUtils.ReaderWriterCompatibilityResult expectedResult =
        new AvroUtils.ReaderWriterCompatibilityResult(
            AvroUtils.ReaderWriterCompatibility.INCOMPATIBLE,
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
    final AvroUtils.ReaderWriterCompatibilityResult validResult =
        new AvroUtils.ReaderWriterCompatibilityResult(
            AvroUtils.ReaderWriterCompatibility.COMPATIBLE,
            validReader,
            ARRAY_SCHEMA,
            "Schemas match");
    final AvroUtils.ReaderWriterCompatibilityResult invalidResult =
        new AvroUtils.ReaderWriterCompatibilityResult(
            AvroUtils.ReaderWriterCompatibility.INCOMPATIBLE,
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
    final AvroUtils.ReaderWriterCompatibilityResult validResult =
        new AvroUtils.ReaderWriterCompatibilityResult(
            AvroUtils.ReaderWriterCompatibility.COMPATIBLE,
            validReader,
            STRING_SCHEMA,
            "Schemas match");
    final AvroUtils.ReaderWriterCompatibilityResult invalidResult =
        new AvroUtils.ReaderWriterCompatibilityResult(
            AvroUtils.ReaderWriterCompatibility.INCOMPATIBLE,
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
}
