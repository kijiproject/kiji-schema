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
package org.kiji.avro.dsl

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.util.parsing.input.CharSequenceReader
import org.apache.avro.Schema
import org.codehaus.jackson.node.JsonNodeFactory
import org.junit.Assert
import org.slf4j.LoggerFactory
import junit.framework.TestCase

/**
 * Tests the Avro schema DSL parser and serializer.
 */
class TestAvroSchemaParser
    extends TestCase {

  private final val Log = LoggerFactory.getLogger(classOf[TestAvroSchemaParser])
  private val parser = new AvroSchemaParser()

  def testAvroName(): Unit = {
    def parseAvroName(text: String, context: parser.Context): AvroName = {
      parser.avroName(context)(new CharSequenceReader(text)) match {
        case success: parser.Success[AvroName] => success.get
        case error => sys.error("Error parsing Avro name '%s' : %s".format(text, error))
      }
    }

    // Test Avro name parser when context has no default namespace:
    {
      val avroName = parseAvroName(text = "SimpleName", context = parser.Context())
      Assert.assertEquals("SimpleName", avroName.simpleName)
      Assert.assertEquals("", avroName.namespace)
      Assert.assertEquals(".SimpleName", avroName.fullName)
    }
    {
      val avroName = parseAvroName(text = ".SimpleName", context = parser.Context())
      Assert.assertEquals("SimpleName", avroName.simpleName)
      Assert.assertEquals("", avroName.namespace)
      Assert.assertEquals(".SimpleName", avroName.fullName)
    }
    {
      val avroName = parseAvroName(text = "name.space.SimpleName", context = parser.Context())
      Assert.assertEquals("SimpleName", avroName.simpleName)
      Assert.assertEquals("name.space", avroName.namespace)
      Assert.assertEquals("name.space.SimpleName", avroName.fullName)
    }
    {
      val avroName = parseAvroName(text = ".name.space.SimpleName", context = parser.Context())
      Assert.assertEquals("SimpleName", avroName.simpleName)
      Assert.assertEquals("name.space", avroName.namespace)
      Assert.assertEquals("name.space.SimpleName", avroName.fullName)
    }

    // Test Avro name parser when context has a default namespace:
    {
      val avroName = parseAvroName(text = "SimpleName",
          context = parser.Context(defaultNamespace = Some("name.space")))
      Assert.assertEquals("SimpleName", avroName.simpleName)
      Assert.assertEquals("name.space", avroName.namespace)
      Assert.assertEquals("name.space.SimpleName", avroName.fullName)
    }
    {
      val avroName = parseAvroName(text = ".SimpleName",
          context = parser.Context(defaultNamespace = Some("name.space")))
      Assert.assertEquals("SimpleName", avroName.simpleName)
      Assert.assertEquals("", avroName.namespace)
      Assert.assertEquals(".SimpleName", avroName.fullName)
    }
    {
      val avroName = parseAvroName(text = "name.space.SimpleName",
          context = parser.Context(defaultNamespace = Some("name.space")))
      Assert.assertEquals("SimpleName", avroName.simpleName)
      Assert.assertEquals("name.space", avroName.namespace)
      Assert.assertEquals("name.space.SimpleName", avroName.fullName)
    }
    {
      val avroName = parseAvroName(text = ".name.space.SimpleName",
          context = parser.Context(defaultNamespace = Some("name.space")))
      Assert.assertEquals("SimpleName", avroName.simpleName)
      Assert.assertEquals("name.space", avroName.namespace)
      Assert.assertEquals("name.space.SimpleName", avroName.fullName)
    }
  }

  def testPrimitives(): Unit = {
    Assert.assertEquals(Schema.create(Schema.Type.NULL), parser.parse("null"))
    Assert.assertEquals(Schema.create(Schema.Type.BOOLEAN), parser.parse("boolean"))
    Assert.assertEquals(Schema.create(Schema.Type.INT), parser.parse("int"))
    Assert.assertEquals(Schema.create(Schema.Type.LONG), parser.parse("long"))
    Assert.assertEquals(Schema.create(Schema.Type.FLOAT), parser.parse("float"))
    Assert.assertEquals(Schema.create(Schema.Type.DOUBLE), parser.parse("double"))
    Assert.assertEquals(Schema.create(Schema.Type.STRING), parser.parse("string"))
    Assert.assertEquals(Schema.create(Schema.Type.BYTES), parser.parse("bytes"))
  }

  def testArray(): Unit = {
    val idl = "array<int>"
    val schema = parser.parse(idl)
    Assert.assertEquals(Schema.Type.ARRAY, schema.getType)
    Assert.assertEquals(Schema.create(Schema.Type.INT), schema.getElementType)
    val str = AvroSchema.toString(schema)
    Assert.assertEquals(idl, str)
    val parser2 = new AvroSchemaParser()
    Assert.assertEquals(schema, parser2.parse(str))
  }

  def testMap(): Unit = {
    val idl = "map<long>"
    val schema = parser.parse(idl)
    Assert.assertEquals(Schema.Type.MAP, schema.getType)
    Assert.assertEquals(Schema.create(Schema.Type.LONG), schema.getValueType)
    val str = AvroSchema.toString(schema)
    Assert.assertEquals(idl, str)
    val parser2 = new AvroSchemaParser()
    Assert.assertEquals(schema, parser2.parse(str))
  }

  def testFixed(): Unit = {
    val idl = "fixed ns.MD5(16)"
    val schema = parser.parse(idl)
    Assert.assertEquals(Schema.Type.FIXED, schema.getType)
    Assert.assertEquals(16, schema.getFixedSize)
    val str = AvroSchema.toString(schema)
    Assert.assertEquals(idl, str)
    val parser2 = new AvroSchemaParser()
    Assert.assertEquals(schema, parser2.parse(str))
  }

  def testEnum(): Unit = {
    val idl = "enum ns.Enum{S1,S2,S3}"
    val schema = parser.parse(idl)
    Assert.assertEquals(Schema.Type.ENUM, schema.getType)
    Assert.assertEquals(List("S1", "S2", "S3"), schema.getEnumSymbols.asScala)
    val str = AvroSchema.toString(schema)
    Assert.assertEquals(idl, str)
    Assert.assertEquals(schema, new AvroSchemaParser().parse(str))
    Assert.assertEquals(schema, new AvroSchemaParser().parse("enum ns.Enum{S1,S2,S3,}"))
    Assert.assertEquals(schema, new AvroSchemaParser().parse("enum ns.Enum{S1 S2 S3}"))
    Assert.assertEquals(schema, new AvroSchemaParser().parse("enum ns.Enum{S1;S2;S3}"))
    Assert.assertEquals(schema, new AvroSchemaParser().parse("enum ns.Enum{S1;S2;S3;}"))
  }

  def testRecord(): Unit = {
    val idl = "record ns.R{int x,string y}"
    val schema = parser.parse(idl)
    val str = AvroSchema.toString(schema)
    Assert.assertEquals(idl, str)
    Assert.assertEquals(schema, new AvroSchemaParser().parse(str))
    Assert.assertEquals(schema, new AvroSchemaParser().parse("record ns.R{int x;string y;}"))
    Assert.assertEquals(schema, new AvroSchemaParser().parse("record ns.R{int x string y}"))
  }

  def testRecordWithDefault(): Unit = {
    val idl = """record ns.R{int x = 0,string y = ""}"""
    val schema = parser.parse(idl)
    Log.info("Parsed '{}' into schema {}", idl.asInstanceOf[Any], schema)
    val str = AvroSchema.toString(schema)
    Assert.assertEquals(idl, str)
    val parser2 = new AvroSchemaParser()
    Assert.assertEquals(schema, parser2.parse(str))
  }

  def testUnionEmpty(): Unit = {
    val idl = "union{}"
    val schema = parser.parse(idl)
    Assert.assertEquals(Schema.Type.UNION, schema.getType)
    Assert.assertTrue(schema.getTypes.isEmpty)
    val str = AvroSchema.toString(schema)
    Assert.assertEquals(idl, str)
    Assert.assertEquals(schema, new AvroSchemaParser().parse(str))
  }

  def testUnionSingleton(): Unit = {
    val idl = "union{null}"
    val schema = parser.parse(idl)
    Assert.assertEquals(Schema.Type.UNION, schema.getType)
    Assert.assertEquals(List(Schema.create(Schema.Type.NULL)), schema.getTypes.asScala)
    val str = AvroSchema.toString(schema)
    Assert.assertEquals(idl, str)
    Assert.assertEquals(schema, new AvroSchemaParser().parse(str))
  }

  def testUnion(): Unit = {
    val idl = "union{null,int}"
    val schema = parser.parse(idl)
    Assert.assertEquals(Schema.Type.UNION, schema.getType)
    Assert.assertEquals(
        List(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)),
        schema.getTypes.asScala)
    val str = AvroSchema.toString(schema)
    Assert.assertEquals(idl, str)
    Assert.assertEquals(schema, new AvroSchemaParser().parse(str))
  }

  def testSimpleRecursiveRecord(): Unit = {
    val schema = parser.parse("""
        |record ns.IntList{
        |  int head
        |  union {null, ns.IntList} tail = null
        |}
    """.stripMargin)
    Assert.assertEquals(Schema.Type.RECORD, schema.getType)
    // TODO: toAvroSchemaIDL doesn't handle recursive records
    // val str = AvroSchema.toAvroSchemaIDL(schema)
    // Assert.assertEquals(schema, new AvroSchemaIDLParser().parse(str))
  }

  def testRecordWithNestedRecordDefault(): Unit = {
    val idl = """
      |record ns.A { boolean is_a }
      |record ns.B { ns.A a = ns.A { is_a = true } }
    """.stripMargin
    val schemas = parser.parseSequence(idl)
    val schemaB = parser.get("ns.B")
    Assert.assertEquals(
        JsonNodeFactory.instance.booleanNode(true),
        schemaB.getField("a").defaultValue.get("is_a"))
  }

  def testRecordWithNestedRecordDefaultAsJson(): Unit = {
    val idl = """
      |record ns.A { boolean is_a }
      |record ns.B { ns.A a = { "is_a": true } }
    """.stripMargin
    val schemas = parser.parseSequence(idl)
    val schemaB = parser.get("ns.B")
    Assert.assertEquals(
        JsonNodeFactory.instance.booleanNode(true),
        schemaB.getField("a").defaultValue.get("is_a"))
  }

  def testParseSequence(): Unit = {
    val schemas = parser.parseSequence("""int long float""")
    Assert.assertEquals(
        List(Schema.create(Schema.Type.INT),
            Schema.create(Schema.Type.LONG),
            Schema.create(Schema.Type.FLOAT)),
        schemas)
    Assert.assertEquals(schemas, parser.parseSequence("""int, long, float"""))
    Assert.assertEquals(schemas, parser.parseSequence("""int, long, float,"""))
    Assert.assertEquals(schemas, parser.parseSequence("""int; long; float;"""))
  }

  def testMutualRecursiveRecords(): Unit = {
    val schemas = parser.parseSequence("""
        |record ns.A
        |record ns.B
        |record ns.A { union { null, ns.B } b = null }
        |record ns.B { union { null, ns.A } a = null }
    """.stripMargin)
    val nsA: Schema = parser.get("ns.A")
    val nsB: Schema = parser.get("ns.B")
    Log.info("ns.A: {}", nsA)
    Log.info("ns.B: {}", nsB)
    Assert.assertEquals(Schema.Type.RECORD, nsA.getType)
    Assert.assertEquals(Schema.Type.RECORD, nsB.getType)
    Assert.assertEquals(1, nsA.getFields.size)
    Assert.assertEquals(1, nsB.getFields.size)
    val nsAField = nsA.getFields.get(0)
    val nsBField = nsB.getFields.get(0)
    Assert.assertEquals("b", nsAField.name)
    Assert.assertEquals("a", nsBField.name)
  }

  def testRecordWithNamedField(): Unit = {
    val schemas = parser.parseSequence("""
        |enum ns.A {}
        |record ns.B {
        |  ns.A a
        |}
    """.stripMargin)
  }

  def testUnionWithDefault(): Unit = {
    parser.parseSequence("""
        |record ns.A {int integer}
        |record ns.B {
        |  union { ns.A, null } field = ns.A { integer = 0 }
        |}
    """.stripMargin)
    parser.parseSequence("""
        |record ns.C {int integer}
        |record ns.D {
        |  union { ns.C, null } field =  { "integer": 0 }
        |}
    """.stripMargin)
  }

  def testSchemaWithComments(): Unit = {
    parser.parse("""
        |record ns.A {
        |  int zip_code; // This is a single line comment
        |  int /* delimited comment */ age = -1;
        |  union { null /* , long */ } field = null
        |  /**
        |   * multi-line
        |   * comment
        |   */
        |}
    """.stripMargin)
  }

  def testMetaSchema(): Unit = {
    val schemas = parser.parseSequence("""
        |record org.apache.avro.Schema
        |
        |record org.apache.avro.NullSchema {}
        |record org.apache.avro.BooleanSchema {}
        |record org.apache.avro.IntSchema {}
        |record org.apache.avro.LongSchema {}
        |record org.apache.avro.FloatSchema {}
        |record org.apache.avro.DoubleSchema {}
        |record org.apache.avro.StringSchema {}
        |record org.apache.avro.BytesSchema {}
        |record org.apache.avro.ArraySchema {
        |  org.apache.avro.Schema itemSchema
        |}
        |record org.apache.avro.MapSchema {
        |  org.apache.avro.Schema elementSchema
        |}
        |record org.apache.avro.FixedSchema {
        |  string name
        |  int nbytes
        |}
        |record org.apache.avro.EnumSchema {
        |  string name
        |  array<string> symbols
        |}
        |record org.apache.avro.RecordField {
        |  org.apache.avro.Schema schema
        |  union { null, string } default
        |}
        |record org.apache.avro.RecordSchema {
        |  string name
        |  map<org.apache.avro.RecordField> fields
        |}
        |record org.apache.avro.UnionSchema {
        |  array<org.apache.avro.Schema> branches
        |}
        |record org.apache.avro.Schema {
        |  union {
        |    org.apache.avro.NullSchema
        |    org.apache.avro.BooleanSchema
        |    org.apache.avro.IntSchema
        |    org.apache.avro.LongSchema
        |    org.apache.avro.FloatSchema
        |    org.apache.avro.DoubleSchema
        |    org.apache.avro.StringSchema
        |    org.apache.avro.BytesSchema
        |    org.apache.avro.ArraySchema
        |    org.apache.avro.MapSchema
        |    org.apache.avro.FixedSchema
        |    org.apache.avro.EnumSchema
        |    org.apache.avro.RecordSchema
        |    org.apache.avro.UnionSchema
        |  } schema
        |}
    """.stripMargin)
    Log.info("{}", schemas)
    Assert.assertEquals(16, schemas.size)
  }
}
