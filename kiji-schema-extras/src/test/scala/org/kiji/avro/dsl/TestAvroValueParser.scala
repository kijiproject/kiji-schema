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

import java.lang.{Integer => JInteger}
import java.lang.{Float => JFloat}
import java.lang.{Double => JDouble}
import java.util.{List => JList}

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData
import org.junit.Assert
import org.kiji.schema.avro.TableLayoutDesc
import org.slf4j.LoggerFactory

import junit.framework.TestCase
/**
 * Tests the Avro value DSL parser and serializer.
 */
class TestAvroValueParser
    extends TestCase {

  final val Log = LoggerFactory.getLogger(classOf[TestAvroValueParser])

  private final val StringSchema = Schema.create(Type.STRING)
  private val schemaParser = new AvroSchemaParser()

  def testPrimitives(): Unit = {
    Assert.assertEquals(null, AvroValueParser.parse("null", schemaParser.parse("null")))
    Assert.assertEquals(true, AvroValueParser.parse("true", Schema.create(Type.BOOLEAN)))
    Assert.assertEquals(false, AvroValueParser.parse("false", Schema.create(Type.BOOLEAN)))
    Assert.assertEquals(new JInteger(1), AvroValueParser.parse("1", Schema.create(Type.INT)))
    Assert.assertEquals(-1L, AvroValueParser.parse("-1", Schema.create(Type.LONG)))
    Assert.assertEquals(-1L, AvroValueParser.parse("-1L", Schema.create(Type.LONG)))
    Assert.assertEquals(
        new JFloat(3.14f),
        AvroValueParser.parse("3.14", Schema.create(Type.FLOAT)))
    Assert.assertEquals(
        new JFloat(3.14f),
        AvroValueParser.parse("3.14f", Schema.create(Type.FLOAT)))
    Assert.assertEquals(
        new JDouble(3.14d),
        AvroValueParser.parse("3.14", Schema.create(Type.DOUBLE)))
    Assert.assertEquals(
        new JDouble(3.14d),
        AvroValueParser.parse("3.14d", Schema.create(Type.DOUBLE)))
  }

  def testBytes(): Unit = {
    Assert.assertArrayEquals(
        Array[Byte](1, 2, 3),
        AvroValueParser.parse("bytes(010203)", Schema.create(Type.BYTES)))
    Assert.assertArrayEquals(
        Array[Byte](1, 2, 3),
        AvroValueParser.parse("bytes(01 02 03)", Schema.create(Type.BYTES)))
    Assert.assertArrayEquals(
        Array[Byte](1, 2, 3),
        AvroValueParser.parse("bytes(01,02,03)", Schema.create(Type.BYTES)))
    Assert.assertArrayEquals(
        Array[Byte](1, 2, 3),
        AvroValueParser.parse("bytes(01,02,03,)", Schema.create(Type.BYTES)))
    Assert.assertArrayEquals(
        Array[Byte](1, 2, 3),
        AvroValueParser.parse("bytes(01:02:03)", Schema.create(Type.BYTES)))
    Assert.assertArrayEquals(
        Array[Byte](1, 2, 3),
        AvroValueParser.parse("bytes(01;02;03)", Schema.create(Type.BYTES)))
    Assert.assertArrayEquals(
        Array[Byte](1, 2, 3),
        AvroValueParser.parse("bytes(01;02;03;)", Schema.create(Type.BYTES)))
  }

  def testArray(): Unit = {
    val intArraySchema = schemaParser.parse("array<int>")
    val list = List(1, 2, 3).asJava
    val strs = List(
        """[1, 2, 3]""",
        """[1, 2, 3,]""",
        """[1 2 3]""",
        """[1; 2; 3]""",
        """[1; 2; 3;]"""
    )
    for (str <- strs) {
      Log.info("Parsing '{}'", str)
      val parsed: Any = AvroValueParser.parse(str, intArraySchema)
      Log.info("Parsed '{}' as {}", str.asInstanceOf[Any], parsed)
      Assert.assertEquals(list, parsed)
      Log.info(AvroValue.toString(parsed, intArraySchema))
    }
  }

  def testMap(): Unit = {
    val intMapSchema = schemaParser.parse("map<int>")
    val map = Map("a" -> 1, "b" -> 2).asJava
    val strs = List(
        """{"a": 1, "b": 2}""",
        """{"a": 1; "b": 2;}""",
        """{"a": 1 "b": 2}"""
    )
    for (str <- strs) {
      Log.info("Parsing '{}'", str)
      val parsed: Any = AvroValueParser.parse(str, intMapSchema)
      Log.info("Parsed '{}' into {}", str.asInstanceOf[Any], parsed)
      Assert.assertEquals(map, parsed)
      Log.info(AvroValue.toString(parsed, intMapSchema))
    }
  }

  def testRecords(): Unit = {
    val schema = schemaParser.parse("record ns.X { int x; string y }")
    val record =
        AvroValueParser.parse(""" ns.X { x=-314159; y="hello" } """, schema)
        .asInstanceOf[GenericData.Record]
    Assert.assertEquals(schema, record.getSchema)
    Assert.assertEquals(-314159, record.get("x"))
    Assert.assertEquals("hello", record.get("y"))
  }

  def testUnion(): Unit = {
    val schema = schemaParser.parse("union { null, int }")
    Assert.assertEquals(null, AvroValueParser.parse("null", schema))
    Assert.assertEquals(15, AvroValueParser.parse[Any]("15", schema))

    Assert.assertEquals("null", AvroValue.toString(null, schema))
    Assert.assertEquals("15", AvroValue.toString(15, schema))
  }

  def testString(): Unit = {
    Assert.assertEquals("", AvroValueParser.parse(""" "" """, StringSchema))
    Assert.assertEquals("", AvroValueParser.parse(""" '' """, StringSchema))
    Assert.assertEquals("", AvroValueParser.parse(" \"\"\"\"\"\" ", StringSchema))

    Assert.assertEquals("hello", AvroValueParser.parse(""" "hello" """, StringSchema))
    Assert.assertEquals("hello", AvroValueParser.parse(""" 'hello' """, StringSchema))
    Assert.assertEquals("hello", AvroValueParser.parse(" \"\"\"hello\"\"\" ", StringSchema))

    Assert.assertEquals("hel'lo", AvroValueParser.parse(""" "hel'lo" """, StringSchema))
    Assert.assertEquals("hel'lo", AvroValueParser.parse(""" 'hel\'lo' """, StringSchema))
    Assert.assertEquals("hel'lo", AvroValueParser.parse(" \"\"\"hel\'lo\"\"\" ", StringSchema))

    Assert.assertEquals("hel\"lo", AvroValueParser.parse(""" "hel\"lo" """, StringSchema))
    Assert.assertEquals("hel\"lo", AvroValueParser.parse(""" 'hel"lo' """, StringSchema))
    Assert.assertEquals("hel\"lo", AvroValueParser.parse(" \"\"\"hel\"lo\"\"\" ", StringSchema))

    Assert.assertEquals("hel\nlo", AvroValueParser.parse(" \"\"\"hel\nlo\"\"\" ", StringSchema))
  }

  def testDoubleQuoteString(): Unit = {
    val str: String =
        AvroValueParser.parse(""" "Here is a \"double quoted\" word." """, StringSchema)
        .asInstanceOf[String]

    Log.info("str=[%s]".format(str))
    Assert.assertEquals("""Here is a "double quoted" word.""", str)
    val reserialized = AvroValue.toString(str, StringSchema)
    Log.info("reserialized=[%s]".format(reserialized))
    Assert.assertEquals(""""Here is a \"double quoted\" word."""", reserialized)
    val str2: String = AvroValueParser.parse(reserialized, StringSchema)
    Log.info("str2=[%s]".format(str2))
    Assert.assertEquals(str, str2)
  }

  def testSingleQuoteString(): Unit = {
    val str: String =
        AvroValueParser.parse(""" 'Here is a \'double quoted\' word.' """, StringSchema)
        .asInstanceOf[String]
    Log.info("str=[%s]".format(str))
    Assert.assertEquals("""Here is a 'double quoted' word.""", str)
    val reserialized = AvroValue.toString(str, StringSchema)
    Log.info("reserialized=[%s]".format(reserialized))
    Assert.assertEquals(""""Here is a 'double quoted' word."""", reserialized)
    val str2: String = AvroValueParser.parse(reserialized, StringSchema)
    Log.info("str2=[%s]".format(str2))
    Assert.assertEquals(str, str2)
  }

  def testLayout(): Unit = {
    val layoutSchemas = schemaParser.parseSequence("""
      |enum org.kiji.schema.avro.CompressionType { NONE, GZ, LZO, SNAPPY }
      |enum org.kiji.schema.avro.SchemaType { INLINE, CLASS, COUNTER, AVRO, RAW_BYTES, PROTOBUF }
      |enum org.kiji.schema.avro.SchemaStorage { HASH, UID, FINAL }
      |record org.kiji.schema.avro.AvroSchema {
      |  union { null, long } uid = null;
      |  union { null, string } json = null;
      |}
      |enum org.kiji.schema.avro.AvroValidationPolicy { STRICT, DEVELOPER, SCHEMA_1_0, NONE }
      |record org.kiji.schema.avro.CellSchema {
      |  org.kiji.schema.avro.SchemaStorage storage = org.kiji.schema.avro.SchemaStorage(HASH);
      |  org.kiji.schema.avro.SchemaType type;
      |  union { null, string } value = null;
      |  org.kiji.schema.avro.AvroValidationPolicy avro_validation_policy =
      |      org.kiji.schema.avro.AvroValidationPolicy(SCHEMA_1_0);
      |  union { null, string } specific_reader_schema_class = null;
      |  union { null, org.kiji.schema.avro.AvroSchema } default_reader = null;
      |  union { null, array<org.kiji.schema.avro.AvroSchema> } readers = null;
      |  union { null, array<org.kiji.schema.avro.AvroSchema> } written = null;
      |  union { null, array<org.kiji.schema.avro.AvroSchema> } writers = null;
      |  union { null, string } protobuf_full_name = null;
      |  union { null, string } protobuf_class_name = null;
      |}
      |record org.kiji.schema.avro.ColumnDesc {
      |  int id = 0;
      |  string name;
      |  array<string> aliases = [];
      |  boolean enabled = true;
      |  string description = "";
      |  org.kiji.schema.avro.CellSchema column_schema;
      |  boolean delete = false;
      |  union { null, string } renamed_from = null;
      |}
      |record org.kiji.schema.avro.FamilyDesc {
      |  int id = 0;
      |  string name;
      |  array<string> aliases = [];
      |  boolean enabled = true;
      |  string description = "";
      |  union { null, org.kiji.schema.avro.CellSchema } map_schema = null;
      |  array<org.kiji.schema.avro.ColumnDesc> columns = [];
      |  boolean delete = false;
      |  union { null, string } renamed_from = null;
      |}
      |enum org.kiji.schema.avro.BloomType { NONE, ROW, ROWCOL }
      |record org.kiji.schema.avro.LocalityGroupDesc {
      |  int id = 0;
      |  string name;
      |  array<string> aliases = [];
      |  boolean enabled = true;
      |  string description = "";
      |  boolean in_memory;
      |  int max_versions;
      |  int ttl_seconds;
      |  union { null, int } block_size = null;
      |  union { null, org.kiji.schema.avro.BloomType } bloom_type = null;
      |  org.kiji.schema.avro.CompressionType compression_type;
      |  array<org.kiji.schema.avro.FamilyDesc> families = [];
      |  boolean delete = false;
      |  union { null, string } renamed_from = null;
      |}
      |enum org.kiji.schema.avro.HashType { MD5 }
      |enum org.kiji.schema.avro.RowKeyEncoding { RAW, HASH, HASH_PREFIX, FORMATTED }
      |record org.kiji.schema.avro.HashSpec {
      |  org.kiji.schema.avro.HashType hash_type = org.kiji.schema.avro.HashType(MD5);
      |  int hash_size = 16;
      |  boolean suppress_key_materialization = false;
      |}
      |record org.kiji.schema.avro.RowKeyFormat {
      |  org.kiji.schema.avro.RowKeyEncoding encoding;
      |  union { null, org.kiji.schema.avro.HashType } hash_type = null;
      |  int hash_size = 0;
      |}
      |enum org.kiji.schema.avro.ComponentType { STRING, INTEGER, LONG }
      |record org.kiji.schema.avro.RowKeyComponent {
      |  string name;
      |  org.kiji.schema.avro.ComponentType type;
      |}
      |record org.kiji.schema.avro.RowKeyFormat2 {
      |  org.kiji.schema.avro.RowKeyEncoding encoding;
      |   union { org.kiji.schema.avro.HashSpec, null } salt =
      |       org.kiji.schema.avro.HashSpec {
      |           hash_type=org.kiji.schema.avro.HashType(MD5),
      |           hash_size=2,
      |           suppress_key_materialization=false
      |       };
      |  int range_scan_start_index = 1;
      |  int nullable_start_index = 1;
      |  array<org.kiji.schema.avro.RowKeyComponent> components = [];
      |}
      |record org.kiji.schema.avro.TableLayoutDesc {
      |  string name;
      |  union { null, long } max_filesize = null;
      |  union { null, long } memstore_flushsize = null;
      |  string description = "";
      |  union {
      |      org.kiji.schema.avro.RowKeyFormat,
      |      org.kiji.schema.avro.RowKeyFormat2
      |  } keys_format;
      |  array<org.kiji.schema.avro.LocalityGroupDesc> locality_groups = [];
      |  string version;
      |  union { null, string } layout_id = null;
      |  union { null, string } reference_layout = null;
      |}
    """.stripMargin)

    val layoutSchema = schemaParser.get("org.kiji.schema.avro.TableLayoutDesc")
    Log.info("TableLayout descriptor schema: {}",
        AvroSchema.toString(layoutSchema))

    val layoutDesc: GenericData.Record = AvroValueParser.parse(
        text="""
          |org.kiji.schema.avro.TableLayoutDesc {
          |  name = "table_name"
          |  keys_format = org.kiji.schema.avro.RowKeyFormat2 {
          |    encoding = org.kiji.schema.avro.RowKeyEncoding(FORMATTED)
          |    components = [
          |      org.kiji.schema.avro.RowKeyComponent {
          |        name = "key"
          |        type = org.kiji.schema.avro.ComponentType(STRING)
          |      }
          |    ]
          |  },
          |  locality_groups = [
          |    org.kiji.schema.avro.LocalityGroupDesc {
          |      name = "default_lg",
          |      max_versions = 1000,
          |      ttl_seconds = 3600
          |      in_memory = false,
          |      compression_type = org.kiji.schema.avro.CompressionType(NONE)
          |      families = [
          |        org.kiji.schema.avro.FamilyDesc {
          |          name = "info",
          |        }
          |      ],
          |    },
          |  ],
          |  version = "",
          |}
        """.stripMargin,
        schema=layoutSchema
    )
    Log.info("Table layout descriptor: {}", layoutDesc)
    Log.info("Table layout descriptor: {}", AvroValue.toString(layoutDesc, layoutSchema))
    Assert.assertEquals("table_name", layoutDesc.get("name"))
    val localityGroups = layoutDesc.get("locality_groups").asInstanceOf[JList[GenericData.Record]]
    Assert.assertEquals(1, localityGroups.size)
    val localityGroup = localityGroups.get(0)
    Assert.assertEquals(1000, localityGroup.get("max_versions"))
    Assert.assertEquals(3600, localityGroup.get("ttl_seconds"))
    val families = localityGroup.get("families").asInstanceOf[JList[GenericData.Record]]
    Assert.assertEquals(1, families.size)
    Assert.assertEquals("info", families.get(0).get("name"))
  }

  def testSchemaWithComments(): Unit = {
    val schema = schemaParser.parse("""
        |record ns.A {
        |  int zip_code
        |  int age = -1
        |  union { null, long } field = null
        |}
    """.stripMargin)

    val value: GenericData.Record = AvroValueParser.parse("""
        |record ns.A {  // This is a comments
        |  zip_code = /* comment */ 1
        |  /**
        |   * This
        |   * is a
        |   * multi-line
        |   * comment
        |   */
        |  age = 20
        |}
        """.stripMargin,
        schema=schema)
    Assert.assertEquals(1, value.get("zip_code"))
    Assert.assertEquals(20, value.get("age"))
  }
}
