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

import java.io.ByteArrayOutputStream

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable
import scala.util.parsing.combinator.JavaTokenParsers
import scala.util.parsing.input.CharSequenceReader

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.Encoder
import org.apache.avro.io.EncoderFactory
import org.apache.commons.lang.StringUtils
import org.codehaus.jackson.JsonFactory
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.node.JsonNodeFactory
import org.slf4j.LoggerFactory

/** Parser for Avro Schema definitions. */
trait AvroSchemaParsers
    extends JavaTokenParsers
    with JacksonJsonParser
    with AvroValueParser {
  final val Log = LoggerFactory.getLogger(classOf[AvroSchemaParsers])

  /** C-style comments are ignored and skipped. */
  protected override val whiteSpace = {
    """(\s|//.*|(?m)/\*(\*(?!/)|[^*])*\*/)+""".r
  }

  /** Ignore comments and white-spaces. */
  override def skipWhitespace = true

  /**
   * Map from fully-qualified Avro name to Avro schema.
   * It is an error to attempt to declare the same Avro name twice.
   */
  private val namedTypes = mutable.Map[String, Schema]()

  /**
   * Gets a parsed schema by (full) name.
   *
   * @param name Full name of the schema.
   * @return the parsed schema with the specified name.
   */
  def get(name: String): Schema = {
    return namedTypes(name)
  }

  /** Parses an optional namespace. */
  private def namespace: Parser[Option[List[String]]] = {
    ("."?) ~ ((ident <~ ".")*) ^^ { parsed =>
      val leadingPeriod = parsed._1
      val nsComponents = parsed._2
      if (nsComponents.isEmpty) {
        if (leadingPeriod.isEmpty) {
          None  // relative
        } else {
          Some(List())
        }
      } else {
        Some(nsComponents)
      }
    }
  }

  /**
   * Parses an Avro name ".name.space.SimpleName".
   */
  private def avroName: Parser[AvroName] = {
    (namespace ~ ident) ^^ { parsed => new AvroName(name = parsed._2, ns = parsed._1) }
  }

  /**
   * Parser for the record declaration prefix.
   * Pre-register the empty record shell, to allow recursive records.
   */
  private def recordDecl: Parser[Schema] = {
    ("record" ~> avroName) ^^ { avroName: AvroName =>
      namedTypes.get(avroName.fullName) match {
        case Some(record) => record
        case None => {
          val name = avroName.name
          val namespace = avroName.nameSpace
          val doc = null
          val isError = false
          val record = Schema.createRecord(name, doc, namespace, isError)
          namedTypes += (avroName.fullName -> record)
          record
        }
      }
    }
  }

  /** Parser for an Avro record declaration. */
  private def record: Parser[Schema] = {
    (recordDecl ~ ("{" ~> (recordField*) <~ "}")) ^^ {
      parsed => {
        val record = parsed._1
        val fields: List[Schema.Field] = parsed._2
        record.setFields(fields.asJava)
        record
      }
    }
  }

  private def avroValueToJsonNode(value: Any, schema: Schema): JsonNode = {
    Log.debug("Avro value as JsonNode for: {}, with schema {}", value.asInstanceOf[Any], schema)
    val baos = new ByteArrayOutputStream()
    val encoder: Encoder = EncoderFactory.get.jsonEncoder(schema, baos)
    val writer: DatumWriter[Any] = new GenericDatumWriter[Any](schema)
    writer.write(value, encoder)
    encoder.flush()
    val jsonStr = new String(baos.toByteArray)
    Log.debug("JSON str = '{}'", jsonStr)
    val mapper: ObjectMapper = new ObjectMapper()
    val factory: JsonFactory = mapper.getJsonFactory()
    val jsonParser: org.codehaus.jackson.JsonParser = factory.createJsonParser(jsonStr)
    val jsonNode: JsonNode = mapper.readTree(jsonParser)
    Log.debug("Default value '{}' converted to JSON node: {}", jsonStr.asInstanceOf[Any], jsonNode)
    jsonNode
  }

  private def avroValueAsJsonNode(schema: Schema): Parser[JsonNode] = {
    firstAvroValue(schema) ^^ {
      value: Any => {
        value match {
          case null => JsonNodeFactory.instance.nullNode
          case value: Any => avroValueToJsonNode(value, schema)
        }
      }
    }
  }

  /** Parser for a single record field. */
  private def recordField: Parser[Schema.Field] = {
    (avroType ~ ident) into {
      parsedTypeName => {
        val schema: Schema = parsedTypeName._1
        val fieldName: String = parsedTypeName._2
        val doc: String = null
        (opt("=" ~> (avroValueAsJsonNode(schema) | jacksonJsonValue)) <~ opt(","|";")) ^^ {
          default: Option[JsonNode] => new Schema.Field(fieldName, schema, doc, default.orNull)
        }
      }
    }
  }

  /** Parser for an Avro enum declaration. */
  private def enum: Parser[Schema] = {
    "enum" ~> avroName ~ ("{" ~> enumSymbols <~ "}") ^^ {
      parsed => {
        val avroName = parsed._1
        if (namedTypes.contains(avroName.fullName)) {
          sys.error("Duplicate Avro name: '%s'".format(avroName.fullName))
        }

        val enumSymbols = parsed._2
        val name = avroName.name
        val namespace = avroName.nameSpace
        val doc = null
        val schema = Schema.createEnum(name, doc, namespace, enumSymbols.asJava)
        namedTypes.put(avroName.fullName, schema)
        schema
      }
    }
  }

  /** Parses a list of identifiers, optionally separated by ';' or ','. */
  private def enumSymbols: Parser[List[String]] = {
    (ident <~ opt(","|";"))*
  }

  /** Parser for an Avro fixed declaration. */
  private def fixed: Parser[Schema] = {
    "fixed" ~> avroName ~ ("(" ~> wholeNumber <~ ")") ^^ {
      parsed => {
        val avroName = parsed._1
        if (namedTypes.contains(avroName.fullName)) {
          sys.error("Duplicate Avro name: '%s'".format(avroName.fullName))
        }

        val name = avroName.name
        val namespace = avroName.nameSpace
        val size = parsed._2.toInt
        val doc = null
        val schema = Schema.createFixed(name, doc, namespace, size)
        namedTypes.put(avroName.fullName, schema)
        schema
      }
    }
  }

  /** Parser for a union schema. */
  private def union: Parser[Schema] = {
    ("union" ~> "{" ~> avroTypeSequence <~ "}") ^^ {
      unionBranches => Schema.createUnion(unionBranches.asJava)
    }
  }

  /** Parser for an Avro schema referenced by name. */
  private def namedSchemaRef: Parser[Schema] = {
    Parser[Schema] { in =>
      avroName(in) match {
        case success: Success[AvroName] => {
          val avroName = success.get
          namedTypes.get(avroName.fullName) match {
            case Some(schema) => Success(schema, success.next)
            case None => Failure("No named schema with name '%s'".format(avroName.fullName), in)
          }
        }
        case result: ParseResult[AvroName] => Failure("Not an Avro name", in)
      }
    }
  }

  /** Parses one Avro type from the input. */
  def avroType: Parser[Schema] = (
      "null" ^^ { _ => Schema.create(Schema.Type.NULL) }
    | "boolean" ^^ { _ => Schema.create(Schema.Type.BOOLEAN) }
    | "int" ^^ { _ => Schema.create(Schema.Type.INT) }
    | "long" ^^ { _ => Schema.create(Schema.Type.LONG) }
    | "float" ^^ { _ => Schema.create(Schema.Type.FLOAT) }
    | "double" ^^ { _ => Schema.create(Schema.Type.DOUBLE) }
    | "string" ^^ { _ => Schema.create(Schema.Type.STRING) }
    | "bytes" ^^ { _ => Schema.create(Schema.Type.BYTES) }

    // Composite unnamed schemas
    | "array" ~> "<" ~> avroType <~ ">" ^^ { arrayItem => Schema.createArray(arrayItem) }
    | "map" ~> "<" ~> avroType <~ ">" ^^ { mapItem => Schema.createMap(mapItem) }

    // Named schemas
    | enum
    | fixed
    | record
    | union
    | namedSchemaRef

    // Allow pre-declaring a record (eg. for mutually recursive records):
    | recordDecl ~> avroType
  )

  /**
   * Parses a sequence of Avro types, optionally separated by ',' or ';'.
   * Used for type unions or sequences.
   */
  def avroTypeSequence: Parser[List[Schema]] = {
    (avroType <~ opt(","|";"))*
  }
}
