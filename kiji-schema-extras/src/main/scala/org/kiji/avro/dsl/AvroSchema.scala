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

import org.apache.avro.Schema
import org.apache.avro.Schema.Type

/**
 * Serializes an Avro schema object back to its DSL representation.
 */
object AvroSchema {

  /**
   * Serializes an Avro schema back to its DSL string representation.
   *
   * @param schema Avro schema to represent as a DSL Avro schema definition.
   * @return the DSL text representation of the given Avro schema.
   */
  def toString(schema: Schema): String = {
    schema.getType match {
      case Type.NULL => {
        return "null"
      }
      case Type.BOOLEAN => {
        return "boolean"
      }
      case Type.INT => {
        return "int"
      }
      case Type.LONG => {
        return "long"
      }
      case Type.FLOAT => {
        return "float"
      }
      case Type.DOUBLE => {
        return "double"
      }
      case Type.BYTES => {
        return "bytes"
      }
      case Type.STRING =>
        return "string"
      case Type.FIXED => {
        return "fixed %s(%s)".format(schema.getFullName, schema.getFixedSize)
      }
      case Type.ENUM => {
        return "enum %s{%s}".format(
            schema.getFullName,
            schema.getEnumSymbols.asScala.mkString(",")
        )
      }
      case Type.RECORD => {
        val fields = schema.getFields.asScala.map {
          field: Schema.Field =>
            Option(field.defaultValue) match {
              case None =>
                  "%s %s".format(toString(field.schema), field.name)
              case Some(default) =>
                  "%s %s = %s".format(toString(field.schema), field.name, default)
            }
        }
        return "record %s{%s}".format(
            schema.getFullName,
            fields.mkString(",")
        )
      }
      case Type.UNION => {
        return "union{%s}".format(schema.getTypes.asScala.map { toString } mkString(","))
      }
      case Type.ARRAY => {
        return "array<%s>".format(toString(schema.getElementType))
      }
      case Type.MAP => {
        return "map<%s>".format(toString(schema.getValueType))
      }
      case _ => {
        sys.error("Unknown/unexpected schema type: " + schema)
      }
    }
  }
}