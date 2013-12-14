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

import java.util.{List => JList}

import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.avro.Schema

/**
 * Provides a pure Java interface to the DSL for Avro schemas and values.
 */
class JavaAvroDSL extends AvroDSLInterface {
  private val schemaParser = new AvroSchemaParser()

  override def parseSchema(text: String): Schema = {
    return schemaParser.parse(text)
  }

  override def parseSchemaSequence(text: String): JList[Schema] = {
    return schemaParser.parseSequence(text).asJava
  }

  override def schemaToString(schema: Schema): String = {
    return AvroSchema.toString(schema)
  }

  override def parseValue[T](text: String, schema: Schema): T = {
    return AvroValueParser.parse(text, schema)
  }

  override def valueToString[T](value: T, schema: Schema): String = {
    return AvroValue.toString(value, schema)
  }
}