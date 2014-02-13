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

import scala.util.parsing.input.CharSequenceReader

import org.apache.avro.Schema
import org.apache.commons.lang.StringEscapeUtils

/**
 * Avro parser: a parser is stateful and accumulates definitions.
 */
class AvroSchemaParser extends AvroSchemaParsers {

  /**
   * Parse a text IDL declaration for a single Avro type.
   *
   * @param text IDL definition to parse into an Avro schema.
   * @return the Avro schema parsed from the IDL definition.
   */
  def parse(text: String): Schema = {
    val input: Input = new CharSequenceReader(text)
    singleType(input) match {
      case success: Success[Schema] => success.get
      case error: Error => sys.error("Error parsing '%s': %s".format(text, error.msg))
      case error => sys.error("Error parsing '%s': %s".format(text, error))
    }
  }

  /**
   * Parse a text IDL declaration for an Avro type.
   *
   * @param text IDL definition to parse into an Avro schema.
   * @return the Avro schema sequence parsed from the IDL definition.
   */
  def parseSequence(text: String): Seq[Schema] = {
    val input: Input = new CharSequenceReader(text)
    typeSequence(input) match {
      case success: Success[Seq[Schema]] => success.get
      case error: Error =>
        sys.error("Parse error in '%s': '%s'".format(text, error.msg))
      case result => sys.error("Parse error in '%s': '%s'"
          .format(StringEscapeUtils.escapeJava(text), result))
    }
  }

  /** Parser for exactly one Avro type, and not trailer allowed. */
  private def singleType: Parser[Schema] = {
    phrase(avroType(Context()))
  }

  /** Parses a sequence of Avro types. */
  private def typeSequence: Parser[Seq[Schema]] = {
    phrase(avroTypeSequence(Context()))
  }
}
