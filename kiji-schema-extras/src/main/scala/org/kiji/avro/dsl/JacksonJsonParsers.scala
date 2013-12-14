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

import scala.util.parsing.combinator.JavaTokenParsers
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.node.JsonNodeFactory
import org.slf4j.Logger

/**
 * Parses JSON strings into Jackson nodes.
 */
trait JacksonJsonParser
    extends JavaTokenParsers
    with StringParsers {
  val Log: Logger

  /** Parser for JSON value. */
  def jacksonJsonValue: Parser[JsonNode] = {
    ( "null" ^^ { _ => JsonNodeFactory.instance.nullNode }
    | "true" ^^ { _ => JsonNodeFactory.instance.booleanNode(true) }
    | "false" ^^ { _ => JsonNodeFactory.instance.booleanNode(false) }
    | wholeNumber ^^ { number => JsonNodeFactory.instance.numberNode(number.toLong) }
    | floatingPointNumber ^^ { number => JsonNodeFactory.instance.numberNode(number.toDouble) }
    | quotedStringLiteral ^^ { literal => JsonNodeFactory.instance.textNode(literal) }
    | "[" ~> jsonValues <~ "]" ^^ {
      values => {
        val array = JsonNodeFactory.instance.arrayNode()
        for (value <- values) { array.add(value) }
        array
      }
    }
    | "{" ~> jsonEntries <~ "}" ^^ {
      entries: List[(String, JsonNode)] => {
        val jsonObject = JsonNodeFactory.instance.objectNode()
        for ((key, value) <- entries) {
          jsonObject.put(key, value)
        }
        jsonObject
      }
    }
    )
  }

  /** Parser for comma-separated list of JSON values. */
  private def jsonValues: Parser[List[JsonNode]] = {
    ((jacksonJsonValue <~ ",")*) ~ (jacksonJsonValue?) ^^ { parsed => parsed._1 ++ parsed._2 }
  }

  /** Parser for a single JSON entry (key, value) where the key must be a string. */
  private def jsonEntry: Parser[(String, JsonNode)] = {
    (quotedStringLiteral ~ (":" ~> jacksonJsonValue)) ^^ {
      parsed => {
        val key: String = parsed._1
        val value: JsonNode = parsed._2
        (key, value)
      }
    }
  }

  /** Parser for a list of JSON entries (part of a JSON object). */
  private def jsonEntries: Parser[List[(String, JsonNode)]] = {
    ((jsonEntry <~ ",")*) ~ (jsonEntry?) ^^ { parsed => parsed._1 ++ parsed._2 }
  }
}
