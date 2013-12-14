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

import org.apache.commons.lang.StringEscapeUtils
import scala.util.parsing.combinator.RegexParsers
import org.slf4j.Logger

/**
 * Parser combinators for various string literals.
 */
trait StringParsers extends RegexParsers {
  val Log: Logger

  /** Parser for a string literal surrounded by single quotes. */
  def singleQuoteStringLiteral: Parser[String] = {
    ("""[']([^'\p{Cntrl}\\]|\\[\\/bfnrt'"]|\\u[a-fA-F0-9]{4})*[']""").r ^^ {
      literal: String => {
        val str = unescapeStringLiteral(literal.slice(1, literal.length - 1))
        Log.debug("Parsed single quoted string: '{}'", str)
        str
      }
    }
  }

  /** Parser for a string literal surrounded by double quotes. */
  def doubleQuoteStringLiteral: Parser[String] = {
    ("""["]([^"\p{Cntrl}\\]|\\[\\/bfnrt'"]|\\u[a-fA-F0-9]{4})*["]""").r ^^ {
      literal: String => {
        val str = unescapeStringLiteral(literal.slice(1, literal.length - 1))
        Log.debug("Parsed double quoted string: '{}'", str)
        str
      }
    }
  }

  /** Parser for a string literal surrounded by 3 double quotes. */
  def tripleDoubleQuoteStringLiteral: Parser[String] = {
    (""""{3}([^\\]|\\[\\/bfnrt'"]|\\u[a-fA-F0-9]{4})*"{3}""").r ^^ {
      literal: String => {
        val str = unescapeStringLiteral(literal.slice(3, literal.length - 3))
        Log.debug("Parsed triple quoted string: '{}'", str)
        str
      }
    }
  }

  /** Parser for a quoted string literal. */
  def quotedStringLiteral: Parser[String] = {
    // Note: the triple-quoted string must be first
    (tripleDoubleQuoteStringLiteral | singleQuoteStringLiteral | doubleQuoteStringLiteral)
  }

  /**
   * Unescape a string literal.
   *
   * @param literal Escaped string literal (includes leading and trailing quotes).
   * @return the unescaped string literal.
   */
  private def unescapeStringLiteral(escaped: String): String = {
    return StringEscapeUtils.unescapeJavaScript(escaped)
  }

}
