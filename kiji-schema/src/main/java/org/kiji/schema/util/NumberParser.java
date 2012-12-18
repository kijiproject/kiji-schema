/**
 * (c) Copyright 2012 WibiData, Inc.
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

import org.kiji.annotations.ApiAudience;

/**
 * Utility for parsing strings to numbers.  It allows certain numbers to be
 * represented by easy-to-write strings.
 */
@ApiAudience.Private
public final class NumberParser {
  /** Utility is not instantiable. */
  private NumberParser() {}

  private static final String INFINITY_TOKEN = "infinity";

  /**
   * This parseInt allows Integer.MAX_VALUE to be written as a convenient string.
   *
   * @param intToParse The string representation of an int to parse.
   * @return The parsed int.
   */
  public static int parseInt(String intToParse) {
    if (INFINITY_TOKEN.equals(intToParse.trim())) {
      return Integer.MAX_VALUE;
    } else {
      return Integer.parseInt(intToParse);
    }
  }

  /**
   * Converts an integer to a string that uses "infinity" as Integer.MAX_VALUE.
   *
   * @param intToPrint The integer to print.
   * @return The integer as a string, or "infinity".
   */
  public static String printInt(int intToPrint) {
    if (Integer.MAX_VALUE == intToPrint) {
      return INFINITY_TOKEN;
    }
    return Integer.toString(intToPrint);
  }

  /**
   * This parseLong allows Long.MAX_VALUE to be written as a convenient string.
   *
   * @param longToParse The string representation of a long to parse.
   * @return The parsed long.
   */
  public static long parseLong(String longToParse) {
    if (INFINITY_TOKEN.equals(longToParse.trim())) {
      return Long.MAX_VALUE;
    } else {
      return Long.parseLong(longToParse);
    }
  }
}
