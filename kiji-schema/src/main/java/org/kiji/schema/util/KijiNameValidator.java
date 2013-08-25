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

import java.util.regex.Pattern;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiInvalidNameException;

/**
 * A utility class for validating layout names,
 * (including table names, locality group names, family names, and column names)
 * and Kiji instance names.
 */
@ApiAudience.Private
public final class KijiNameValidator {
  /** Unused private constructor since this is a utility class. */
  private KijiNameValidator() {}

  /** Regular expression that defines a valid instance name. */
  public static final Pattern VALID_INSTANCE_PATTERN = Pattern.compile("[a-zA-Z0-9_]+");

  /** Regular expression that defines a valid layout name (family, qualifier, table, etc). */
  public static final Pattern VALID_LAYOUT_NAME_PATTERN =
      Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");

  /** Regular expression that defines a valid alias for a layout name. */
  public static final Pattern VALID_ALIAS_PATTERN =
      Pattern.compile("[a-zA-Z0-9_-]+");

  /**
   * @return true if name is a valid name for a table, locality group, family,
   *     or column name, and false otherwise.
   * @param name the name to check.
   */
  public static boolean isValidLayoutName(CharSequence name) {
    return VALID_LAYOUT_NAME_PATTERN.matcher(name).matches();
  }

  /**
   * Determines whether a string is a valid layout name,
   * including table names, locality group names, family names, and column names.
   *
   * @param name The string to validate as a layout name.
   * @throws KijiInvalidNameException If the name is invalid.
   */
  public static void validateLayoutName(CharSequence name) {
    if (!isValidLayoutName(name)) {
      throw new KijiInvalidNameException("Invalid layout name: " + name);
    }
  }

  /**
   * @return true if name is a valid alias for a table, locality group, family,
   *     or column name, and false otherwise.
   * @param name the name to check.
   */
  public static boolean isValidAlias(CharSequence name) {
    return VALID_ALIAS_PATTERN.matcher(name).matches();
  }

  /**
   * Validates characters that may be used in an alias for a qualifier, family, or
   * locality group. This is a superset of valid characters for a layout name.
   *
   * @param name The string to validate as a layout name alias.
   * @throws KijiInvalidNameException If the name is invalid.
   */
  public static void validateAlias(CharSequence name) {
    if (!isValidAlias(name)) {
      throw new KijiInvalidNameException("Invalid alias: " + name);
    }
  }

  /**
   * @return true if name is a valid name for a Kiji instance and false otherwise.
   * @param name the name to check.
   */
  public static boolean isValidKijiName(CharSequence name) {
    return VALID_INSTANCE_PATTERN.matcher(name).matches();
  }

  /**
   * Determines whether a string is a valid Kiji instance name.
   *
   * @param name The string to validate as a Kiji instance name.
   * @throws KijiInvalidNameException If the name is invalid.
   */
  public static void validateKijiName(CharSequence name) {
    if (!isValidKijiName(name)) {
      throw new KijiInvalidNameException("Invalid instance name: " + name);
    }
  }
}
