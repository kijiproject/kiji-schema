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

import org.apache.commons.lang.StringUtils;

import org.kiji.annotations.ApiAudience;

/**
 * A utility class for dealing with identifiers in the Java language.
 */
@ApiAudience.Private
public final class JavaIdentifiers {
  /** Disable constructor for this utility class. */
  private JavaIdentifiers() {}

  /**
   * Determines whether a string is a valid Java identifier.
   *
   * <p>A valid Java identifier may not start with a number, but may contain any
   * combination of letters, digits, underscores, or dollar signs.</p>
   *
   * <p>See the <a href="http://java.sun.com/docs/books/jls/third_edition/html/lexical.html#3.8">
   * Java Language Specification</a></p>
   *
   * @param identifier The identifier to test for validity.
   * @return Whether the identifier was valid.
   */
  public static boolean isValidIdentifier(String identifier) {
    if (identifier.isEmpty() || !Character.isJavaIdentifierStart(identifier.charAt(0))) {
      return false;
    }
    for (int i = 1; i < identifier.length(); i++) {
      if (!Character.isJavaIdentifierPart(identifier.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Determines whether a string could be the name of a Java class.
   *
   * <p>If this method returns true, it does not necessarily mean that the Java class with
   * <code>className</code> exists; it only means that one could write a Java class with
   * that fully-qualified name.</p>
   *
   * @param className A string to test.
   * @return Whether the class name was valid.
   */
  public static boolean isValidClassName(String className) {
    // A valid class name is a bunch of valid Java identifiers separated by dots.
    for (String part : StringUtils.splitByWholeSeparatorPreserveAllTokens(className, ".")) {
      if (!isValidIdentifier(part)) {
        return false;
      }
    }
    return true;
  }
}
