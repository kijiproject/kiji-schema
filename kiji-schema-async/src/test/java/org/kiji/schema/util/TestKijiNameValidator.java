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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestKijiNameValidator {
  @Test
  public void testIsValidIdentifier() {
    assertFalse(KijiNameValidator.isValidLayoutName(""));
    assertFalse(KijiNameValidator.isValidLayoutName("0123"));
    assertFalse(KijiNameValidator.isValidLayoutName("0123abc"));
    assertFalse(KijiNameValidator.isValidLayoutName("-asdb"));
    assertFalse(KijiNameValidator.isValidLayoutName("abc-def"));
    assertFalse(KijiNameValidator.isValidLayoutName("abcdef$"));
    assertFalse(KijiNameValidator.isValidLayoutName("abcdef-"));
    assertFalse(KijiNameValidator.isValidLayoutName("abcdef("));
    assertFalse(KijiNameValidator.isValidLayoutName("(bcdef"));
    assertFalse(KijiNameValidator.isValidLayoutName("(bcdef)"));

    assertTrue(KijiNameValidator.isValidLayoutName("_"));
    assertTrue(KijiNameValidator.isValidLayoutName("foo"));
    assertTrue(KijiNameValidator.isValidLayoutName("FOO"));
    assertTrue(KijiNameValidator.isValidLayoutName("FooBar"));
    assertTrue(KijiNameValidator.isValidLayoutName("foo123"));
    assertTrue(KijiNameValidator.isValidLayoutName("foo_bar"));

    assertFalse(KijiNameValidator.isValidLayoutName("abc:def"));
    assertFalse(KijiNameValidator.isValidLayoutName("abc\\def"));
    assertFalse(KijiNameValidator.isValidLayoutName("abc/def"));
    assertFalse(KijiNameValidator.isValidLayoutName("abc=def"));
    assertFalse(KijiNameValidator.isValidLayoutName("abc+def"));
  }

  @Test
  public void testIsValidAlias() {
    assertFalse(KijiNameValidator.isValidAlias(""));
    assertTrue(KijiNameValidator.isValidAlias("0123")); // Digits are ok in a leading capacity.
    assertTrue(KijiNameValidator.isValidAlias("0123abc"));
    assertTrue(KijiNameValidator.isValidAlias("abc-def")); // Dashes are ok in aliases...
    assertTrue(KijiNameValidator.isValidAlias("-asdb")); // Even as the first character.
    assertTrue(KijiNameValidator.isValidAlias("asdb-")); // Even as the first character.
    assertFalse(KijiNameValidator.isValidAlias("abcdef("));
    assertFalse(KijiNameValidator.isValidAlias("(bcdef"));
    assertFalse(KijiNameValidator.isValidAlias("(bcdef)"));

    assertTrue(KijiNameValidator.isValidAlias("_"));
    assertTrue(KijiNameValidator.isValidAlias("foo"));
    assertTrue(KijiNameValidator.isValidAlias("FOO"));
    assertTrue(KijiNameValidator.isValidAlias("FooBar"));
    assertTrue(KijiNameValidator.isValidAlias("foo123"));
    assertTrue(KijiNameValidator.isValidAlias("foo_bar"));

    assertFalse(KijiNameValidator.isValidAlias("abc:def"));
    assertFalse(KijiNameValidator.isValidAlias("abc\\def"));
    assertFalse(KijiNameValidator.isValidAlias("abc/def"));
    assertFalse(KijiNameValidator.isValidAlias("abc=def"));
    assertFalse(KijiNameValidator.isValidAlias("abc+def"));
  }

  @Test
  public void testIsValidInstanceName() {
    assertFalse(KijiNameValidator.isValidKijiName("")); // empty string is disallowed here.
    assertTrue(KijiNameValidator.isValidKijiName("0123")); // leading digits are okay.
    assertTrue(KijiNameValidator.isValidKijiName("0123abc")); // leading digits are okay.
    assertFalse(KijiNameValidator.isValidKijiName("-asdb"));
    assertFalse(KijiNameValidator.isValidKijiName("abc-def"));
    assertFalse(KijiNameValidator.isValidKijiName("abcd-"));
    assertFalse(KijiNameValidator.isValidKijiName("abcd$"));

    assertTrue(KijiNameValidator.isValidKijiName("_"));
    assertTrue(KijiNameValidator.isValidKijiName("foo"));
    assertTrue(KijiNameValidator.isValidKijiName("FOO"));
    assertTrue(KijiNameValidator.isValidKijiName("FooBar"));
    assertTrue(KijiNameValidator.isValidKijiName("foo123"));
    assertTrue(KijiNameValidator.isValidKijiName("foo_bar"));

    assertFalse(KijiNameValidator.isValidKijiName("abc:def"));
    assertFalse(KijiNameValidator.isValidKijiName("abc\\def"));
    assertFalse(KijiNameValidator.isValidKijiName("abc/def"));
    assertFalse(KijiNameValidator.isValidKijiName("abc=def"));
    assertFalse(KijiNameValidator.isValidKijiName("abc+def"));
  }
}
