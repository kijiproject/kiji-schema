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

public class TestJavaIdentifiers {
  @Test
  public void testIsValidIdentifier() {
    assertFalse(JavaIdentifiers.isValidIdentifier(""));
    assertTrue(JavaIdentifiers.isValidIdentifier("a"));
    assertTrue(JavaIdentifiers.isValidIdentifier("B"));
    assertTrue(JavaIdentifiers.isValidIdentifier("_"));
    assertFalse(JavaIdentifiers.isValidIdentifier("-"));
    assertFalse(JavaIdentifiers.isValidIdentifier("."));
    assertFalse(JavaIdentifiers.isValidIdentifier("0"));
    assertTrue(JavaIdentifiers.isValidIdentifier("_1"));
    assertTrue(JavaIdentifiers.isValidIdentifier("_c"));
    assertTrue(JavaIdentifiers.isValidIdentifier("_giw07nf"));
    assertTrue(JavaIdentifiers.isValidIdentifier("giw07nf"));
    assertFalse(JavaIdentifiers.isValidIdentifier("2giw07nf"));
  }

  @Test
  public void testIsValidClassName() {
    assertTrue(JavaIdentifiers.isValidClassName("org.kiji.schema.Foo"));
    assertFalse(JavaIdentifiers.isValidClassName("org.kiji.schema..Foo"));
    assertTrue(JavaIdentifiers.isValidClassName("org.kiji.schema.Foo$Bar"));
    assertTrue(JavaIdentifiers.isValidClassName("Foo"));
    assertFalse(JavaIdentifiers.isValidClassName("Foo."));
    assertFalse(JavaIdentifiers.isValidClassName(".Foo"));
    assertTrue(JavaIdentifiers.isValidClassName("_Foo"));
    assertTrue(JavaIdentifiers.isValidClassName("com._Foo"));
    assertFalse(JavaIdentifiers.isValidClassName("com.8Foo"));
    assertTrue(JavaIdentifiers.isValidClassName("com.Foo8"));
  }
}
