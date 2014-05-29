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

package org.kiji.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class TestDecodedCell {
  private static final Schema SCHEMA_INT = Schema.create(Schema.Type.INT);
  private static final Schema SCHEMA_LONG = Schema.create(Schema.Type.LONG);
  private static final Schema SCHEMA_STRING = Schema.create(Schema.Type.STRING);

  @Test
  public void testEqualsAndHashCode() {
    final DecodedCell<Integer> int5a = new DecodedCell<Integer>(SCHEMA_INT, 5);
    final DecodedCell<Integer> int5b = new DecodedCell<Integer>(SCHEMA_INT, 5);
    final DecodedCell<Integer> int6 = new DecodedCell<Integer>(SCHEMA_INT, 6);
    final DecodedCell<Long> long1 = new DecodedCell<Long>(SCHEMA_LONG, 5L);
    final DecodedCell<CharSequence> csFoo1 = new DecodedCell<CharSequence>(SCHEMA_STRING, "foo");
    final DecodedCell<CharSequence> csFoo2 =
        new DecodedCell<CharSequence>(SCHEMA_STRING, new Utf8("foo"));
    final DecodedCell<CharSequence> csBar =
        new DecodedCell<CharSequence>(SCHEMA_STRING, new Utf8("bar"));

    assertEquals(int5a, int5b);

    assertEquals(csFoo1, csFoo2);

    assertFalse(csFoo1.equals(csBar));
    assertFalse(int5a.equals(int6));

    // Cells with different schema are not equal.
    assertFalse(int5a.equals(long1));
    assertFalse(csFoo1.equals(int5a));

    // Cells that are equal have the same hashcode.
    assertEquals(int5a.hashCode(), int5b.hashCode());
    assertEquals(csFoo1.hashCode(), csFoo2.hashCode());
  }
}
