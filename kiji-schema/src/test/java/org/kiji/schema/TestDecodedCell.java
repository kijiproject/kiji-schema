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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
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
  public void testEquals() {
    final DecodedCell<Integer> int1 = new DecodedCell<Integer>(SCHEMA_INT, 5);
    final DecodedCell<Integer> int2 = new DecodedCell<Integer>(SCHEMA_INT, 5);
    final DecodedCell<Integer> int3 = new DecodedCell<Integer>(SCHEMA_INT, 6);
    final DecodedCell<Long> long1 = new DecodedCell<Long>(SCHEMA_LONG, 5L);

    assertEquals(int1, int2);
    assertThat(int1, is(not(int3)));
    assertFalse(int1.equals(long1));

    final DecodedCell<CharSequence> cs1 = new DecodedCell<CharSequence>(SCHEMA_STRING, "foo");
    final DecodedCell<CharSequence> cs2 =
        new DecodedCell<CharSequence>(SCHEMA_STRING, new Utf8("foo"));
    assertEquals(cs1, cs2);
  }
}
