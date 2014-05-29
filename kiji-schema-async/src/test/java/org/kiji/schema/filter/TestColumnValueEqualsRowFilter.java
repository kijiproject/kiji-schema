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

package org.kiji.schema.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.avro.Schema;
import org.junit.Test;

import org.kiji.schema.DecodedCell;

public class TestColumnValueEqualsRowFilter {
  private static final Schema SCHEMA_INT = Schema.create(Schema.Type.INT);

  @Test
  public void testEqualsAndHashCode() {
    final DecodedCell<Integer> value5a = new DecodedCell<Integer>(SCHEMA_INT, 5);
    final DecodedCell<Integer> value5b = new DecodedCell<Integer>(SCHEMA_INT, 5);
    final DecodedCell<Integer> value6 = new DecodedCell<Integer>(SCHEMA_INT, 6);
    final ColumnValueEqualsRowFilter filter5a =
        new ColumnValueEqualsRowFilter("family", "qualifier", value5a);
    final ColumnValueEqualsRowFilter filter5b =
        new ColumnValueEqualsRowFilter("family", "qualifier", value5b);
    final ColumnValueEqualsRowFilter filter6 =
        new ColumnValueEqualsRowFilter("family", "qualifier", value6);

    assertEquals(filter5a, filter5b);
    assertFalse(filter5a.equals(filter6));

    assertEquals(filter5a.hashCode(), filter5b.hashCode());
  }
}
