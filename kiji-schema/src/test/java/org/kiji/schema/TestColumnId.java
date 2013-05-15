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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import org.kiji.schema.layout.impl.ColumnId;

public class TestColumnId {
  @Test
  public void testNegativeId() {
    try {
      new ColumnId(-1).toByteArray();
      fail("Should fail with IllegalArgumentException.");
    } catch (IllegalArgumentException ise) {
      assertEquals("id may not be negative", ise.getMessage());
    }
  }

  @Test
  public void testToByteArray() {
    assertArrayEquals(Bytes.toBytes("A"), new ColumnId(0).toByteArray());
    assertArrayEquals(Bytes.toBytes("B"), new ColumnId(1).toByteArray());
    assertArrayEquals(Bytes.toBytes("C"), new ColumnId(2).toByteArray());
    assertArrayEquals(Bytes.toBytes("/"), new ColumnId(63).toByteArray());
    assertArrayEquals(Bytes.toBytes("AB"), new ColumnId(64).toByteArray());
    assertArrayEquals(Bytes.toBytes("BB"), new ColumnId(65).toByteArray());
    assertArrayEquals(Bytes.toBytes("CB"), new ColumnId(66).toByteArray());
    assertArrayEquals(Bytes.toBytes("oP"), new ColumnId(1000).toByteArray());
    assertArrayEquals(Bytes.toBytes("AAB"), new ColumnId(64 * 64).toByteArray());
  }

  @Test
  public void testFromByteArray() {
    assertEquals(0, ColumnId.fromByteArray(Bytes.toBytes("A")).getId());
    assertEquals(1, ColumnId.fromByteArray(Bytes.toBytes("B")).getId());
    assertEquals(2, ColumnId.fromByteArray(Bytes.toBytes("C")).getId());
    assertEquals(63, ColumnId.fromByteArray(Bytes.toBytes("/")).getId());
    assertEquals(64, ColumnId.fromByteArray(Bytes.toBytes("AB")).getId());
    assertEquals(65, ColumnId.fromByteArray(Bytes.toBytes("BB")).getId());
    assertEquals(66, ColumnId.fromByteArray(Bytes.toBytes("CB")).getId());
    assertEquals(1000, ColumnId.fromByteArray(Bytes.toBytes("oP")).getId());
    assertEquals(64 * 64, ColumnId.fromByteArray(Bytes.toBytes("AAB")).getId());
  }
}
