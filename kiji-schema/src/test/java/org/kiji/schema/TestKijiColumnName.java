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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestKijiColumnName {
  @Test
  public void testNull() {
    try {
      KijiColumnName.create(null);
      fail("An exception should have been thrown.");
    } catch (IllegalArgumentException iae) {
      assertEquals("Column name may not be null. At least specify family", iae.getMessage());
    }
  }

  @Test
  public void testNullFamily() {
    try {
      KijiColumnName.create(null, "qualifier");
      fail("An exception should have been thrown.");
    } catch (IllegalArgumentException iae) {
      assertEquals("Family name may not be null.", iae.getMessage());
    }
  }

  @Test
  public void testMapFamily() {
    KijiColumnName columnName = KijiColumnName.create("family");
    assertEquals("family", columnName.getFamily());
    assertArrayEquals(Bytes.toBytes("family"), columnName.getFamilyBytes());
    assertNull(columnName.getQualifier());
    assertFalse(columnName.isFullyQualified());
  }

  @Test
  public void testEmptyQualifier() {
    KijiColumnName columnName = KijiColumnName.create("family:");
    assertEquals("family", columnName.getFamily());
    assertNull(columnName.getQualifier());
    assertArrayEquals(Bytes.toBytes("family"), columnName.getFamilyBytes());
    assertNull(columnName.getQualifierBytes());
    assertFalse(columnName.isFullyQualified());
  }

  @Test
  public void testEmptyQualifierTwo() {
    KijiColumnName columnName = KijiColumnName.create("family", "");
    assertEquals("family", columnName.getFamily());
    assertNull(columnName.getQualifier());
    assertArrayEquals(Bytes.toBytes("family"), columnName.getFamilyBytes());
    assertNull(columnName.getQualifierBytes());
    assertFalse(columnName.isFullyQualified());
  }

  @Test
  public void testNormal() {
    KijiColumnName columnName = KijiColumnName.create("family:qualifier");
    assertEquals("family", columnName.getFamily());
    assertEquals("qualifier", columnName.getQualifier());
    assertArrayEquals(Bytes.toBytes("family"), columnName.getFamilyBytes());
    assertArrayEquals(Bytes.toBytes("qualifier"), columnName.getQualifierBytes());
    assertTrue(columnName.isFullyQualified());
  }

  @Test
  public void testNullQualifier() {
    KijiColumnName columnName = KijiColumnName.create("family", null);
    assertEquals("family", columnName.getFamily());
    assertNull(columnName.getQualifier());
    assertArrayEquals(Bytes.toBytes("family"), columnName.getFamilyBytes());
    assertNull(columnName.getQualifierBytes());
    assertFalse(columnName.isFullyQualified());
  }

  @Test
  public void testInvalidFamilyName() {
    try {
      KijiColumnName.create("1:qualifier");
      fail("An exception should have been thrown.");
    } catch (KijiInvalidNameException kine) {
      assertEquals("Invalid family name: 1 Name must match pattern: [a-zA-Z_][a-zA-Z0-9_]*",
          kine.getMessage());
    }
  }

  @Test
  public void testEquals() {
    KijiColumnName columnA = KijiColumnName.create("family", "qualifier1");
    KijiColumnName columnC = KijiColumnName.create("family", null);
    KijiColumnName columnD = KijiColumnName.create("family", "qualifier2");
    assertTrue(columnA.equals(columnA)); // reflexive
    assertFalse(columnA.equals(columnC) || columnC.equals(columnA));
    assertFalse(columnA.equals(columnD) || columnD.equals(columnA));
  }

  @Test
  public void testHashCode() {
    KijiColumnName columnA = KijiColumnName.create("family", "qualifier");
    KijiColumnName columnB = KijiColumnName.create("family:qualifier");
    assertEquals(columnA.hashCode(), columnB.hashCode());
  }

  @Test
  public void testCompareTo() {
    KijiColumnName columnA = KijiColumnName.create("family");
    KijiColumnName columnB = KijiColumnName.create("familyTwo");
    KijiColumnName columnC = KijiColumnName.create("family:qualifier");
    assertTrue(0 == columnA.compareTo(columnA));
    assertTrue(0 > columnA.compareTo(columnB) && 0 < columnB.compareTo(columnA));
    assertTrue(0 > columnA.compareTo(columnC) && 0 < columnC.compareTo(columnA));
    assertTrue(0 < columnB.compareTo(columnC) && 0 > columnC.compareTo(columnB));
  }
}
