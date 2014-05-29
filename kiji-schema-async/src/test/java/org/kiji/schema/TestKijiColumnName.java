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
      new KijiColumnName(null);
      fail("An exception should have been thrown.");
    } catch (IllegalArgumentException iae) {
      assertEquals("Column name may not be null. At least specify family", iae.getMessage());
    }
  }

  @Test
  public void testNullFamily() {
    try {
      new KijiColumnName(null, "qualifier");
      fail("An exception should have been thrown.");
    } catch (IllegalArgumentException iae) {
      assertEquals("Family name may not be null.", iae.getMessage());
    }
  }

  @Test
  public void testMapFamily() {
    KijiColumnName columnName = new KijiColumnName("family");
    assertEquals("family", columnName.getFamily());
    assertArrayEquals(Bytes.toBytes("family"), columnName.getFamilyBytes());
    assertNull(columnName.getQualifier());
    assertFalse(columnName.isFullyQualified());
  }

  @Test
  public void testEmptyQualifier() {
    KijiColumnName columnName = new KijiColumnName("family:");
    assertEquals("family", columnName.getFamily());
    assertNull(columnName.getQualifier());
    assertArrayEquals(Bytes.toBytes("family"), columnName.getFamilyBytes());
    assertNull(columnName.getQualifierBytes());
    assertFalse(columnName.isFullyQualified());
  }

  @Test
  public void testEmptyQualifierTwo() {
    KijiColumnName columnName = new KijiColumnName("family", "");
    assertEquals("family", columnName.getFamily());
    assertNull(columnName.getQualifier());
    assertArrayEquals(Bytes.toBytes("family"), columnName.getFamilyBytes());
    assertNull(columnName.getQualifierBytes());
    assertFalse(columnName.isFullyQualified());
  }

  @Test
  public void testNormal() {
    KijiColumnName columnName = new KijiColumnName("family:qualifier");
    assertEquals("family", columnName.getFamily());
    assertEquals("qualifier", columnName.getQualifier());
    assertArrayEquals(Bytes.toBytes("family"), columnName.getFamilyBytes());
    assertArrayEquals(Bytes.toBytes("qualifier"), columnName.getQualifierBytes());
    assertTrue(columnName.isFullyQualified());
  }

  @Test
  public void testNullQualifier() {
    KijiColumnName columnName = new KijiColumnName("family", null);
    assertEquals("family", columnName.getFamily());
    assertNull(columnName.getQualifier());
    assertArrayEquals(Bytes.toBytes("family"), columnName.getFamilyBytes());
    assertNull(columnName.getQualifierBytes());
    assertFalse(columnName.isFullyQualified());
  }

  @Test
  public void testInvalidFamilyName() {
    try {
      new KijiColumnName("1:qualifier");
      fail("An exception should have been thrown.");
    } catch (KijiInvalidNameException kine) {
      assertEquals("Invalid family name: 1 Name must match pattern: [a-zA-Z_][a-zA-Z0-9_]*",
          kine.getMessage());
    }
  }

  @Test
  public void testEquals() {
    KijiColumnName columnA = new KijiColumnName("family", "qualifier1");
    KijiColumnName columnC = new KijiColumnName("family", null);
    KijiColumnName columnD = new KijiColumnName("family", "qualifier2");
    assertTrue(columnA.equals(columnA)); // reflexive
    assertFalse(columnA.equals(columnC) || columnC.equals(columnA));
    assertFalse(columnA.equals(columnD) || columnD.equals(columnA));
  }

  @Test
  public void testHashCode() {
    KijiColumnName columnA = new KijiColumnName("family", "qualifier");
    KijiColumnName columnB = new KijiColumnName("family:qualifier");
    assertEquals(columnA.hashCode(), columnB.hashCode());
  }

  @Test
  public void testCompareTo() {
    KijiColumnName columnA = new KijiColumnName("family");
    KijiColumnName columnB = new KijiColumnName("familyTwo");
    KijiColumnName columnC = new KijiColumnName("family:qualifier");
    assertTrue(0 == columnA.compareTo(columnA));
    assertTrue(0 > columnA.compareTo(columnB) && 0 < columnB.compareTo(columnA));
    assertTrue(0 > columnA.compareTo(columnC) && 0 < columnC.compareTo(columnA));
    assertTrue(0 < columnB.compareTo(columnC) && 0 > columnC.compareTo(columnB));
  }
}
