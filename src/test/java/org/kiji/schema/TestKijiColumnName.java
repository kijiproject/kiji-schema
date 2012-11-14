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

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestKijiColumnName {
  @Test(expected=NullPointerException.class)
  public void testNull() {
    new KijiColumnName(null);
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
    assertArrayEquals(Bytes.toBytes("family"), columnName.getFamilyBytes());
    assertTrue(columnName.getQualifier().isEmpty());
    assertTrue(columnName.isFullyQualified());
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
}
