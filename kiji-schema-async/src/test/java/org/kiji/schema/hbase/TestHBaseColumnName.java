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

package org.kiji.schema.hbase;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestHBaseColumnName {
  @Test
  public void testHBaseColumnName() {
    HBaseColumnName column = new HBaseColumnName(Bytes.toBytes("foo"), Bytes.toBytes("bar"));
    assertArrayEquals(Bytes.toBytes("foo"), column.getFamily());
    assertEquals("foo", column.getFamilyAsString());
    assertArrayEquals(Bytes.toBytes("bar"), column.getQualifier());
    assertEquals("bar", column.getQualifierAsString());
  }
}
