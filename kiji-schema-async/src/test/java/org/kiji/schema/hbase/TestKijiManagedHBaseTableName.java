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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestKijiManagedHBaseTableName {
  @Test
  public void testMetaTable() {
    KijiManagedHBaseTableName tableName = KijiManagedHBaseTableName.getMetaTableName("default");
    assertEquals("kiji.default.meta", tableName.toString());
    assertArrayEquals(Bytes.toBytes("kiji.default.meta"), tableName.toBytes());
  }

  @Test
  public void testSchemaTable() {
    final KijiManagedHBaseTableName hashTableName =
        KijiManagedHBaseTableName.getSchemaHashTableName("default");
    assertEquals("kiji.default.schema_hash", hashTableName.toString());
    assertArrayEquals(Bytes.toBytes("kiji.default.schema_hash"), hashTableName.toBytes());

    final KijiManagedHBaseTableName idTableName =
        KijiManagedHBaseTableName.getSchemaIdTableName("default");
    assertEquals("kiji.default.schema_id", idTableName.toString());
    assertArrayEquals(Bytes.toBytes("kiji.default.schema_id"), idTableName.toBytes());
  }

  @Test
  public void testSystemTable() {
    KijiManagedHBaseTableName tableName = KijiManagedHBaseTableName.getSystemTableName("default");
    assertEquals("kiji.default.system", tableName.toString());
    assertArrayEquals(Bytes.toBytes("kiji.default.system"), tableName.toBytes());
  }

  @Test
  public void testUserTable() {
    KijiManagedHBaseTableName tableName
        = KijiManagedHBaseTableName.getKijiTableName("default", "foo");
    assertEquals("kiji.default.table.foo", tableName.toString());
    assertArrayEquals(Bytes.toBytes("kiji.default.table.foo"), tableName.toBytes());
  }

  @Test
  public void testEquals() {
    KijiManagedHBaseTableName foo = KijiManagedHBaseTableName.getKijiTableName("default", "foo");
    KijiManagedHBaseTableName bar = KijiManagedHBaseTableName.getKijiTableName("default", "bar");
    KijiManagedHBaseTableName bar2 = KijiManagedHBaseTableName.getKijiTableName("default", "bar");

    assertFalse(foo.equals(bar));
    assertTrue(bar.equals(bar2));
  }

  @Test
  public void testHashCode() {
    KijiManagedHBaseTableName foo = KijiManagedHBaseTableName.getKijiTableName("default", "foo");
    KijiManagedHBaseTableName bar = KijiManagedHBaseTableName.getKijiTableName("default", "bar");
    KijiManagedHBaseTableName bar2 = KijiManagedHBaseTableName.getKijiTableName("default", "bar");

    assertFalse(foo.hashCode() == bar.hashCode());
    assertTrue(bar.hashCode() == bar2.hashCode());
  }
}
