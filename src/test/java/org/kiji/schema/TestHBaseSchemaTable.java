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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.KijiSchemaTable.SchemaEntry;
import org.kiji.schema.impl.HBaseSchemaTable;
import org.kiji.schema.util.BytesKey;
import org.kiji.schema.util.ZooKeeperLock;


public class TestHBaseSchemaTable {

  private static SchemaEntry makeSchemaEntry(long id, Schema schema) {
    return new SchemaEntry(id, new BytesKey(KijiSchemaTable.hashSchema(schema)), schema);
  }

  private static final SchemaEntry INT_SCHEMA_ENTRY =
      makeSchemaEntry(0, Schema.create(Schema.Type.INT));

  private HTable mHashHTable;
  private HTable mIdHTable;
  private ZooKeeperLock mZKLock;


  @Before
  public void setup() {
    mHashHTable = createMock(HTable.class);
    mIdHTable = createMock(HTable.class);
    mZKLock = createMock(ZooKeeperLock.class);
  }

  @Test
  public void testGetSchemaByIdReturnsNull() throws IOException {
    final Result result = createMock(Result.class);
    expect(mIdHTable.get(anyObject(Get.class))).andReturn(result);
    expect(result.isEmpty()).andReturn(true);

    mIdHTable.flushCommits();
    mIdHTable.close();

    mHashHTable.flushCommits();
    mHashHTable.close();

    mZKLock.close();

    replay(result);
    replay(mIdHTable);
    replay(mHashHTable);
    replay(mZKLock);

    final HBaseSchemaTable schemaTable = new HBaseSchemaTable(mHashHTable, mIdHTable, mZKLock);
    assertNull(schemaTable.getSchema(0));
    schemaTable.close();

    verify(result);
    verify(mIdHTable);
    verify(mHashHTable);
    verify(mZKLock);
  }

  @Test
  public void testGetSchemaByHashReturnsNull() throws IOException {
    final Result result = createMock(Result.class);
    expect(mHashHTable.get(anyObject(Get.class))).andReturn(result);
    expect(result.isEmpty()).andReturn(true);

    mIdHTable.flushCommits();
    mIdHTable.close();

    mHashHTable.flushCommits();
    mHashHTable.close();

    mZKLock.close();

    replay(result);
    replay(mIdHTable);
    replay(mHashHTable);
    replay(mZKLock);

    final HBaseSchemaTable schemaTable = new HBaseSchemaTable(mHashHTable, mIdHTable, mZKLock);
    assertNull(schemaTable.getSchema(new BytesKey(new byte[] {0})));
    schemaTable.close();

    verify(result);
    verify(mIdHTable);
    verify(mHashHTable);
    verify(mZKLock);
  }

  @Test
  public void testGetSchemaByIdReturnsSomething() throws IOException {
    final Result result = createMock(Result.class);
    expect(mIdHTable.get(anyObject(Get.class))).andReturn(result);
    expect(result.isEmpty()).andReturn(false);
    expect(result.value()).andReturn(
        HBaseSchemaTable.encodeSchemaEntry(HBaseSchemaTable.toAvroEntry(INT_SCHEMA_ENTRY)));

    mIdHTable.flushCommits();
    mIdHTable.close();

    mHashHTable.flushCommits();
    mHashHTable.close();

    mZKLock.close();

    replay(result);
    replay(mIdHTable);
    replay(mHashHTable);
    replay(mZKLock);

    final HBaseSchemaTable schemaTable = new HBaseSchemaTable(mHashHTable, mIdHTable, mZKLock);
    assertEquals(Schema.Type.INT, schemaTable.getSchema(0).getType());
    schemaTable.close();

    verify(result);
    verify(mIdHTable);
    verify(mHashHTable);
    verify(mZKLock);
  }

  @Test
  public void testGetSchemaByHashReturnsSomething() throws IOException {
    final Result result = createMock(Result.class);
    expect(mHashHTable.get(anyObject(Get.class))).andReturn(result);
    expect(result.isEmpty()).andReturn(false);
    expect(result.value()).andReturn(
        HBaseSchemaTable.encodeSchemaEntry(HBaseSchemaTable.toAvroEntry(INT_SCHEMA_ENTRY)));

    mIdHTable.flushCommits();
    mIdHTable.close();

    mHashHTable.flushCommits();
    mHashHTable.close();

    mZKLock.close();

    replay(result);
    replay(mIdHTable);
    replay(mHashHTable);
    replay(mZKLock);

    final HBaseSchemaTable schemaTable = new HBaseSchemaTable(mHashHTable, mIdHTable, mZKLock);
    assertEquals(Schema.Type.INT, schemaTable.getSchema(INT_SCHEMA_ENTRY.getHash()).getType());
    schemaTable.close();

    verify(result);
    verify(mIdHTable);
    verify(mHashHTable);
    verify(mZKLock);
  }

  @Test
  public void testGetOrCreateSchemaId() throws IOException {
    final Result result = createMock(Result.class);
    expect(mHashHTable.get(anyObject(Get.class))).andReturn(result);
    expect(result.isEmpty()).andReturn(true);

    mZKLock.lock();

    expect(mHashHTable.get(anyObject(Get.class))).andReturn(result);
    expect(result.isEmpty()).andReturn(true);

    expect(mIdHTable.incrementColumnValue(
        aryEq(Bytes.toBytes(HBaseSchemaTable.SCHEMA_COUNTER_ROW_NAME)),
        aryEq(Bytes.toBytes(HBaseSchemaTable.SCHEMA_COLUMN_FAMILY)),
        aryEq(Bytes.toBytes(HBaseSchemaTable.SCHEMA_COLUMN_QUALIFIER)),
        eq(1L)))
        .andReturn(10L);

    mIdHTable.put(anyObject(Put.class));
    mHashHTable.put(anyObject(Put.class));

    mIdHTable.flushCommits();
    mHashHTable.flushCommits();

    mZKLock.unlock();

    mIdHTable.flushCommits();
    mIdHTable.close();

    mHashHTable.flushCommits();
    mHashHTable.close();

    mZKLock.close();

    replay(result);
    replay(mIdHTable);
    replay(mHashHTable);
    replay(mZKLock);

    final HBaseSchemaTable schemaTable = new HBaseSchemaTable(mHashHTable, mIdHTable, mZKLock);
    assertEquals(9, schemaTable.getOrCreateSchemaId(INT_SCHEMA_ENTRY.getSchema()));
    schemaTable.close();

    verify(result);
    verify(mIdHTable);
    verify(mHashHTable);
    verify(mZKLock);
  }

}
