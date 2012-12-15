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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.impl.HBaseTableKeyValueDatabase;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;

/**
 * This class tests that HBaseTableLayoutDatabase is correctly writing and reading from HBase
 * when performing its operations.
 */
public class IntegrationTestHBaseTableKeyValueDatabase extends AbstractKijiIntegrationTest {
  private static final String TABLE_NAME =  "metaTable";
  private static final Map<String, Map<String, byte[]>> TABLE_KV_MAP = createTableMap();
  private static final String FAMILY_NAME = "meta";
  private HTable mTable;
  private HBaseTableKeyValueDatabase mDb;

  private static Map<String, Map<String, byte[]>> createTableMap() {
    Map<String, byte[]> innerMap1 = new HashMap<String, byte[]>();
    innerMap1.put("config1", Bytes.toBytes("1one"));
    innerMap1.put("config2", Bytes.toBytes("1two"));
    Map<String, Map<String, byte[]>> result = new HashMap<String, Map<String, byte[]>>();
    result.put("table1", innerMap1);

    Map<String, byte[]> innerMap2 = new HashMap<String, byte[]>();
    innerMap2.put("config1", Bytes.toBytes("2one"));
    innerMap2.put("config2", Bytes.toBytes("2two"));
    result.put("table2", innerMap2);
    return Collections.unmodifiableMap(result);
  }

  @Before
  public void setupHBaseTable() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      // Create an HBase table.
      HTableDescriptor tableDescriptor = new HTableDescriptor(TABLE_NAME);
      tableDescriptor.addFamily(new HColumnDescriptor(FAMILY_NAME));
      admin.createTable(tableDescriptor);
    } finally {
      admin.close();
    }
    mTable = new HTable(conf, TABLE_NAME);
    // Fill it with some data.
    Put put;
    mDb = new HBaseTableKeyValueDatabase(mTable, FAMILY_NAME);
    mDb.putValue("table1", "config1", Bytes.toBytes("1one"));
    mDb.putValue("table1", "config2", Bytes.toBytes("1two"));
    mDb.putValue("table2", "config1", Bytes.toBytes("2one"));
    mDb.putValue("table2", "config2", Bytes.toBytes("2two"));
  }

  @After
  public void teardownHBaseTable() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      admin.disableTable(TABLE_NAME);
      admin.deleteTable(TABLE_NAME);
    } finally {
      admin.close();
      mTable.close();
      mTable = null;
      mDb = null;
    }
  }

  @Test
  public void testGet() throws IOException {
    byte[] result = mDb.getValue("table1", "config1");
    assertArrayEquals(Bytes.toBytes("1one"), result);
  }

  @Test
  public void testTableSet() throws IOException {
    Set<String> tableNames = mDb.tableSet();
    assertEquals(TABLE_KV_MAP.keySet(), tableNames);
  }

  @Test
  public void testKeySet() throws IOException {
    Set<String> keys = mDb.keySet("table1");
    assertEquals(TABLE_KV_MAP.get("table1").keySet(), keys);
  }

  @Test
  public void testRemoveValues() throws IOException {
    mDb.removeValues("table1", "config1");
    assertEquals(1, mDb.keySet("table1").size());
    assertTrue("The key set should still contain config2", mDb.keySet("table1")
        .contains("config2"));
  }

  @Test(expected=IOException.class)
  public void testRemoveAllValues() throws IOException {
    mDb.removeAllValues("table1");
    mDb.getValue("table1", "config1");
  }
}
