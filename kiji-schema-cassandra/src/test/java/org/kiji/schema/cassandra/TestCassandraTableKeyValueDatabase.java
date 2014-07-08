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

package org.kiji.schema.cassandra;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTableKeyValueDatabase;
import org.kiji.schema.KijiURI;

/**
 * This class tests that HBaseTableLayoutDatabase is correctly writing and reading from HBase
 * when performing its operations.
 */
public class TestCassandraTableKeyValueDatabase extends CassandraKijiClientTest {
  private static final Map<String, Map<String, byte[]>> TABLE_KV_MAP = createTableMap();

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

  //private CassandraAdmin mAdmin;
  private KijiTableKeyValueDatabase mDb;

  @Before
  public final void setupTable() throws IOException {
    // This will install a new Kiji instance.
    final Kiji kiji = getKiji();

    // Get an admin and create a table for this instance.
    KijiURI kijiURI = kiji.getURI();
    //mAdmin = CassandraFactory
        //.Provider
        //.get()
        //.getCassandraAdminFactory(kijiURI)
        //.create(kijiURI);

    // Fill it with some data.
    mDb = kiji.getMetaTable();
    mDb.putValue("table1", "config1", Bytes.toBytes("1one"));
    mDb.putValue("table1", "config2", Bytes.toBytes("1two"));
    mDb.putValue("table2", "config1", Bytes.toBytes("2one"));
    mDb.putValue("table2", "config2", Bytes.toBytes("2two"));
  }

  //@After
  //public final void tearDownHBaseTable() throws IOException {
    //mAdmin.close();
  //}

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

  @Test
  public void testRemoveAllValues() throws IOException {
    mDb.removeAllValues("table1");
    try {
      mDb.getValue("table1", "config1");
      fail("An exception should have been thrown.");
    } catch (IOException ioe) {
      assertEquals("Could not find any values associated with table table1 and key config1",
          ioe.getMessage());
    }
  }
}
