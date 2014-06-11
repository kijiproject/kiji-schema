/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.schema.impl.async;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import org.hbase.async.Bytes;
import org.hbase.async.HBaseClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiURI;
import org.kiji.schema.hbase.HBaseFactory;

/**
 * This class tests that AsyncTableLayoutDatabase is correctly writing and reading from HBase
 * when performing its operations.
 */
public class TestAsyncTableKeyValueDatabase extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestAsyncTableKeyValueDatabase.class);

  private static final String TABLE_NAME =  "metaTable";
  private static final Map<String, Map<String, byte[]>> TABLE_KV_MAP = createTableMap();
  private static final String FAMILY_NAME = "meta";

  private static Map<String, Map<String, byte[]>> createTableMap() {
    Map<String, byte[]> innerMap1 = new HashMap<String, byte[]>();
    innerMap1.put("config1", Bytes.UTF8("1one"));
    innerMap1.put("config2", Bytes.UTF8("1two"));
    Map<String, Map<String, byte[]>> result = new HashMap<String, Map<String, byte[]>>();
    result.put("table1", innerMap1);

    Map<String, byte[]> innerMap2 = new HashMap<String, byte[]>();
    innerMap2.put("config1", Bytes.UTF8("2one"));
    innerMap2.put("config2", Bytes.UTF8("2two"));
    result.put("table2", innerMap2);
    return Collections.unmodifiableMap(result);
  }

  private Configuration mConf;
  private HBaseClient mHBClient;
  private HBaseAdmin mHBaseAdmin;
  private AsyncTableKeyValueDatabase mDb;

  @Before
  public final void setupHBaseTable() throws IOException {
    final KijiURI hbaseURI = createTestHBaseURI();
    final String instanceName =
        String.format("%s_%s", getClass().getSimpleName(), mTestName.getMethodName());
    final KijiURI mKijiURI = KijiURI.newBuilder(hbaseURI).withInstanceName(instanceName).build();
    final HBaseFactory factory = HBaseFactory.Provider.get();

    mConf = HBaseConfiguration.create();
    mHBaseAdmin = factory.getHBaseAdminFactory(mKijiURI).create(mConf);
    // Create an HBase table.
    HTableDescriptor tableDescriptor = new HTableDescriptor(TABLE_NAME);
    tableDescriptor.addFamily(new HColumnDescriptor(FAMILY_NAME));
    mHBaseAdmin.createTable(tableDescriptor);

    mHBClient = new HBaseClient(mKijiURI.getZooKeeperEnsemble());

    LOG.debug("TABLE_NAME: {}", TABLE_NAME);
    // Fill it with some data.
    mDb = new AsyncTableKeyValueDatabase(mHBClient, Bytes.UTF8(TABLE_NAME), FAMILY_NAME);
    mDb.putValue("table1", "config1", Bytes.UTF8("1one"));
    mDb.putValue("table1", "config2", Bytes.UTF8("1two"));
    mDb.putValue("table2", "config1", Bytes.UTF8("2one"));
    mDb.putValue("table2", "config2", Bytes.UTF8("2two"));
  }

  @After
  public final void teardownHBaseTable() throws IOException {
    mHBClient.shutdown();
    mHBaseAdmin.disableTable(TABLE_NAME);
    mHBaseAdmin.deleteTable(TABLE_NAME);
    mHBaseAdmin.close();
  }

  @Test
  public void testGet() throws IOException {
    byte[] result = mDb.getValue("table1", "config1");
    assertArrayEquals(Bytes.UTF8("1one"), result);
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
