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
//
//package org.kiji.schema.impl.hbase;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//import static org.junit.Assert.fail;
//
//import java.io.IOException;
//
//import org.apache.hadoop.hbase.client.Get;
//import org.apache.hadoop.hbase.client.HConnectionManager;
//import org.apache.hadoop.hbase.client.HTable;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import org.kiji.schema.Kiji;
//import org.kiji.schema.avro.TableLayoutDesc;
//import org.kiji.schema.impl.HTableInterfaceFactory;
//import org.kiji.schema.layout.KijiTableLayouts;
//import org.kiji.schema.testutil.AbstractKijiIntegrationTest;
//
//public class IntegrationTestKijiHTablePool extends AbstractKijiIntegrationTest {
//  private static final Logger LOG = LoggerFactory.getLogger(KijiHTablePool.class);
//
//  private Kiji mKiji;
//  private String mTableName;
//
//  @Before
//  public void setupHTablePoolTest() throws Exception {
//    mKiji = Kiji.Factory.get().open(getKijiURI());
//    final TableLayoutDesc fullFeaturedLayout =
//        KijiTableLayouts.getLayout(KijiTableLayouts.FULL_FEATURED);
//    mKiji.createTable(fullFeaturedLayout);
//    mTableName = fullFeaturedLayout.getName();
//  }
//
//  @After
//  public void teardownHTablePoolTest() throws Exception {
//    mKiji.release();
//  }
//
//  /**
//   * This test gets a table from the pool, closes it, and then confirms that a second table drawn
//   * from the pool uses the same underlying HTable.
//   *
//   * @throws IOException
//   */
//  @Test
//  public void testBasicPoolReuse() throws IOException {
//    final HTableInterfaceFactory hTableFactory = DefaultHTableInterfaceFactory.get();
//    final KijiHTablePool tablePool =
//        new KijiHTablePool(mTableName, (HBaseKiji)mKiji, hTableFactory);
//    KijiHTablePool.PooledHTable pooledHTable = (KijiHTablePool.PooledHTable)tablePool.getTable();
//    final HTable table1 = (HTable)pooledHTable.getWrappedTable();
//    pooledHTable.close();
//    pooledHTable = (KijiHTablePool.PooledHTable)tablePool.getTable();
//    final HTable table2 = (HTable)pooledHTable.getWrappedTable();
//    assertEquals(table1, table2);
//
//    tablePool.close();
//  }
//
//  /**
//   * A test that manually invalidates tables by closing their connection and ensures that after
//   * doing so we no longer receive a different table.
//   *
//   * Note that because this test directly closes the underlying HConnection, it isn't safe to run
//   * except in isolation. As such it is disabled by default.
//   */
//  //@Test
//  public void testTableInvalidation() throws Exception {
//    final HTableInterfaceFactory hTableFactory = DefaultHTableInterfaceFactory.get();
//    final KijiHTablePool tablePool =
//        new KijiHTablePool(mTableName, (HBaseKiji)mKiji, hTableFactory);
//
//    // Get a table and close its underlying connection, then return it to the pool.
//    KijiHTablePool.PooledHTable pooledHTable = (KijiHTablePool.PooledHTable)tablePool.getTable();
//    final HTable table1 = (HTable)pooledHTable.getWrappedTable();
//    // This approach is highly dangerous, but alright for a test:
//    table1.getConnection().close();
//    HConnectionManager.deleteStaleConnection(table1.getConnection());
//    // Unfortunately this is somewhat time-sensitive. We need to wait while things quiesce.
//    Thread.sleep(2000);
//    assertTrue(table1.getConnection().isClosed());
//    assertTrue(!pooledHTable.isValid());
//    try {
//      table1.get(new Get());
//      fail("Should not be able to successfuly get off a closed connection.");
//    } catch (IOException ioe) {
//      // The precise exception we catch here is time-dependent, so we don't try to match against
//      // a particular string.
//      LOG.debug(ioe.toString());
//    }
//    pooledHTable.close();
//
//    // Get a second table and make sure it's not the same as the first and is valid.
//    pooledHTable = (KijiHTablePool.PooledHTable)tablePool.getTable();
//    final HTable table2 = (HTable)pooledHTable.getWrappedTable();
//    assert(!table1.equals(table2));
//    assert(pooledHTable.isValid());
//    table2.get(new Get());
//
//    tablePool.close();
//  }
//}
