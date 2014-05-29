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

package org.kiji.schema.impl.hbase;

import java.io.IOException;

import com.google.common.collect.ImmutableList;
import junit.framework.Assert;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.hbase.HBaseFactory;
import org.kiji.schema.layout.KijiTableLayouts;


public class TestHBaseMetaTable extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseMetaTable.class);

  private KijiURI mKijiURI;
  private HBaseMetaTable mMetaTable;
  private HBaseAdmin mAdmin;

  @Before
  public final void setupTest() throws IOException {
    final KijiSchemaTable schemaTable = getKiji().getSchemaTable();

    final KijiURI hbaseURI = createTestHBaseURI();
    final String instanceName =
        String.format("%s_%s", getClass().getSimpleName(), mTestName.getMethodName());
    mKijiURI = KijiURI.newBuilder(hbaseURI).withInstanceName(instanceName).build();
    final HBaseFactory factory = HBaseFactory.Provider.get();
    mAdmin = factory.getHBaseAdminFactory(mKijiURI).create(getConf());

    HBaseMetaTable.install(mAdmin, mKijiURI);

    mMetaTable = new HBaseMetaTable(
        mKijiURI, getConf(), schemaTable, factory.getHTableInterfaceFactory(mKijiURI));
  }

  @After
  public final void teardownTest() throws IOException {
    mMetaTable.close();
    mMetaTable = null;
    HBaseMetaTable.uninstall(mAdmin, mKijiURI);
  }

  @Test
  public void testLayouts() throws Exception {
    final String tableName = "table";
    Assert.assertTrue(mMetaTable.listTables().isEmpty());
    mMetaTable.updateTableLayout(tableName, KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE));
    Assert.assertEquals(ImmutableList.of(tableName), mMetaTable.listTables());
    mMetaTable.deleteTable(tableName);
    Assert.assertTrue(mMetaTable.listTables().isEmpty());
  }
}
