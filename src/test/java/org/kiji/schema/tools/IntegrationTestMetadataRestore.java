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

package org.kiji.schema.tools;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiAdmin;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;
import org.kiji.schema.testutil.ToolResult;

/**
 * Integration test for metadata backup and restore.
 */
public class IntegrationTestMetadataRestore extends AbstractKijiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestMetadataRestore.class);
  private KijiAdmin mAdmin;
  private Kiji mKiji;
  private KijiMetaTable mMetaTable;
  private static final byte[] BYTES_VALUE = Bytes.toBytes("value");

  @Before
  public void setup() throws IOException {
    KijiConfiguration kijiConf = getKijiConfiguration();
    mKiji = Kiji.open(kijiConf);
    mMetaTable = mKiji.getMetaTable();
    mAdmin = new KijiAdmin(new HBaseAdmin(kijiConf.getConf()), mKiji);
  }

  @After
  public void tearDown() throws IOException {
    mMetaTable.close();
    mKiji.close();
  }

  @Test
  public void testBackupAndRestore() throws Exception {
    KijiConfiguration kijiConf = getKijiConfiguration();

    // Add a table.
    final File layoutFile =
        KijiTableLayouts.getTempFile(KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST));
    ToolResult createFooTableResult = runTool(new CreateTableTool(), new String[] {
      "--table=foo",
      "--layout=" + layoutFile,
    });
    assertEquals(0, createFooTableResult.getReturnCode());

    // The metatable should list "foo" as one of its tables.
    assertTrue(mMetaTable.listTables().contains("foo"));
    // Add an arbitrary key-value pair to the metadata for the "foo" table.
    mMetaTable.putValue("foo", "key", BYTES_VALUE);
    assertArrayEquals(BYTES_VALUE, mMetaTable.getValue("foo", "key"));

    // Now, backup the system metadata.
    File tmpFile = File.createTempFile("metadata", "avro");
    tmpFile.delete(); // Can't actually exist yet.
    tmpFile.deleteOnExit(); // Schedule a delete of the backup data for when the test is over.
    ToolResult backupResult = runTool(new MetadataTool(), new String[] {
      "--backup=" + tmpFile.getAbsolutePath(),
    });
    assertEquals(backupResult.getStdoutUtf8(), 0, backupResult.getReturnCode());

    // Now, delete the foo table from the metatable, but leave the physical table
    // behind.
    mMetaTable.deleteTable("foo");
    mKiji.close();

    // Verify that there's no entry left behind in the meta table.
    mKiji = Kiji.open(kijiConf);
    mMetaTable = mKiji.getMetaTable();
    try {
      KijiTableLayout fooLayout = mMetaTable.getTableLayout("foo");
      fail("Table layout for foo should not exist.");
    } catch (KijiTableNotFoundException e) {
      // Good, that's what we expected.
    }

    // Now restore the metadata
    ToolResult restoreResult = runTool(new MetadataTool(), new String[] {
      "--restore=" + tmpFile.getAbsolutePath(),
      "--confirm=true",
    });
    assertEquals(restoreResult.getStdoutUtf8(), 0, restoreResult.getReturnCode());

    // The metatable should list "foo" as one of its tables, again.
    for (String tableName : mMetaTable.tableSet()) {
      LOG.info("A table named " + tableName + "has key-val info attached to it.");
    }
    assertTrue(mMetaTable.tableSet().contains("foo"));

    assertArrayEquals(BYTES_VALUE, mMetaTable.getValue("foo", "key"));
    assertEquals(mMetaTable.getTableLayout("foo").getName(),
        KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST).getName());
    assertEquals(mMetaTable.getTableLayout("foo").getDesc().getDescription(),
        KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST).getDescription());
    assertEquals(mMetaTable.getTableLayout("foo").getDesc().getVersion(),
        KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST).getVersion());
  }
}
