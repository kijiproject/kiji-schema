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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.avro.MetadataBackup;
import org.kiji.schema.avro.SchemaTableEntry;
import org.kiji.schema.avro.TableBackup;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.impl.MetadataRestorer;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;

/**
 *
 */
public class IntegrationTestKijiMetaTable extends AbstractKijiIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestKijiMetaTable.class);

  private Kiji mKiji;
  private KijiMetaTable mMetaTable;
  private static final byte[] BYTES_VALUE = Bytes.toBytes("value");

  @Before
  public void setup() throws IOException {
    mKiji = new Kiji(getKijiConfiguration());
    mMetaTable = mKiji.getMetaTable();
    TableLayoutDesc layout = KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST);
    mMetaTable.updateTableLayout("foo", layout);
    mMetaTable.putValue("foo", "key", BYTES_VALUE);
  }

  @Test
  public void testBackupAndRestore() throws InterruptedException, IOException {
    assertEquals(1, mMetaTable.listTables().size());
    assertEquals(1, mMetaTable.tableSet().size());
    assertEquals(1, mMetaTable.keySet("foo").size());
    assertArrayEquals(BYTES_VALUE, mMetaTable.getValue("foo", "key"));
    // write to backupBuilder
    final MetadataBackup.Builder backupBuilder = MetadataBackup.newBuilder()
        .setLayoutVersion(mKiji.getSystemTable().getDataVersion())
        .setMetaTable(new HashMap<String, TableBackup>())
        .setSchemaTable(new ArrayList<SchemaTableEntry>());
    Map<String, TableBackup> metadata = mMetaTable.toBackup();
    backupBuilder.setMetaTable(metadata);
    backupBuilder.setSchemaTable(mKiji.getSchemaTable().toBackup());
    final MetadataBackup backup = backupBuilder.build();

    // make sure metadata key-value pairs are what we expect.
    assertEquals(1, backup.getMetaTable().get("foo").getKeyValues().size());
    assertEquals("key", backup.getMetaTable().get("foo").getKeyValues().get(0).getKey());
    assertArrayEquals(BYTES_VALUE, backup.getMetaTable().get("foo").getKeyValues().get(0)
        .getValue().array());
    mMetaTable.deleteTable("foo");
    assertTrue(!mMetaTable.tableSet().contains("foo"));
    assertEquals(0, mMetaTable.listTables().size());
    assertEquals(0, mMetaTable.tableSet().size());

    MetadataRestorer restorer = new MetadataRestorer();
    restorer.restoreTables(backup, mKiji);
    mMetaTable = mKiji.getMetaTable();
    assertEquals("The number of tables with layouts is incorrect.", 1,
        mMetaTable.listTables().size());
    assertEquals("The number of tables with kv pairs is incorrect.", 1,
        mMetaTable.tableSet().size());
    assertEquals("The number of keys for the foo table is incorrect.", 1,
        mMetaTable.keySet("foo").size());
    assertArrayEquals(BYTES_VALUE, mMetaTable.getValue("foo", "key"));
  }

  @After
  public void teardown() throws IOException {
    mMetaTable.close();
    mKiji.close();
  }

}
