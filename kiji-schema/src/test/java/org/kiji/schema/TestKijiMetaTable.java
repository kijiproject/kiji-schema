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
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import org.kiji.schema.avro.KeyValueBackupEntry;
import org.kiji.schema.avro.MetaTableBackup;
import org.kiji.schema.avro.MetadataBackup;
import org.kiji.schema.avro.SchemaTableBackup;
import org.kiji.schema.avro.SchemaTableEntry;
import org.kiji.schema.avro.SystemTableBackup;
import org.kiji.schema.avro.SystemTableEntry;
import org.kiji.schema.avro.TableBackup;
import org.kiji.schema.avro.TableLayoutBackupEntry;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.impl.MetadataRestorer;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;

/** Tests backuping and restoring Kiji meta tables. */
public class TestKijiMetaTable extends KijiClientTest {

  private static final byte[] BYTES_VALUE = Bytes.toBytes("value");

  @Test
  public void testBackupAndRestore() throws InterruptedException, IOException {
    final Kiji kiji = getKiji();
    final KijiMetaTable metaTable = kiji.getMetaTable();
    final KijiSchemaTable schemaTable = kiji.getSchemaTable();
    final KijiSystemTable systemTable = kiji.getSystemTable();

    final TableLayoutDesc layout = KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST);
    final KijiTableLayout updatedLayout = metaTable.updateTableLayout("foo", layout);
    metaTable.putValue("foo", "key", BYTES_VALUE);

    systemTable.putValue("testKey", Bytes.toBytes("testValue"));
    assertEquals(1, metaTable.listTables().size());
    assertEquals(1, metaTable.tableSet().size());
    assertEquals(1, metaTable.keySet("foo").size());
    assertArrayEquals(BYTES_VALUE, metaTable.getValue("foo", "key"));
    // write to backupBuilder
    final MetadataBackup.Builder backupBuilder = MetadataBackup.newBuilder()
        .setLayoutVersion(kiji.getSystemTable().getDataVersion().toString())
        .setMetaTable(
            MetaTableBackup.newBuilder()
                .setTables(new HashMap<String, TableBackup>())
                .build())
        .setSchemaTable(
            SchemaTableBackup.newBuilder()
                .setEntries(new ArrayList<SchemaTableEntry>())
                .build())
        .setSystemTable(
            SystemTableBackup.newBuilder()
                .setEntries(new ArrayList<SystemTableEntry>())
                .build());
    backupBuilder.setMetaTable(metaTable.toBackup());
    backupBuilder.setSchemaTable(schemaTable.toBackup());
    backupBuilder.setSystemTable(systemTable.toBackup());
    final MetadataBackup backup = backupBuilder.build();

    // make sure metadata key-value pairs are what we expect.
    List<KeyValueBackupEntry> keyValues =
        backup.getMetaTable().getTables().get("foo").getKeyValueBackup().getKeyValues();
    assertEquals(1, keyValues.size());
    assertEquals("key", keyValues.get(0).getKey());
    assertArrayEquals(BYTES_VALUE, keyValues.get(0).getValue().array());

    // make sure layouts are what we expect.
    List<TableLayoutBackupEntry> layoutBackups =
        backup.getMetaTable().getTables().get("foo").getTableLayoutsBackup().getLayouts();
    assertEquals(1, layoutBackups.size());
    assertEquals(updatedLayout.getDesc(), layoutBackups.get(0).getLayout());

    metaTable.deleteTable("foo");
    assertTrue(!metaTable.tableSet().contains("foo"));
    assertEquals(0, metaTable.listTables().size());
    assertEquals(0, metaTable.tableSet().size());

    final MetadataRestorer restorer = new MetadataRestorer();
    restorer.restoreTables(backup, kiji);

    final KijiMetaTable newMetaTable = kiji.getMetaTable();
    assertEquals("The number of tables with layouts is incorrect.", 1,
        newMetaTable.listTables().size());
    assertEquals("The number of tables with kv pairs is incorrect.", 1,
        newMetaTable.tableSet().size());
    assertEquals("The number of keys for the foo table is incorrect.", 1,
        newMetaTable.keySet("foo").size());
    assertArrayEquals(BYTES_VALUE, newMetaTable.getValue("foo", "key"));

    systemTable.putValue("testKey", Bytes.toBytes("changedValue"));
    restorer.restoreSystemVars(backup, kiji);
    assertEquals("testValue", Bytes.toString(systemTable.getValue("testKey")));
  }

  @Test
  public void testSameMetaTableOnPut() throws InterruptedException, IOException {
    final Kiji kiji = getKiji();
    final KijiMetaTable metaTable = kiji.getMetaTable();

    final KijiTableKeyValueDatabase<?> outDb = metaTable.putValue("foo", "key", BYTES_VALUE);
    assertEquals("putValue() exposes the delegate", metaTable, outDb);
  }

  @Test
  public void testChainedMetaTable() throws InterruptedException, IOException {
    // Do an operation on the metatable, then set a key with putValue().
    // Use the KijiMetaTable obj returned by this to modify the underlying db.
    // Verify that the original KijiMetaTable sees the change.
    final Kiji kiji = getKiji();
    final KijiMetaTable metaTable = kiji.getMetaTable();

    final TableLayoutDesc layout = KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST);
    final KijiTableLayout updatedLayout = metaTable.updateTableLayout("foo", layout);

    final KijiMetaTable outMeta = metaTable.putValue("foo", "key", BYTES_VALUE);
    assertEquals("putValue() exposes the delegate", metaTable, outMeta);

    outMeta.deleteTable("foo");

    assertTrue(!outMeta.tableSet().contains("foo"));
    assertEquals(0, outMeta.listTables().size());
    assertEquals(0, outMeta.tableSet().size());

    assertTrue(!metaTable.tableSet().contains("foo"));
    assertEquals(0, metaTable.listTables().size());
    assertEquals(0, metaTable.tableSet().size());
  }

}
