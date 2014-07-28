/**
 * (c) Copyright 2013 WibiData, Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HConstants;
import org.junit.Test;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestMetadataTool extends KijiToolTest {

  private static final String LAYOUT_V1 = "org/kiji/schema/layout/layout-updater-v1.json";
  private static final String LAYOUT_V2 = "org/kiji/schema/layout/layout-updater-v2.json";

  @Test
  public void testInTempFolder() throws Exception {
    final File backupFile = new File(getLocalTempDir(), "tempMetadataBackup");

    final Kiji kijiBackup = createTestKiji();
    final Kiji kijiRestored = createTestKiji();
    assertFalse(kijiBackup.getURI().equals(kijiRestored.getURI()));

    // Make this a non-trivial instance by creating a couple of tables.
    kijiBackup.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST));
    kijiBackup.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.FULL_FEATURED));

    // Backup metadata for kijiBackup.
    List<String> args = Lists.newArrayList("--kiji=" + kijiBackup.getURI().toOrderedString(),
        "--backup=" + backupFile.getPath());
    MetadataTool tool = new MetadataTool();
    tool.setConf(getConf());
    assertEquals(BaseTool.SUCCESS, tool.toolMain(args));

    // Check that kijiBackup and kijiRestored do not intersect.
    assertFalse(kijiRestored.getTableNames().containsAll(kijiBackup.getTableNames()));

    // Restore metadata from kijiBackup to kijiRestored.
    args = Lists.newArrayList("--kiji=" + kijiRestored.getURI().toOrderedString(),
        "--restore=" + backupFile.getPath(),
        "--interactive=false");
    tool = new MetadataTool();
    tool.setConf(getConf());
    assertEquals(BaseTool.SUCCESS, tool.toolMain(args));
    assertTrue(kijiRestored.getTableNames().containsAll(kijiBackup.getTableNames()));
  }

  @Test
  public void testBackupUpdatedLayout() throws Exception {
    final File backupFile = new File(getLocalTempDir(), "tempMetadataBackup");

    final Kiji kijiBackup = createTestKiji();
    final Kiji kijiRestored = createTestKiji();
    assertFalse(kijiBackup.getURI().equals(kijiRestored.getURI()));

    // Create a table and update it's layout
    kijiBackup.createTable(KijiTableLayouts.getLayout(LAYOUT_V1));
    kijiBackup.modifyTableLayout(KijiTableLayouts.getLayout(LAYOUT_V2));
    KijiMetaTable backupMetaTable = kijiBackup.getMetaTable();
    assertEquals(2,
        backupMetaTable.getTableLayoutVersions("table_name", HConstants.ALL_VERSIONS).size());

    // Check that kijiBackup and kijiRestored do not intersect.
    assertFalse(kijiRestored.getTableNames().containsAll(kijiBackup.getTableNames()));

    // Backup metadata for kijiBackup.
    List<String> args = Lists.newArrayList("--kiji=" + kijiBackup.getURI().toOrderedString(),
        "--backup=" + backupFile.getPath());
    MetadataTool tool = new MetadataTool();
    tool.setConf(getConf());
    assertEquals(BaseTool.SUCCESS, tool.toolMain(args));

    // Restore metadata from kijiBackup to kijiRestored.
    args = Lists.newArrayList("--kiji=" + kijiRestored.getURI().toOrderedString(),
        "--restore=" + backupFile.getPath(),
        "--interactive=false");
    tool = new MetadataTool();
    tool.setConf(getConf());
    assertEquals(BaseTool.SUCCESS, tool.toolMain(args));

    // Validate that all tables are present with the correct number of versions
    assertTrue(kijiRestored.getTableNames().containsAll(kijiBackup.getTableNames()));
    KijiMetaTable restoreMetaTable = kijiRestored.getMetaTable();
    assertTrue(
        restoreMetaTable.getTableLayoutVersions("table_name", HConstants.ALL_VERSIONS).size() > 1);
  }
}
