// (c) Copyright 2011 WibiData, Inc.

package com.wibidata.core.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.layout.KijiTableLayouts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wibidata.core.WibiIntegrationTest;
import com.wibidata.core.testutil.ToolResult;
import com.wibidata.core.tools.WibiCreateTable;

/**
 * Base class for integration tests that exercise KijiTableWriter delete functionality.
 */
public abstract class BaseWibiTableWriterDeletesIntegrationTest extends WibiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(
      BaseWibiTableWriterDeletesIntegrationTest.class);

  public static final String TABLE_NAME = "test";

  private final KijiDataRequest mDataRequest = new KijiDataRequest()
      .addColumn(new KijiDataRequest.Column("group").withMaxVersions(Integer.MAX_VALUE))
      .addColumn(new KijiDataRequest.Column("map").withMaxVersions(Integer.MAX_VALUE))
      .addColumn(new KijiDataRequest.Column("memoryMap").withMaxVersions(Integer.MAX_VALUE));

  private Kiji mWibi = null;
  private KijiTable mTable = null;
  private KijiTableWriter mWriter = null;
  private KijiTableReader mReader = null;

  /**
   * Creates the KijiTableWriter to test.
   *
   * @param table The table to write to.
   * @return A new KijiTableWriter to test.
   * @throws IOException If there is an error creating the writer.
   */
  protected abstract KijiTableWriter createWriter(KijiTable table) throws IOException;

  /**
   * Creates a test table for deleting data.
   */
  @Before
  public void setup() throws Exception {
    mWibi = Kiji.open(getKijiConfiguration());

    LOG.info("Creating test table.");
    final File layoutFile =
        KijiTableLayouts.getTempFile(KijiTableLayouts.getLayout(KijiTableLayouts.DELETES_TEST));
    final ToolResult createResult = runTool(new WibiCreateTable(), new String[] {
      "--table=" + TABLE_NAME,
      "--layout=" + layoutFile,
    });
    assertEquals(0, createResult.getReturnCode());

    LOG.info("Populating test table.");
    mTable = mWibi.openTable(TABLE_NAME);
    mWriter = createWriter(mTable);

    mWriter.put(mTable.getEntityId("alpha"), "group", "a", 1L, createCell("1"));
    mWriter.put(mTable.getEntityId("alpha"), "group", "a", 2L, createCell("2"));
    mWriter.put(mTable.getEntityId("alpha"), "group", "a", 3L, createCell("3"));
    mWriter.put(mTable.getEntityId("alpha"), "group", "b", 3L, createCell("3"));
    mWriter.put(mTable.getEntityId("alpha"), "map", "key1", 1L, createCell("1"));
    mWriter.put(mTable.getEntityId("alpha"), "map", "key2", 3L, createCell("3"));
    mWriter.put(mTable.getEntityId("alpha"), "memoryMap", "key1", 2L, createCell("2"));
    mWriter.put(mTable.getEntityId("alpha"), "memoryMap", "key2", 4L, createCell("4"));

    mWriter.flush();

    mReader = mTable.openTableReader();
    KijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertTrue(result.containsColumn("group"));
    assertTrue(result.containsColumn("map"));
    assertTrue(result.containsColumn("memoryMap"));
    LOG.info("Test table populated");
  }

  /**
   * Deletes the test table.
   */
  @After
  public void teardown() throws IOException {
    IOUtils.closeQuietly(mReader);
    IOUtils.closeQuietly(mWriter);
    IOUtils.closeQuietly(mTable);
    IOUtils.closeQuietly(mWibi);
  }

  @Test
  public void testDeleteRow() throws IOException {
    mWriter.deleteRow(mTable.getEntityId("alpha"));

    KijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertFalse(result.containsColumn("group"));
    assertFalse(result.containsColumn("map"));
    assertFalse(result.containsColumn("memoryMap"));
  }

  @Test
  public void testDeleteRowUpToTimestamp() throws IOException {
    mWriter.deleteRow(mTable.getEntityId("alpha"), 3L);

    KijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertFalse(result.containsColumn("group"));
    assertFalse(result.containsColumn("map"));
    assertTrue(result.containsColumn("memoryMap"));
    assertFalse(result.getQualifiers("memoryMap").contains("key1"));
    assertTrue(result.getQualifiers("memoryMap").contains("key2"));
  }

  @Test
  public void testDeleteGroupFamily() throws IOException {
    mWriter.deleteFamily(mTable.getEntityId("alpha"), "group");

    KijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertFalse(result.containsColumn("group"));
    assertTrue(result.containsColumn("map"));
    assertTrue(result.containsColumn("memoryMap"));
  }

  @Test
  public void testDeleteGroupFamilyUpToTimestamp() throws IOException {
    mWriter.deleteFamily(mTable.getEntityId("alpha"), "group", 2L);

    KijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertTrue(result.containsColumn("group"));
    assertEquals(1, result.getTimestamps("group", "a").size());
    assertTrue(result.getTimestamps("group", "a").contains(3L));
    assertTrue(result.getQualifiers("group").contains("b"));
    assertTrue(result.containsColumn("map"));
    assertTrue(result.containsColumn("memoryMap"));
  }

  @Test
  public void testDeleteMapFamily() throws IOException {
    mWriter.deleteFamily(mTable.getEntityId("alpha"), "map");

    KijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertTrue(result.containsColumn("group"));
    assertFalse(result.containsColumn("map"));
    assertTrue(result.containsColumn("memoryMap"));
  }

  @Test
  public void testDeleteMapFamilyUpToTimestamp() throws IOException {
    mWriter.deleteFamily(mTable.getEntityId("alpha"), "map", 2L);

    KijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertTrue(result.containsColumn("group"));
    assertTrue(result.containsColumn("map"));
    assertEquals(1, result.getQualifiers("map").size());
    assertTrue(result.containsColumn("map", "key2"));
    assertTrue(result.containsColumn("memoryMap"));
  }

  @Test
  public void testDeleteColumn() throws IOException {
    mWriter.deleteColumn(mTable.getEntityId("alpha"), "group", "a");

    KijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertTrue(result.containsColumn("group"));
    assertFalse(result.getQualifiers("group").contains("a"));
    assertTrue(result.getQualifiers("group").contains("b"));
    assertTrue(result.containsColumn("map"));
    assertTrue(result.containsColumn("memoryMap"));
  }

  @Test
  public void testDeleteColumnUpToTimestamp() throws IOException {
    mWriter.deleteColumn(mTable.getEntityId("alpha"), "group", "a", 2L);

    KijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertTrue(result.containsColumn("group"));
    assertEquals(1, result.getTimestamps("group", "a").size());
    assertTrue(result.getTimestamps("group", "a").contains(3L));
    assertTrue(result.getQualifiers("group").contains("b"));
    assertTrue(result.containsColumn("map"));
    assertTrue(result.containsColumn("memoryMap"));
  }

  @Test
  public void testDeleteCellMostRecent() throws IOException {
    mWriter.deleteCell(mTable.getEntityId("alpha"), "group", "a");

    KijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertTrue(result.containsColumn("group"));
    assertTrue(result.getQualifiers("group").contains("a"));
    assertTrue(result.getQualifiers("group").contains("b"));
    assertFalse(result.getTimestamps("group", "a").contains(3L));
    assertTrue(result.getTimestamps("group", "a").contains(2L));
    assertTrue(result.getTimestamps("group", "a").contains(1L));
    assertTrue(result.containsColumn("map"));
    assertTrue(result.containsColumn("memoryMap"));
  }

  @Test
  public void testDeleteCellAtTimestamp() throws IOException {
    mWriter.deleteCell(mTable.getEntityId("alpha"), "group", "a", 2L);

    KijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertTrue(result.containsColumn("group"));
    assertTrue(result.getQualifiers("group").contains("a"));
    assertTrue(result.getQualifiers("group").contains("b"));
    assertTrue(result.getTimestamps("group", "a").contains(3L));
    assertFalse(result.getTimestamps("group", "a").contains(2L));
    assertTrue(result.getTimestamps("group", "a").contains(1L));
    assertTrue(result.containsColumn("map"));
    assertTrue(result.containsColumn("memoryMap"));
  }

  /**
   * Creates a string cell.
   *
   * @param value The value to put in the string cell.
   * @return A Wibi cell.
   */
  private KijiCell<CharSequence> createCell(String value) {
    return new KijiCell<CharSequence>(Schema.create(Schema.Type.STRING), value);
  }
}
