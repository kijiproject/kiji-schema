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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;
import org.kiji.schema.testutil.ToolResult;

/**
 * Integration test for the kiji administrative commands, e.g., create a table, scan it, delete it.
 */
public class IntegrationTestKijiAdminTools extends AbstractKijiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestKijiAdminTools.class);

  public static final String SPLIT_KEY_FILE = "org/kiji/schema/tools/split-keys.txt";

  /**
   * A utility function that returns a sorted list of table names in a string. Used to test the
   * output of 'kiji ls' in {@link #testTables() testTables}.
   *
   * @param names A List of table names in arbitrary order. Will be sorted in place.
   * @return A string consisting of the table names sorted case sensitively, each ending in a
   * newline.
   */
  private static String sortAndJoinTableNames(List<String> names) {
    Collections.sort(names);
    return StringUtils.join(names, "\n") + "\n";
  }

  private static List<String> namesOfDefaultKijiTables() {
    return Lists.newArrayList();
  }

  /** Removes the first line of a string. */
  private static String trimHead(String output) {
    return output.substring(output.indexOf("\n") + 1);
  }

  @Test
  public void testTables() throws Exception {
    final KijiURI fooTableURI = KijiURI.newBuilder(getKijiURI()).withTableName("foo").build();

    // The names of tables in the kiji instance. Updated as we add more.
    List<String> tableNames = namesOfDefaultKijiTables(); //
    // Fresh Kiji, so 'kiji ls' should report the default tables.
    ToolResult lsFreshResult = runTool(new LsTool(), new String[]{
      "--kiji=" + getKijiURI(),
    });
    assertEquals(0, lsFreshResult.getReturnCode());
    assertTrue(trimHead(lsFreshResult.getStdoutUtf8()).isEmpty());

    // Add a table.
    final File layoutFile =
        KijiTableLayouts.getTempFile(KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST));
    ToolResult createFooTableResult = runTool(new CreateTableTool(), new String[] {
      "--kiji=" + getKijiURI(),
      "--table=foo",
      "--layout=" + layoutFile,
    });
    assertEquals(0, createFooTableResult.getReturnCode());
    assertEquals("Parsing table layout: " + layoutFile + "\n"
        + "Creating kiji table: "
        + getKijiURI().toString() + "foo/...\n",
        createFooTableResult.getStdoutUtf8());

    // Now when we 'kiji ls' again, we should see table 'foo'.
    tableNames.add("foo");
    ToolResult lsFooResult = runTool(new LsTool(), new String[]{
      "--kiji=" + getKijiURI(),
    });
    assertEquals(0, lsFooResult.getReturnCode());
    assertEquals(sortAndJoinTableNames(tableNames), trimHead(lsFooResult.getStdoutUtf8()));

    // Synthesize some user data.
    ToolResult synthesizeResult = runTool(new SynthesizeUserDataTool(), new String[] {
      "--kiji=" + getKijiURI(),
      "--table=foo",
      "--num-users=10",
    });
    assertEquals(0, synthesizeResult.getReturnCode());
    assertEquals("Generating 10 users on kiji table '" + getKijiURI().toString() + "foo/'...\n"
        + "0 rows synthesized...\n"
        + "10 rows synthesized...\n"
        + "Done.\n",
        synthesizeResult.getStdoutUtf8());


    // Make sure there are 10 rows.
    ToolResult ls10RowsResult = runTool(new LsTool(), new String[] {
      "--kiji=" + fooTableURI,
    });
    assertEquals(0, ls10RowsResult.getReturnCode());
    LOG.debug("Output from 'kiji ls --table=foo' after synthesized users:\n"
        + ls10RowsResult.getStdoutUtf8());
    // 10 rows, each with:
    //   2 columns, each with:
    //     1 line for column name, timestamp.
    //     1 line for column data.
    //   1 blank line.
    // + 1 row for the header
    assertEquals(10 * ((2 * 2) + 1) + 1,
        StringUtils.countMatches(ls10RowsResult.getStdoutUtf8(), "\n"));


    // Look at just the "name" column for 3 rows.
    ToolResult ls3NameResult = runTool(new LsTool(), new String[] {
      "--kiji=" + fooTableURI,
      "--columns=info:name",
      "--max-rows=3",
    });
    assertEquals(0, ls3NameResult.getReturnCode());
    // 3 rows, each with:
    //   1 column, with:
    //     1 line for column name, timestamp.
    //     1 line for column data.
    //   1 blank line.
    // + 1 row for the header
    assertEquals(3 * ((1 * 2) + 1) + 1,
        StringUtils.countMatches(ls3NameResult.getStdoutUtf8(), "\n"));

    // Delete the foo table.
    ToolResult deleteResult = runTool(new DeleteTool(), new String[] {
      "--kiji=" + getKijiURI().toString(),
      "--table=foo",
      "--interactive=false",
    });
    assertEquals(0, deleteResult.getReturnCode());


    // Make sure there are only the standard tables left.
    tableNames.remove("foo");
    ToolResult lsCleanedUp = runTool(new LsTool(), new String[]{
      "--kiji=" + getKijiURI(),
    });
    assertEquals(0, lsCleanedUp.getReturnCode());
    assertTrue(trimHead(lsCleanedUp.getStdoutUtf8()).isEmpty());
  }

  @Test
  public void testKijiLsStartAndLimitRow() throws Exception {
    final KijiURI fooTableURI = KijiURI.newBuilder(getKijiURI()).withTableName("foo").build();
    createAndPopulateFooTable();
    try {
      // There should be 7 rows of input.
      ToolResult lsAllResult = runTool(new LsTool(), new String[] {
        "--kiji=" + fooTableURI,
        "--columns=info:name",
      });
      assertEquals(0, lsAllResult.getReturnCode());
      // Expect 6 rows of 'kiji ls' output, each with:
      //   1 column, with:
      //     1 line for the column name, timestamp.
      //     1 line for column data.
      //   1 blank line.
      // + 1 row for the header
      assertEquals(6 * ((1 * 2) + 1) + 1,
          StringUtils.countMatches(lsAllResult.getStdoutUtf8(), "\n"));

      // The foo table has row key hashing enabled.  Now let's run another 'kiji ls'
      // command starting from after the second row and before the last row, which
      // should only give us 3 results.  The start-row and limit-row keys here are just
      // numbers that I picked after looking at the results of the 'kiji ls' execution
      // above.
      ToolResult lsLimitResult = runTool(new LsTool(), new String[] {
        "--kiji=" + fooTableURI,
        "--columns=info:name",
        "--start-row=hex:50000000000000000000000000000000",  // after the second row.
        "--limit-row=hex:e0000000000000000000000000000000",  // before the last row.
      });
      assertEquals(0, lsLimitResult.getReturnCode());
      // Expect 2 rows of 'kiji ls' output, each with:
      //   1 column, with:
      //     1 line for the column name, timestamp.
      //     1 line for column data.
      //   1 blank line.
      // + 1 row for the header
      assertEquals(4 * ((1 * 2) + 1) + 1,
          StringUtils.countMatches(lsLimitResult.getStdoutUtf8(), "\n"));
    } finally {
      deleteFooTable();
    }
  }

  @Test
  public void testChangeRowKeyHashing() throws Exception {
    // Create a table.
    final File layoutFile =
        KijiTableLayouts.getTempFile(KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST));
    ToolResult createFooTableResult = runTool(new CreateTableTool(), new String[] {
      "--kiji=" + getKijiURI(),
      "--table=foo",
      "--layout=" + layoutFile,
    });
    assertEquals(0, createFooTableResult.getReturnCode());
    assertEquals("Parsing table layout: " + layoutFile + "\n"
        + "Creating kiji table: " + getKijiURI().toString() + "foo/...\n",
        createFooTableResult.getStdoutUtf8());

    // Attempt to change hashRowKeys (should not be possible).
    final File changedLayoutFile =
        KijiTableLayouts.getTempFile(KijiTableLayouts.getFooChangeHashingTestLayout());
    try {
      ToolResult setHashRowKeyResult = runTool(new LayoutTool(), new String[] {
        "--kiji=" + getKijiURI(),
        "--do=set",
        "--table=foo",
        "--layout=" + changedLayoutFile,
      });
      assertEquals(3, setHashRowKeyResult.getReturnCode());
    } catch (InvalidLayoutException e) {
      LOG.debug("Exception message: " + e.getMessage());
      assertTrue(e.getMessage().contains("Invalid layout update from reference row keys format"));
    } finally {
      // Delete the table.
      ToolResult deleteResult = runTool(new DeleteTool(), new String[] {
            "--kiji=" + getKijiURI(),
            "--table=foo",
            "--interactive=false",
          });
      assertEquals(0, deleteResult.getReturnCode());
    }

  }

  @Test
  public void testCreateHashedTableWithNumRegions() throws Exception {
    // Create a table.
    final File layoutFile =
        KijiTableLayouts.getTempFile(KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST));
    ToolResult createFooTableResult = runTool(new CreateTableTool(), new String[] {
      "--kiji=" + getKijiURI(),
      "--table=foo",
      "--layout=" + layoutFile,
      "--num-regions=" + 2,
    });
    assertEquals(0, createFooTableResult.getReturnCode());
    assertEquals("Parsing table layout: " + layoutFile + "\n"
        + "Creating kiji table: " + getKijiURI().toString() + "foo/...\n",
        createFooTableResult.getStdoutUtf8());

    // Delete the table.
    ToolResult deleteResult = runTool(new DeleteTool(), new String[] {
          "--kiji=" + getKijiURI(),
          "--table=foo",
          "--interactive=false",
        });
    assertEquals(0, deleteResult.getReturnCode());
  }

  @Test(expected=RuntimeException.class)
  public void testCreateHashedTableWithSplitKeys() throws Exception {
    // Create a table.
    final File layoutFile =
        KijiTableLayouts.getTempFile(KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST));
    String splitKeyPath = getClass().getClassLoader().getResource(SPLIT_KEY_FILE).getPath();
    @SuppressWarnings("unused")
    ToolResult createFooTableResult = runTool(new CreateTableTool(), new String[] {
      "--table=foo",
      "--layout=" + layoutFile,
      "--split-key-file=file://" + splitKeyPath,
    });
  }

  @Test(expected=RuntimeException.class)
  public void testCreateUnhashedTableWithNumRegions() throws Exception {
    // Create a table.
    final File layoutFile =
        KijiTableLayouts.getTempFile(KijiTableLayouts.getFooChangeHashingTestLayout());
    ToolResult createUnhashedTable = runTool(new CreateTableTool(), new String[]{
        "--kiji=" + getKijiURI(),
        "--table=foo",
        "--layout=" + layoutFile,
        "--num-regions=4",
    });
  }

  @Test
  public void testCreateUnhashedTableWithSplitKeys() throws Exception {
    // Create a table.
    final TableLayoutDesc desc = KijiTableLayouts.getFooUnhashedTestLayout();
    final String tableName = desc.getName();
    final File layoutFile = KijiTableLayouts.getTempFile(desc);
    String splitKeyPath = getClass().getClassLoader().getResource(SPLIT_KEY_FILE).getPath();
    ToolResult createFooTableResult = runTool(new CreateTableTool(), new String[] {
      "--kiji=" + getKijiURI(),
      "--table=" + tableName,
      "--layout=" + layoutFile,
      "--split-key-file=file://" + splitKeyPath,
    });
    assertEquals(0, createFooTableResult.getReturnCode());
    assertEquals("Parsing table layout: " + layoutFile + "\n"
        + "Creating kiji table: " + getKijiURI().toString() + tableName + "/...\n",
        createFooTableResult.getStdoutUtf8());

    // Delete the table.
    ToolResult deleteResult = runTool(new DeleteTool(), new String[] {
          "--kiji=" + getKijiURI().toString(),
          "--table=" + tableName,
          "--interactive=false",
        });
    assertEquals(0, deleteResult.getReturnCode());
  }

  @Test(expected=InvalidLayoutException.class)
  public void testCreateTableWithInvalidSchemaClassInLayout() throws Exception {
    final File layoutFile =
        KijiTableLayouts.getTempFile(KijiTableLayouts.getLayout(KijiTableLayouts.INVALID_SCHEMA));
    ToolResult createFailResult = runTool(new CreateTableTool(), new String[]{
        "--kiji=" + getKijiURI(),
        "--table=foo",
        "--layout=" + layoutFile,
    });
  }

  @Test
  public void testDeleteRow() throws Exception {
    createAndPopulateFooTable();
    final KijiURI fooTableURI = KijiURI.newBuilder(getKijiURI()).withTableName("foo").build();

    ToolResult deleteRowResult = runTool(new DeleteTool(), new String[] {
      "--kiji=" + getKijiURI(),
      "--table=foo",
      "--row=gwu@usermail.example.com",
      "--interactive=false",
    });
    assertEquals(0, deleteRowResult.getReturnCode());

    ToolResult lsCheckDeletion = runTool(new LsTool(), new String[] {
      "--kiji=" + fooTableURI,
      "--columns=info:email",
      "--entity-id=gwu@usermail.example.com",
    });
    assertEquals(0, lsCheckDeletion.getReturnCode());
    assertEquals(0, StringUtils.countMatches(lsCheckDeletion.getStdoutUtf8(), "@"));

    deleteFooTable();
  }

  @Test
  public void testDeleteFamily() throws Exception {
    createAndPopulateFooTable();
    final KijiURI fooTableURI = KijiURI.newBuilder(getKijiURI()).withTableName("foo").build();

    ToolResult deleteFamilyResult = runTool(new DeleteTool(), new String[] {
      "--kiji=" + getKijiURI().toString(),
      "--table=foo",
      "--row=gwu@usermail.example.com",
      "--family=info",
      "--interactive=false",
    });
    assertEquals(0, deleteFamilyResult.getReturnCode());

    ToolResult lsCheckDeletion = runTool(new LsTool(), new String[] {
      "--kiji=" + fooTableURI,
      "--columns=info:email",
      "--entity-id=gwu@usermail.example.com",
    });
    assertEquals(0, lsCheckDeletion.getReturnCode());
    assertEquals(0, StringUtils.countMatches(lsCheckDeletion.getStdoutUtf8(), "@"));

    deleteFooTable();
  }

  @Test
  public void testDeleteColumn() throws Exception {
    createAndPopulateFooTable();
    final KijiURI fooTableURI = KijiURI.newBuilder(getKijiURI()).withTableName("foo").build();

    ToolResult deleteColumnResult = runTool(new DeleteTool(), new String[] {
      "--kiji=" + getKijiURI(),
      "--table=foo",
      "--row=gwu@usermail.example.com",
      "--family=info",
      "--qualifier=email",
      "--interactive=false",
    });
    assertEquals(0, deleteColumnResult.getReturnCode());

    ToolResult lsCheckDeletion = runTool(new LsTool(), new String[] {
      "--kiji=" + fooTableURI,
      "--columns=info:email",
      "--entity-id=gwu@usermail.example.com",
    });
    assertEquals(0, lsCheckDeletion.getReturnCode());
    assertEquals(0, StringUtils.countMatches(lsCheckDeletion.getStdoutUtf8(), "@"));

    deleteFooTable();
  }

  @Test
  public void testDeleteMostRecent() throws Exception {
    createAndPopulateFooTable();
    final KijiURI fooTableURI = KijiURI.newBuilder(getKijiURI()).withTableName("foo").build();

    ToolResult deleteMostRecentResult = runTool(new DeleteTool(), new String[] {
      "--kiji=" + getKijiURI(),
      "--table=foo",
      "--row=gwu@usermail.example.com",
      "--family=info",
      "--qualifier=email",
      "--most-recent",
      "--interactive=false",
    });
    assertEquals(0, deleteMostRecentResult.getReturnCode());

    ToolResult lsCheckDeletion = runTool(new LsTool(), new String[] {
      "--kiji=" + fooTableURI,
      "--columns=info:email",
      "--entity-id=gwu@usermail.example.com",
    });
    assertEquals(0, lsCheckDeletion.getReturnCode());
    assertEquals(0, StringUtils.countMatches(lsCheckDeletion.getStdoutUtf8(), "@"));

    deleteFooTable();
  }

  @Test
  public void testDeleteExact() throws Exception {
    createAndPopulateFooTable();
    final KijiURI fooTableURI = KijiURI.newBuilder(getKijiURI()).withTableName("foo").build();

    Kiji testKiji = Kiji.Factory.open(getKijiURI(), getIntegrationHelper().getConf());
    KijiTable testTable = testKiji.openTable("foo");
    KijiTableWriter testWriter = testTable.openTableWriter();
    testWriter.put(testTable.getEntityId("gwu@usermail.example.com"), "info", "email",
      Long.MAX_VALUE - 1, "gwu@usermail.example.com");
    IOUtils.closeQuietly(testWriter);
    IOUtils.closeQuietly(testTable);
    testKiji.release();

    ToolResult deleteExactResult = runTool(new DeleteTool(), new String[] {
      "--kiji=" + getKijiURI(),
      "--table=foo",
      "--row=gwu@usermail.example.com",
      "--family=info",
      "--qualifier=email",
      "--exact-timestamp=" + (Long.MAX_VALUE - 1),
      "--interactive=false",
    });
    assertEquals(0, deleteExactResult.getReturnCode());

    ToolResult lsCheckDeletion = runTool(new LsTool(), new String[] {
      "--kiji=" + fooTableURI,
      "--columns=info:email",
      "--entity-id=gwu@usermail.example.com",
    });
    assertEquals(0, lsCheckDeletion.getReturnCode());
    assertEquals(0, StringUtils.countMatches(lsCheckDeletion.getStdoutUtf8(),
      "9223372036854775806"));

    deleteFooTable();
 }

  @Test
  public void testDeleteUpTo() throws Exception {
    createAndPopulateFooTable();
    final KijiURI fooTableURI = KijiURI.newBuilder(getKijiURI()).withTableName("foo").build();
    Kiji testKiji = Kiji.Factory.open(getKijiURI(), getIntegrationHelper().getConf());
    KijiTable testTable = testKiji.openTable("foo");
    KijiTableWriter testWriter = testTable.openTableWriter();
    testWriter.put(testTable.getEntityId("gwu@usermail.example.com"), "info", "email",
      Long.MAX_VALUE - 1, "gwu@usermail.example.com");
    IOUtils.closeQuietly(testWriter);
    IOUtils.closeQuietly(testTable);
    testKiji.release();

    ToolResult deleteUpToResult = runTool(new DeleteTool(), new String[] {
      "--kiji=" + getKijiURI(),
      "--table=foo",
      "--row=gwu@usermail.example.com",
      "--family=info",
      "--qualifier=email",
      "--upto-timestamp=" + (Long.MAX_VALUE - 1),
      "--interactive=false",
    });
    assertEquals(0, deleteUpToResult.getReturnCode());

    ToolResult lsCheckDeletion = runTool(new LsTool(), new String[] {
      "--kiji=" + fooTableURI,
      "--columns=info:email",
      "--entity-id=gwu@usermail.example.com",
    });
    assertEquals(0, lsCheckDeletion.getReturnCode());
    assertEquals(0, StringUtils.countMatches(lsCheckDeletion.getStdoutUtf8(), "@"));

    deleteFooTable();
 }
}
