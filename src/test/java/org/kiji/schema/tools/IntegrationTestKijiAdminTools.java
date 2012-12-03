// (c) Copyright 2011 WibiData, Inc.

package com.wibidata.core.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayouts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wibidata.core.JobHistoryWibiTable;
import com.wibidata.core.WibiIntegrationTest;
import com.wibidata.core.testutil.ToolResult;

/**
 * Integration test for the wibi administrative commands, e.g., create a table, scan it, delete it.
 */
public class IntegrationTestWibiAdminTools extends WibiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestWibiAdminTools.class);

  public static final String SPLIT_KEY_FILE = "com/wibidata/core/split-keys.txt";

  /**
   * A utility function that returns a sorted list of table names in a string. Used to test the
   * output of 'wibi ls' in {@link #testTables() testTables}.
   *
   * @param names A List of table names in arbitrary order. Will be sorted in place.
   * @return A string consisting of the table names sorted case sensitively, each ending in a
   * newline.
   */
  private String sortAndJoinTableNames(List<String> names) {
    Collections.sort(names);
    return StringUtils.join(names, "\n") + "\n";
  }

  public List<String> namesOfDefaultWibiTables() {
    List<String> names = new ArrayList<String>();
    names.add(JobHistoryWibiTable.getInstallName());
    return names;
  }

  @Test
  public void testTables() throws Exception {
    // The names of tables in the wibi instance. Updated as we add more.
    List<String> tableNames = namesOfDefaultWibiTables(); //
    // Fresh wibi, so 'wibi ls' should report the default tables.
    ToolResult lsFreshResult = runTool(new WibiLs(), new String[0]);
    assertEquals(0, lsFreshResult.getReturnCode());
    assertEquals(sortAndJoinTableNames(tableNames), lsFreshResult.getStdoutUtf8());

    // Add a table.
    final File layoutFile =
        KijiTableLayouts.getTempFile(KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST));
    ToolResult createFooTableResult = runTool(new WibiCreateTable(), new String[] {
      "--table=foo",
      "--layout=" + layoutFile,
    });
    assertEquals(0, createFooTableResult.getReturnCode());
    assertEquals("Parsing table layout: " + layoutFile + "\n"
        + "Creating wibi table: foo...\n",
        createFooTableResult.getStdoutUtf8());


    // Now when we 'wibi ls' again, we should see table 'foo'.
    tableNames.add("foo");
    ToolResult lsFooResult = runTool(new WibiLs(), new String[0]);
    assertEquals(0, lsFooResult.getReturnCode());
    assertEquals(sortAndJoinTableNames(tableNames), lsFooResult.getStdoutUtf8());


    // Synthesize some user data.
    ToolResult synthesizeResult = runTool(new WibiSynthesizeMoreUserData(), new String[] {
      "--table=foo",
      "--num-users=10",
    });
    assertEquals(0, synthesizeResult.getReturnCode());
    assertEquals("Generating 10 users on wibi table 'foo'...\n"
        + "0 rows synthesized...\n"
        + "10 rows synthesized...\n"
        + "Done.\n",
        synthesizeResult.getStdoutUtf8());


    // Make sure there are 10 rows.
    ToolResult ls10RowsResult = runTool(new WibiLs(), new String[] {
      "--table=foo",
    });
    assertEquals(0, ls10RowsResult.getReturnCode());
    LOG.debug("Output from 'wibi ls --table=foo' after synthesized users:\n"
        + ls10RowsResult.getStdoutUtf8());
    // 10 rows, each with:
    //   3 columns, each with:
    //     1 line for column name, timestamp.
    //     1 line for column data.
    //   1 blank line.
    assertEquals(10 * ((3 * 2) + 1),
        StringUtils.countMatches(ls10RowsResult.getStdoutUtf8(), "\n"));


    // Look at just the "name" column for 3 rows.
    ToolResult ls3NameResult = runTool(new WibiLs(), new String[] {
      "--table=foo",
      "--columns=info:name",
      "--max-rows=3",
    });
    assertEquals(0, ls3NameResult.getReturnCode());
    // 3 rows, each with:
    //   1 column, with:
    //     1 line for column name, timestamp.
    //     1 line for column data.
    //   1 blank line.
    assertEquals(3 * ((1 * 2) + 1),
        StringUtils.countMatches(ls3NameResult.getStdoutUtf8(), "\n"));

    // Look at just the "searches" map family for 3 rows.
    ToolResult ls4SearchesResult = runTool(new WibiLs(), new String[] {
      "--table=foo",
      "--columns=searches",
      "--max-rows=3",
    });
    assertEquals(0, ls4SearchesResult.getReturnCode());
    // 3 rows, each with:
    //   1 column, with:
    //     1 line for column name, timestamp.
    //     1 line for column data.
    //   1 blank line.
    assertEquals(3 * ((1 * 2) + 1),
        StringUtils.countMatches(ls4SearchesResult.getStdoutUtf8(), "\n"));

    // Look at just the "searches:tabby_cat" column for 3 rows.
    ToolResult ls5TabbySearchResult = runTool(new WibiLs(), new String[] {
      "--table=foo",
      "--columns=searches:tabby_cat",
      "--max-rows=3",
    });
    assertEquals(0, ls5TabbySearchResult.getReturnCode());
    // 1 rows, with:
    //   1 column, with:
    //     1 line for column name, timestamp.
    //     1 line for column data.
    //   1 blank line.
    assertEquals(1 * ((1 * 2) + 1),
        StringUtils.countMatches(ls5TabbySearchResult.getStdoutUtf8(), "\n"));


    // Delete the foo table.
    ToolResult deleteResult = runTool(new WibiDeleteTable(), new String[] {
      "--table=foo",
      "--confirm",
    });
    assertEquals(0, deleteResult.getReturnCode());
    assertEquals("Deleting wibi table: foo...\nOK.\n",
        deleteResult.getStdoutUtf8());


    // Make sure there are only the standard tables left.
    tableNames.remove("foo");
    ToolResult lsCleanedUp = runTool(new WibiLs(), new String[0]);
    assertEquals(0, lsCleanedUp.getReturnCode());
    assertEquals(sortAndJoinTableNames(tableNames), lsCleanedUp.getStdoutUtf8());
  }

  @Test
  public void testWibiLsStartAndLimitRow() throws Exception {
    createAndPopulateFooTable();
    try {
      // There should be 7 rows of input.
      ToolResult lsAllResult = runTool(new WibiLs(), new String[] {
        "--table=foo",
        "--columns=info:name",
      });
      assertEquals(0, lsAllResult.getReturnCode());
      // Expect 6 rows of 'wibi ls' output, each with:
      //   1 column, with:
      //     1 line for the column name, timestamp.
      //     1 line for column data.
      //   1 blank line.
      assertEquals(6 * ((1 * 2) + 1),
          StringUtils.countMatches(lsAllResult.getStdoutUtf8(), "\n"));

      // The foo table has row key hashing enabled.  Now let's run another 'wibi ls'
      // command starting from after the second row and before the last row, which
      // should only give us 3 results.  The start-row and limit-row keys here are just
      // numbers that I picked after looking at the results of the 'wibi ls' execution
      // above.
      ToolResult lsLimitResult = runTool(new WibiLs(), new String[] {
        "--table=foo",
        "--columns=info:name",
        "--start-row=hex:50000000000000000000000000000000",  // after the second row.
        "--limit-row=hex:e0000000000000000000000000000000",  // before the last row.
      });
      assertEquals(0, lsLimitResult.getReturnCode());
      // Expect 2 rows of 'wibi ls' output, each with:
      //   1 column, with:
      //     1 line for the column name, timestamp.
      //     1 line for column data.
      //   1 blank line.
      assertEquals(2 * ((1 * 2) + 1),
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
    ToolResult createFooTableResult = runTool(new WibiCreateTable(), new String[] {
      "--table=foo",
      "--layout=" + layoutFile,
    });
    assertEquals(0, createFooTableResult.getReturnCode());
    assertEquals("Parsing table layout: " + layoutFile + "\n"
        + "Creating wibi table: foo...\n",
        createFooTableResult.getStdoutUtf8());

    // Attempt to change hashRowKeys (should not be possible).
    final File changedLayoutFile =
        KijiTableLayouts.getTempFile(KijiTableLayouts.getFooChangeHashingTestLayout());
    boolean exceptionThrown = false;
    try {
      runTool(new WibiLayout(), new String[] {
        "--do=set",
        "--table=foo",
        "--layout=" + changedLayoutFile,
      });
    } catch (InvalidLayoutException e) {
      LOG.debug("Exception message: " + e.getMessage());
      assertTrue(e.getMessage().contains("Invalid layout update from reference row keys format"));
      exceptionThrown = true;
    } finally {
      // Delete the table.
      ToolResult deleteResult = runTool(new WibiDeleteTable(), new String[] {
            "--table=foo",
            "--confirm",
          });
      assertEquals(0, deleteResult.getReturnCode());
      assertEquals("Deleting wibi table: foo...\nOK.\n",
          deleteResult.getStdoutUtf8());
    }

    assertTrue(exceptionThrown);
  }

  @Test
  public void testCreateHashedTableWithNumRegions() throws Exception {
    // Create a table.
    final File layoutFile =
        KijiTableLayouts.getTempFile(KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST));
    ToolResult createFooTableResult = runTool(new WibiCreateTable(), new String[] {
      "--table=foo",
      "--layout=" + layoutFile,
      "--num-regions=" + 2,
    });
    assertEquals(0, createFooTableResult.getReturnCode());
    assertEquals("Parsing table layout: " + layoutFile + "\n"
        + "Creating wibi table: foo...\n",
        createFooTableResult.getStdoutUtf8());

    // Delete the table.
    ToolResult deleteResult = runTool(new WibiDeleteTable(), new String[] {
          "--table=foo",
          "--confirm",
        });
    assertEquals(0, deleteResult.getReturnCode());
    assertEquals("Deleting wibi table: foo...\nOK.\n",
        deleteResult.getStdoutUtf8());
  }

  @Test(expected=RuntimeException.class)
  public void testCreateHashedTableWithSplitKeys() throws Exception {
    // Create a table.
    final File layoutFile =
        KijiTableLayouts.getTempFile(KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST));
    String splitKeyPath = getClass().getClassLoader().getResource(SPLIT_KEY_FILE).getPath();
    @SuppressWarnings("unused")
    ToolResult createFooTableResult = runTool(new WibiCreateTable(), new String[] {
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
    runTool(new WibiCreateTable(), new String[] {
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
    ToolResult createFooTableResult = runTool(new WibiCreateTable(), new String[] {
      "--table=" + tableName,
      "--layout=" + layoutFile,
      "--split-key-file=file://" + splitKeyPath,
    });
    assertEquals(0, createFooTableResult.getReturnCode());
    assertEquals("Parsing table layout: " + layoutFile + "\n"
        + "Creating wibi table: " + tableName + "...\n",
        createFooTableResult.getStdoutUtf8());

    // Delete the table.
    ToolResult deleteResult = runTool(new WibiDeleteTable(), new String[] {
          "--table=" + tableName,
          "--confirm",
        });
    assertEquals(0, deleteResult.getReturnCode());
    assertEquals("Deleting wibi table: " + tableName + "...\nOK.\n",
        deleteResult.getStdoutUtf8());
  }

  @Test(expected=InvalidLayoutException.class)
  public void testCreateTableWithInvalidSchemaClassInLayout() throws Exception {
    final File layoutFile =
        KijiTableLayouts.getTempFile(KijiTableLayouts.getLayout(KijiTableLayouts.INVALID_SCHEMA));
    runTool(new WibiCreateTable(), new String[] {
      "--table=foo",
      "--layout=" + layoutFile,
    });
  }
}
