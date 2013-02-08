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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.schema.util.ResourceUtils;

public class TestLsTool extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestLsTool.class);

  /** Horizontal ruler to delimit CLI outputs in logs. */
  private static final String RULER =
      "--------------------------------------------------------------------------------";

  /** Output of the CLI tool, as bytes. */
  private ByteArrayOutputStream mToolOutputBytes = new ByteArrayOutputStream();

  /** Output of the CLI tool, as a single string. */
  private String mToolOutputStr;

  /** Output of the CLI tool, as an array of lines. */
  private String[] mToolOutputLines;

  private int runTool(BaseTool tool, String...arguments) throws Exception {
    mToolOutputBytes.reset();
    final PrintStream pstream = new PrintStream(mToolOutputBytes);
    tool.setPrintStream(pstream);
    try {
      LOG.info("Running tool: '{}' with parameters {}", tool.getName(), arguments);
      return tool.toolMain(Lists.newArrayList(arguments));
    } finally {
      pstream.flush();
      pstream.close();

      mToolOutputStr = Bytes.toString(mToolOutputBytes.toByteArray());
      LOG.info("Captured output for tool: '{}' with parameters {}:\n{}\n{}{}\n",
          tool.getName(), arguments,
          RULER, mToolOutputStr, RULER);
      mToolOutputLines = mToolOutputStr.split("\n");
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Tests for the parsing of Kiji instance names from HBase table names for the
   * LsTool --instances functionality.
   */
  @Test
  public void testParseInstanceName() {
    assertEquals("default", LsTool.parseInstanceName("kiji.default.meta"));
    assertEquals("default", LsTool.parseInstanceName("kiji.default.schema_hash"));
    assertEquals("default", LsTool.parseInstanceName("kiji.default.schema_id"));
    assertEquals("default", LsTool.parseInstanceName("kiji.default.system"));
    assertEquals("default", LsTool.parseInstanceName("kiji.default.table.job_history"));
  }

  @Test
  public void testParseNonKijiTable() {
    String tableName = "hbase_table";
    assertEquals(null, LsTool.parseInstanceName(tableName));
  }

  @Test
  public void testParseMalformedKijiTableName() {
    assertEquals(null, LsTool.parseInstanceName("kiji.default"));
    assertEquals(null, LsTool.parseInstanceName("kiji."));
    assertEquals(null, LsTool.parseInstanceName("kiji"));
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  public void testListIntances() throws Exception {
    final Kiji kiji = getKiji();
    final KijiURI hbaseURI = KijiURI.newBuilder(kiji.getURI()).withInstanceName(null).build();

    final LsTool ls = new LsTool();
    assertEquals(BaseTool.SUCCESS, runTool(ls, "--kiji=" + hbaseURI));
    assertEquals(1, mToolOutputLines.length);
    assertEquals(kiji.getURI(), KijiURI.newBuilder(mToolOutputLines[0]).build());
  }

  @Test
  public void testListTables() throws Exception {
    final Kiji kiji = getKiji();

    assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), "--kiji=" + kiji.getURI()));
    assertEquals(1, mToolOutputLines.length);
    assertTrue(mToolOutputLines[0].startsWith("Listing tables in kiji instance:"));

    final KijiTableLayout layout = KijiTableLayouts.getTableLayout(KijiTableLayouts.SIMPLE);
    kiji.createTable(layout.getName(), layout);

    assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), "--kiji=" + kiji.getURI()));
    assertEquals(2, mToolOutputLines.length);
    assertTrue(mToolOutputLines[0].startsWith("Listing tables in kiji instance:"));
    assertEquals(layout.getName(), mToolOutputLines[1]);
  }

  @Test
  public void testScanTable() throws Exception {
    final Kiji kiji = getKiji();
    final KijiTableLayout layout = KijiTableLayouts.getTableLayout(KijiTableLayouts.SIMPLE);
    kiji.createTable(layout.getName(), layout);
    final KijiTable table = kiji.openTable(layout.getName());
    try {
      // Table is empty:
      assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), "--kiji=" + table.getURI()));
      assertEquals(1, mToolOutputLines.length);
      assertTrue(mToolOutputLines[0].startsWith("Scanning kiji table: "));

      new InstanceBuilder(kiji)
          .withTable(table)
              .withRow("hashed")
                  .withFamily("family").withQualifier("column").withValue(314L, "value")
          .build();

      // Table has now one row:
      assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), "--kiji=" + table.getURI()));
      assertEquals(3, mToolOutputLines.length);
      assertTrue(mToolOutputLines[0].startsWith("Scanning kiji table: "));
      assertTrue(mToolOutputLines[1].startsWith("entity-id=hbase=hex:"));

    } finally {
      ResourceUtils.releaseOrLog(table);
    }
  }

  @Test
  public void testFormattedRowKey() throws Exception {
    final Kiji kiji = getKiji();
    final KijiTableLayout layout = KijiTableLayouts.getTableLayout(KijiTableLayouts.FORMATTED_RKF);
    new InstanceBuilder(kiji)
        .withTable(layout.getName(), layout)
            .withRow("dummy", "str1", "str2", 1, 2L)
                .withFamily("family").withQualifier("column")
                    .withValue(1L, "string-value")
                    .withValue(2L, "string-value2")
            .withRow("dummy", "str1", "str2", 1)
                .withFamily("family").withQualifier("column").withValue(1L, "string-value")
            .withRow("dummy", "str1", "str2")
                .withFamily("family").withQualifier("column").withValue(1L, "string-value")
            .withRow("dummy", "str1")
                .withFamily("family").withQualifier("column").withValue(1L, "string-value")
            .withRow("dummy")
                .withFamily("family").withQualifier("column").withValue(1L, "string-value")
        .build();

    final KijiTable table = kiji.openTable(layout.getName());
    try {
      assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), "--kiji=" + table.getURI()));
      // TODO: Validate LsTool output
    } finally {
      ResourceUtils.releaseOrLog(table);
    }
  }

  @Test
  public void testKijiLsStartAndLimitRow() throws Exception {
    final Kiji kiji = getKiji();
    final KijiTableLayout layout = KijiTableLayouts.getTableLayout(KijiTableLayouts.FOO_TEST);
    final long timestamp = 10L;
    new InstanceBuilder(kiji)
        .withTable(layout.getName(), layout)
            .withRow("gwu@usermail.example.com")
                .withFamily("info")
                    .withQualifier("email").withValue(timestamp, "gwu@usermail.example.com")
                    .withQualifier("name").withValue(timestamp, "Garrett Wu")
            .withRow("aaron@usermail.example.com")
                .withFamily("info")
                    .withQualifier("email").withValue(timestamp, "aaron@usermail.example.com")
                    .withQualifier("name").withValue(timestamp, "Aaron Kimball")
            .withRow("christophe@usermail.example.com")
                .withFamily("info")
                    .withQualifier("email")
                        .withValue(timestamp, "christophe@usermail.example.com")
                    .withQualifier("name").withValue(timestamp, "Christophe Bisciglia")
            .withRow("kiyan@usermail.example.com")
                .withFamily("info")
                    .withQualifier("email").withValue(timestamp, "kiyan@usermail.example.com")
                    .withQualifier("name").withValue(timestamp, "Kiyan Ahmadizadeh")
            .withRow("john.doe@gmail.com")
                .withFamily("info")
                    .withQualifier("email").withValue(timestamp, "john.doe@gmail.com")
                    .withQualifier("name").withValue(timestamp, "John Doe")
            .withRow("jane.doe@gmail.com")
                .withFamily("info")
                    .withQualifier("email").withValue(timestamp, "jane.doe@gmail.com")
                    .withQualifier("name").withValue(timestamp, "Jane Doe")
        .build();

    final KijiTable table = kiji.openTable(layout.getName());
    try {
      assertEquals(BaseTool.SUCCESS, runTool(new LsTool(),
          "--kiji=" + table.getURI(),
          "--columns=info:name"
      ));
      // TODO: Validate output

      assertEquals(BaseTool.SUCCESS, runTool(new LsTool(),
          "--kiji=" + table.getURI(),
          "--columns=info:name",
          "--start-row=hex:50000000000000000000000000000000",  // after the second row.
          "--limit-row=hex:e0000000000000000000000000000000"  // before the last row.
      ));
      // TODO: Validate output
    } finally {
      ResourceUtils.releaseOrLog(table);
    }
  }
}
