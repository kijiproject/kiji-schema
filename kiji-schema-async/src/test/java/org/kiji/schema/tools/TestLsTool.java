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

import java.util.Set;

import com.google.common.collect.Sets;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.schema.util.ResourceUtils;

public class TestLsTool extends KijiToolTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestLsTool.class);

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
  public void testListInstances() throws Exception {
    final Kiji kiji = getKiji();
    final KijiURI hbaseURI = KijiURI.newBuilder(kiji.getURI()).withInstanceName(null).build();

    final LsTool ls = new LsTool();
    assertEquals(BaseTool.SUCCESS, runTool(ls, hbaseURI.toString()));
    final Set<String> instances = Sets.newHashSet(mToolOutputLines);
    assertTrue(instances.contains(kiji.getURI().toString()));
  }

  @Test
  public void testListTables() throws Exception {
    final Kiji kiji = getKiji();

    assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), kiji.getURI().toString()));
    assertEquals(1, mToolOutputLines.length);

    final KijiTableLayout layout = KijiTableLayouts.getTableLayout(KijiTableLayouts.SIMPLE);
    kiji.createTable(layout.getDesc());

    assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), kiji.getURI().toString()));
    assertEquals(1, mToolOutputLines.length);
    assertEquals(kiji.getURI() + layout.getName(), mToolOutputLines[0]);
  }

  @Test
  public void testTableColumns() throws Exception {
    final Kiji kiji = getKiji();
    final KijiTableLayout layout = KijiTableLayouts.getTableLayout(KijiTableLayouts.SIMPLE);
    kiji.createTable(layout.getDesc());
    final KijiTable table = kiji.openTable(layout.getName());
    try {
      // Table is empty:
      assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), table.getURI().toString()));
      assertEquals(1, mToolOutputLines.length);
      assertTrue(mToolOutputLines[0].contains("family:column"));
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
      assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), table.getURI().toString()));
    } finally {
      ResourceUtils.releaseOrLog(table);
    }
  }

  @Test
  public void testKijiLsStartAndLimitRow() throws Exception {
    final Kiji kiji = getKiji();
    final KijiTableLayout layout = KijiTableLayouts.getTableLayout(KijiTableLayouts.FOO_TEST);
    kiji.createTable(layout.getDesc());
    final KijiTable table = kiji.openTable(layout.getName());
    try {
      assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), table.getURI().toString()));
      // TODO: Validate output
    } finally {
      ResourceUtils.releaseOrLog(table);
    }
  }


  @Test
  public void testMultipleArguments() throws Exception {
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

    final KijiTableLayout layoutTwo = KijiTableLayouts.getTableLayout(KijiTableLayouts.FOO_TEST);
    kiji.createTable(layoutTwo.getDesc());

    final KijiTable table = kiji.openTable(layout.getName());
    try {
      final KijiTable tableTwo = kiji.openTable(layoutTwo.getName());
      try {
        assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), table.getURI().toString(),
            tableTwo.getURI().toString()));
        assertEquals(9, mToolOutputLines.length);
        assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), kiji.getURI().toString()));
        assertEquals(2, mToolOutputLines.length);
        assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), kiji.getURI().toString(),
            table.getURI().toString()));
        assertEquals(3, mToolOutputLines.length);
      } finally {
        tableTwo.release();
      }
    } finally {
      table.release();
    }
  }

  @Test
  public void testTableNoFamilies() throws Exception {
    final Kiji kiji = new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(KijiTableLayouts.NOFAMILY))
        .build();
    final KijiTable table = kiji.openTable("nofamily");
    try {
      assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), table.getURI().toString()));
    } finally {
      table.release();
    }
  }
}
