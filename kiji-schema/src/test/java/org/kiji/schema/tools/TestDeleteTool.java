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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.schema.util.ResourceUtils;

public class TestDeleteTool extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestDeleteTool.class);

  /** Horizontal ruler to delimit CLI outputs in logs. */
  private static final String RULER =
      "--------------------------------------------------------------------------------";

  /** Output of the CLI tool, as bytes. */
  private ByteArrayOutputStream mToolOutputBytes = new ByteArrayOutputStream();

  /** Output of the CLI tool, as a single string. */
  private String mToolOutputStr;

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
    }
  }

  /** Table used to test against. Owned by this test. */
  private KijiTable mTable = null;

  private KijiTableReader mReader = null;

  // -----------------------------------------------------------------------------------------------

  @Before
  public final void setupTestDeleteTool() throws Exception {
    final KijiTableLayout layout = KijiTableLayouts.getTableLayout(KijiTableLayouts.SIMPLE);
    new InstanceBuilder(getKiji())
        .withTable(layout.getName(), layout)
            .withRow("row-1")
                .withFamily("family").withQualifier("column")
                    .withValue(313L, "value1")
                    .withValue(314L, "value2")
                    .withValue(315L, "value3")
        .build();
    mTable = getKiji().openTable(layout.getName());
    mReader = mTable.openTableReader();
  }

  @After
  public final void teardownTestDeleteTool() throws Exception {
    ResourceUtils.closeOrLog(mReader);
    ResourceUtils.releaseOrLog(mTable);
    mReader = null;
    mTable = null;
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  public void testDeleteAllCellsInRow() throws Exception {
    final KijiRowData rowBefore =
        mReader.get(mTable.getEntityId("row-1"), KijiDataRequest.create("family"));
    assertTrue(rowBefore.containsColumn("family"));

    assertEquals(BaseTool.SUCCESS, runTool(new DeleteTool(),
      "--target=" + mTable.getURI(),
      "--entity-id=row-1",
      "--interactive=false"
    ));

    final KijiRowData rowAfter =
        mReader.get(mTable.getEntityId("row-1"), KijiDataRequest.create("family"));
    assertFalse(rowAfter.containsColumn("family"));
  }

  @Test
  public void testDeleteAllCellsInFamilyFromRow() throws Exception {
    final KijiRowData rowBefore =
        mReader.get(mTable.getEntityId("row-1"), KijiDataRequest.create("family"));
    assertTrue(rowBefore.containsColumn("family"));

    // Target one column family:
    final KijiURI target =
        KijiURI.newBuilder(mTable.getURI()).addColumnName(new KijiColumnName("family")).build();

    assertEquals(BaseTool.SUCCESS, runTool(new DeleteTool(),
      "--target=" + target,
      "--entity-id=row-1",
      "--interactive=false"
    ));

    final KijiRowData rowAfter =
        mReader.get(mTable.getEntityId("row-1"), KijiDataRequest.create("family"));
    assertFalse(rowAfter.containsColumn("family"));
  }

  @Test
  public void testDeleteAllCellsInColumnFromRow() throws Exception {
    final KijiRowData rowBefore =
        mReader.get(mTable.getEntityId("row-1"), KijiDataRequest.create("family"));
    assertTrue(rowBefore.containsColumn("family"));

    // Target one column:
    final KijiURI target = KijiURI.newBuilder(mTable.getURI())
        .addColumnName(new KijiColumnName("family", "column"))
        .build();

    assertEquals(BaseTool.SUCCESS, runTool(new DeleteTool(),
      "--target=" + target,
      "--entity-id=row-1",
      "--interactive=false"
    ));

    final KijiRowData rowAfter =
        mReader.get(mTable.getEntityId("row-1"), KijiDataRequest.create("family"));
    assertFalse(rowAfter.containsColumn("family"));
  }

  @Test
  public void testDeleteMostRecentCellInColumnFromRow() throws Exception {
    final KijiDataRequestBuilder kdrb = KijiDataRequest.builder();
    kdrb.newColumnsDef().withMaxVersions(HConstants.ALL_VERSIONS)
        .add("family", "column");
    final KijiDataRequest kdr = kdrb.build();
    final KijiRowData rowBefore =
        mReader.get(mTable.getEntityId("row-1"), kdr);
    assertEquals(3, rowBefore.getValues("family", "column").size());
    assertEquals(315L, (long) rowBefore.getValues("family", "column").firstKey());

    // Target one column:
    final KijiURI target = KijiURI.newBuilder(mTable.getURI())
        .addColumnName(new KijiColumnName("family", "column"))
        .build();

    // Delete cells with latest timestamp, ie. timestamp == 315
    assertEquals(BaseTool.SUCCESS, runTool(new DeleteTool(),
      "--target=" + target,
      "--entity-id=row-1",
      "--timestamp=latest",
      "--interactive=false"
    ));

    final KijiRowData rowAfter =
        mReader.get(mTable.getEntityId("row-1"), kdr);
    assertEquals(2, rowAfter.getValues("family", "column").size());
    assertEquals(314L, (long) rowAfter.getValues("family", "column").firstKey());
  }

  @Test
  public void testDeleteExactTimestampCellInColumnFromRow() throws Exception {
    final KijiDataRequestBuilder kdrb = KijiDataRequest.builder();
    kdrb.newColumnsDef().withMaxVersions(HConstants.ALL_VERSIONS)
        .add("family", "column");
    final KijiDataRequest kdr = kdrb.build();
    final KijiRowData rowBefore =
        mReader.get(mTable.getEntityId("row-1"), kdr);
    assertEquals(3, rowBefore.getValues("family", "column").size());
    assertEquals(315L, (long) rowBefore.getValues("family", "column").firstKey());

    // Target one column:
    final KijiURI target = KijiURI.newBuilder(mTable.getURI())
        .addColumnName(new KijiColumnName("family", "column"))
        .build();

    // Delete cells with timestamp == 314
    assertEquals(BaseTool.SUCCESS, runTool(new DeleteTool(),
      "--target=" + target,
      "--entity-id=row-1",
      "--timestamp=314",
      "--interactive=false"
    ));

    final KijiRowData rowAfter =
        mReader.get(mTable.getEntityId("row-1"), kdr);
    assertEquals(2, rowAfter.getValues("family", "column").size());
    assertEquals(315L, (long) rowAfter.getValues("family", "column").firstKey());
    assertEquals(313L, (long) rowAfter.getValues("family", "column").lastKey());
  }

  @Test
  public void testDeleteUpToTimestampCellInColumnFromRow() throws Exception {
    final KijiDataRequestBuilder kdrb = KijiDataRequest.builder();
    kdrb.newColumnsDef().withMaxVersions(HConstants.ALL_VERSIONS)
        .add("family", "column");
    final KijiDataRequest kdr = kdrb.build();
    final KijiRowData rowBefore =
        mReader.get(mTable.getEntityId("row-1"), kdr);
    assertEquals(3, rowBefore.getValues("family", "column").size());
    assertEquals(315L, (long) rowBefore.getValues("family", "column").firstKey());

    // Target one column:
    final KijiURI target = KijiURI.newBuilder(mTable.getURI())
        .addColumnName(new KijiColumnName("family", "column"))
        .build();

    // Delete cells with timestamps <= 314
    assertEquals(BaseTool.SUCCESS, runTool(new DeleteTool(),
      "--target=" + target,
      "--entity-id=row-1",
      "--timestamp=upto:314",
      "--interactive=false"
    ));

    final KijiRowData rowAfter =
        mReader.get(mTable.getEntityId("row-1"), kdr);
    assertEquals(1, rowAfter.getValues("family", "column").size());
    assertEquals(315L, (long) rowAfter.getValues("family", "column").firstKey());
  }
}
