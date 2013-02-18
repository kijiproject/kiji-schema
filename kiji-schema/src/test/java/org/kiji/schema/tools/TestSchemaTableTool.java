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

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiSchemaTable;


public class TestSchemaTableTool extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestSchemaTableTool.class);

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
  private KijiSchemaTable mTable = null;

  private Schema mSimpleSchema = Schema.create(Schema.Type.INT);

  private long mSimpleSchemaId;

  private File mFile;

  // -----------------------------------------------------------------------------------------------

  @Before
  public final void setupTestSchematableTool() throws Exception {
    mFile = new File("./SchemaTableTest");
    mTable = getKiji().getSchemaTable();
    final FileWriter fw = new FileWriter(mFile);
    final BufferedWriter bw = new BufferedWriter(fw);
    bw.write(mSimpleSchema.toString());
    bw.close();
  }

  @After
  public final void teardownTestSchemaTabletool() throws Exception {
    mFile.delete();
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  public void testCreateSchema() throws Exception {
    assertEquals(BaseTool.SUCCESS, runTool(new SchemaTableTool(),
        "--kiji=" + getKiji().getURI(),
        "--get-or-create=./SchemaTableTest",
        "--interactive=false"
    ));
    assertEquals(mSimpleSchema, mTable.getSchema(Long.parseLong(mToolOutputStr.trim())));
  }

  @Test
  public void testGetSchema() throws Exception {
    mSimpleSchemaId = getKiji().getSchemaTable().getOrCreateSchemaId(mSimpleSchema);
    assertEquals(BaseTool.SUCCESS, runTool(new SchemaTableTool(),
        "--kiji=" + getKiji().getURI(),
        String.format("--get-schema=%d", mSimpleSchemaId),
        "--output=SchemaTableTest",
        "--interactive=false"
    ));
    assertEquals(new Schema.Parser().parse(mFile), mSimpleSchema);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testBothModes() throws Exception {
    runTool(new SchemaTableTool(),
        "--kiji=" + getKiji().getURI(),
        "--get-or-create=./SchemaTableTest",
        "--get-schema=1",
        "--interactive=false"
        );
  }

  @Test(expected=IOException.class)
  public void testInvalidInputFile() throws Exception {
    runTool(new SchemaTableTool(),
        "--kiji=" + getKiji().getURI(),
        "--get-or-create=./invalidFile",
        "--interactive=false"
        );
  }

  @Test(expected=IllegalArgumentException.class)
  public void testNoSchema() throws Exception {
    runTool(new SchemaTableTool(),
        "--kiji=" + getKiji().getURI(),
        "--get-schema=-2",
        "--output=./SchemaTableTest",
        "--interactive=false"
        );
  }
}
