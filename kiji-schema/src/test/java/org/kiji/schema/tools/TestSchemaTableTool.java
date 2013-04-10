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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
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
import org.kiji.schema.util.ByteArrayFormatter;
import org.kiji.schema.util.BytesKey;


public class TestSchemaTableTool extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestSchemaTableTool.class);

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
    tool.setConf(getConf());
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

  /** Table used to test against. Owned by this test. */
  private KijiSchemaTable mTable = null;

  private Schema mSimpleSchema = Schema.create(Schema.Type.INT);

  private File mFile;

  // -----------------------------------------------------------------------------------------------

  @Before
  public final void setupTestSchematableTool() throws Exception {
    mTable = getKiji().getSchemaTable();
    mFile = File.createTempFile("TestSchemaTableTool", ".txt", getLocalTempDir());
    final FileOutputStream fop = new FileOutputStream(mFile.getAbsoluteFile());
    try {
      fop.write(mSimpleSchema.toString().getBytes("utf-8"));
      fop.flush();
    } finally {
      fop.close();
    }
  }

  @After
  public final void teardownTestSchemaTabletool() throws Exception {
    mFile.delete();
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  public void testRegisterSchema() throws Exception {
    assertEquals(BaseTool.SUCCESS, runTool(new SchemaTableTool(),
        getKiji().getURI().toString(),
        "--register=" + mFile.toString(),
        "--interactive=false"
    ));
    assertEquals(mSimpleSchema, mTable.getSchema(Long.parseLong(mToolOutputLines[0])));
  }

  @Test
  public void testGetSchemaById() throws Exception {
    final long simpleSchemaId = getKiji().getSchemaTable().getOrCreateSchemaId(mSimpleSchema);
    assertEquals(BaseTool.SUCCESS, runTool(new SchemaTableTool(),
        getKiji().getURI().toString(),
        "--get-schema-by-id=" + simpleSchemaId,
        "--output=" + mFile.toString(),
        "--interactive=false"
    ));
    assertEquals(mSimpleSchema, new Schema.Parser().parse(mFile));
  }

  @Test
  public void testGetSchemaByHash() throws Exception {
    final BytesKey hash = mTable.getSchemaHash(mSimpleSchema);
    assertEquals(BaseTool.SUCCESS, runTool(new SchemaTableTool(),
        getKiji().getURI().toString(),
        "--get-schema-by-hash=" + hash,
        "--output=" + mFile.toString(),
        "--interactive=false"
    ));

    assertEquals(mSimpleSchema, mTable.getSchema(Long.parseLong(mToolOutputLines[0])));
  }

  @Test
  public void lookupSchema() throws Exception {
    final BytesKey hash = mTable.getSchemaHash(mSimpleSchema);
    final long id = mTable.getOrCreateSchemaId(mSimpleSchema);
    assertEquals(BaseTool.SUCCESS, runTool(new SchemaTableTool(),
        getKiji().getURI().toString(),
        "--lookup=" + mFile.toString(),
        "--interactive=false"
    ));
    assertEquals(id, Long.parseLong(mToolOutputLines[0]));
    assertEquals(hash, new BytesKey(ByteArrayFormatter.parseHex(mToolOutputLines[1], ':')));
  }

  @Test
  public void loop() throws Exception {
    // Get the id and hash of a schema.
    assertEquals(BaseTool.SUCCESS, runTool(new SchemaTableTool(),
        getKiji().getURI().toString(),
        "--lookup=" + mFile.toString(),
        "--interactive=false"
        ));
    final long id = Long.parseLong(mToolOutputLines[0]);
    final BytesKey hash = new BytesKey(ByteArrayFormatter.parseHex(mToolOutputLines[1], ':'));

    // Confirm the id returns the correct schema and hash.
    assertEquals(BaseTool.SUCCESS, runTool(new SchemaTableTool(),
        getKiji().getURI().toString(),
        "--get-schema-by-id=" + id,
        "--output=" + mFile.toString(),
        "--interactive=false"
        ));
    assertEquals(mSimpleSchema, new Schema.Parser().parse(mFile));
    assertEquals(hash, new BytesKey(ByteArrayFormatter.parseHex(mToolOutputLines[0], ':')));

    // Confirm the hash returns the correct schema and id.
    assertEquals(BaseTool.SUCCESS, runTool(new SchemaTableTool(),
        getKiji().getURI().toString(),
        "--get-schema-by-hash=" + hash.toString(),
        "--output=" + mFile.toString(),
        "--interactive=false"
        ));
    assertEquals(mSimpleSchema, new Schema.Parser().parse(mFile));
    assertEquals(id, Long.parseLong(mToolOutputLines[0]));
  }
}
