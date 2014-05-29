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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;

import org.apache.avro.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.util.ByteArrayFormatter;
import org.kiji.schema.util.BytesKey;


public class TestSchemaTableTool extends KijiToolTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestSchemaTableTool.class);

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

  /**
   * Similar to the above, but expects output on standard out.
   * @throws Exception
   */
  @Test
  public void testGetSchemaByIdStdOut() throws Exception {
    final long simpleSchemaId = getKiji().getSchemaTable().getOrCreateSchemaId(mSimpleSchema);
    assertEquals(BaseTool.SUCCESS, runTool(new SchemaTableTool(),
        getKiji().getURI().toString(),
        "--get-schema-by-id=" + simpleSchemaId
    ));
    assertTrue(mToolOutputStr.contains(mSimpleSchema.toString()));
  }

  @Test
  public void testList() throws Exception {
    final long simpleSchemaId = getKiji().getSchemaTable().getOrCreateSchemaId(mSimpleSchema);
    assertEquals(BaseTool.SUCCESS, runTool(new SchemaTableTool(),
        getKiji().getURI().toString(),
        "--list"));
    assertTrue(mToolOutputStr.contains(
        String.format("%d: %s%n", simpleSchemaId, mSimpleSchema.toString())));
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
  public void testWrongOperationCount() throws Exception {
    final BytesKey hash = mTable.getSchemaHash(mSimpleSchema);
    final long id = mTable.getOrCreateSchemaId(mSimpleSchema);
    try {
      runTool(new SchemaTableTool(),
          getKiji().getURI().toString(),
          "--get-schema-by-hash=" + hash,
          "--get-schema-by-id=" + id,
          "--lookup=" + mFile.toString(),
          "--output=" + mFile.toString(),
          "--interactive=false"
      );
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("Specify exactly one operation."));
    }
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
