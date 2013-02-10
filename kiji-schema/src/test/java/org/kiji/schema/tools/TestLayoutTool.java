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
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.ToJson;

public class TestLayoutTool extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestLayoutTool.class);

  /** Horizontal ruler to delimit CLI outputs in logs. */
  private static final String RULER =
      "--------------------------------------------------------------------------------";

  /** Path to a region splits files. */
  public static final String REGION_SPLIT_KEY_FILE = "org/kiji/schema/tools/split-keys.txt";

  /** Output of the CLI tool, as bytes. */
  private ByteArrayOutputStream mToolOutputBytes = new ByteArrayOutputStream();

  /** Output of the CLI tool, as a single string. */
  private String mToolOutputStr;

  /** Output of the CLI tool, as an array of lines. */
  private String[] mToolOutputLines;

  private int runTool(BaseTool tool, String...arguments) throws Exception {
    mToolOutputBytes.reset();
    final PrintStream pstream = new PrintStream(mToolOutputBytes);
    tool.setConf(getConf());
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

  /**
   * Writes a table layout as a JSON descriptor in a temporary file.
   *
   * @param layoutDesc Table layout descriptor to write.
   * @return the temporary File where the layout has been written.
   * @throws Exception on error.
   */
  private File getTempLayoutFile(TableLayoutDesc layoutDesc) throws Exception {
    final File layoutFile = File.createTempFile(layoutDesc.getName(), ".json", getLocalTempDir());
    final OutputStream fos = new FileOutputStream(layoutFile);
    try {
      IOUtils.write(ToJson.toJsonString(layoutDesc), fos);
    } finally {
      fos.close();
    }
    return layoutFile;
  }

  @Test
  public void testChangeRowKeyHashing() throws Exception {
    final KijiTableLayout layout = KijiTableLayouts.getTableLayout(KijiTableLayouts.FOO_TEST);
    getKiji().createTable(layout.getName(), layout);

    final File newLayoutFile = getTempLayoutFile(KijiTableLayouts.getFooChangeHashingTestLayout());
    final KijiURI tableURI =
        KijiURI.newBuilder(getKiji().getURI()).withTableName(layout.getName()).build();

    assertEquals(BaseTool.FAILURE, runTool(new LayoutTool(),
        "--table=" + tableURI,
        "--do=set",
        "--layout=" + newLayoutFile
    ));
    assertTrue(mToolOutputLines[0].startsWith(
        "Error: Invalid layout update from reference row keys format"));
  }

}
