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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;

import com.google.common.collect.Lists;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiClientTest;

/**
 * Base class providing some helpers around running Kiji CLI tools. The tool output (both
 * as an array of lines and a single string (where the linse are joined by the "\n")) are returned
 * through the member fields mToolOutputLines and mToolOutputStr respectively.
 */
public abstract class KijiToolTest extends KijiClientTest {

  /** Horizontal ruler to delimit CLI outputs in logs. */
  private static final String RULER =
      "--------------------------------------------------------------------------------";

  /** Output of the CLI tool, as a single string. */
  protected String mToolOutputStr;

  /** Output of the CLI tool, as an array of lines. */
  protected String[] mToolOutputLines;

  private static final Logger LOG = LoggerFactory.getLogger(KijiToolTest.class);

  /**
   * Runs the given tool with the specified command line arguments.
   * @param tool is the tool to execute.
   * @param arguments are the arguments to pass into the tool.
   * @return the UNIX like status code representing the tool's execution result.
   * @throws Exception if there is an error when running the tool.
   */
  protected final int runTool(BaseTool tool, String... arguments) throws Exception {
    return runToolWithInput(tool, "", arguments);
  }

  /**
   * Runs the given tool with the specified command line arguments and user input.
   * @param tool is the tool to execute.
   * @param input is any user input the tool requires.
   * @param arguments are the arguments to pass into the tool.
   * @return the UNIX like status code representing the tool's execution result.
   * @throws Exception if there is an error when running the tool.
   */
  protected final int runToolWithInput(BaseTool tool, String input, String... arguments)
      throws Exception {
    final ByteArrayOutputStream toolOutputBytes = new ByteArrayOutputStream();
    final PrintStream pstream = new PrintStream(toolOutputBytes);
    final InputStream istream = new ByteArrayInputStream(input.getBytes());

    tool.setPrintStream(pstream);
    tool.setInputStream(istream);
    tool.setConf(getConf());

    try {
      LOG.info("Running tool: '{}' with parameters {}", tool.getName(), arguments);
      return tool.toolMain(Lists.newArrayList(arguments));
    } finally {
      pstream.flush();
      pstream.close();

      mToolOutputStr = Bytes.toString(toolOutputBytes.toByteArray());
      LOG.info("Captured output for tool: '{}' with parameters {}:\n{}\n{}{}\n",
          tool.getName(), arguments,
          RULER, mToolOutputStr, RULER);
      mToolOutputLines = mToolOutputStr.split("\n");
    }
  }
}
