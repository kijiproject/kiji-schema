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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import org.kiji.schema.KijiClientTest;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.schema.util.VersionInfo;

public class TestVersionTool extends KijiClientTest {
  @Test
  public void testVersionTool() throws Exception {
    final ProtocolVersion clientVersion = VersionInfo.getClientDataVersion();
    final VersionTool tool = new VersionTool();
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final PrintStream ps = new PrintStream(baos);
    tool.setConf(getConf());
    tool.setPrintStream(ps);
    final int exitCode = tool.toolMain(Lists.newArrayList("--debug",
        String.format("--kiji=%s", getKiji().getURI())));

    ps.close();
    final String toolOutput = Bytes.toString(baos.toByteArray());

    assertEquals(
        "kiji client software version: " + VersionInfo.getSoftwareVersion() + "\n"
        + "kiji client data version: " + clientVersion + "\n"
        + "kiji cluster data version: " + clientVersion + "\n"
        + "layout versions supported: "
            + KijiTableLayout.getMinSupportedLayoutVersion()
            + " to " + KijiTableLayout.getMaxSupportedLayoutVersion() + "\n",
        toolOutput);

    assertEquals(0, exitCode);
  }
}
