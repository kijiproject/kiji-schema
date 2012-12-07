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

import org.junit.Test;

import org.kiji.schema.testutil.AbstractKijiIntegrationTest;
import org.kiji.schema.testutil.ToolResult;
import org.kiji.schema.util.VersionInfo;

public class IntegrationTestKijiVersion extends AbstractKijiIntegrationTest {
  @Test
  public void testVersionTool() throws Exception {
    final String clientVersion = VersionInfo.getClientDataVersion();
    final ToolResult toolResult = runTool(new VersionTool(), new String[0]);
    assertEquals(0, toolResult.getReturnCode());
    assertEquals(
        "kiji client software version: " + VersionInfo.getSoftwareVersion() + "\n"
        + "kiji client data version: " + clientVersion + "\n"
        + "kiji cluster data version: " + clientVersion + "\n",
        toolResult.getStdoutUtf8());
  }
}
