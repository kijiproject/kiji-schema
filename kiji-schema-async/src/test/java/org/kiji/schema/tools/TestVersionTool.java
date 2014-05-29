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

import org.kiji.schema.Kiji;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.schema.util.VersionInfo;

public class TestVersionTool extends KijiToolTest {

  @Test
  public void testVersionTool() throws Exception {
    final ProtocolVersion clientDataVersion = VersionInfo.getClientDataVersion();
    final Kiji kiji = getKiji();

    final int exitCode =
        runTool(new VersionTool(), "--debug", String.format("--kiji=%s", kiji.getURI()));

    final ProtocolVersion clusterDataVersion = kiji.getSystemTable().getDataVersion();

    assertEquals(
        "kiji client software version: " + VersionInfo.getSoftwareVersion() + "\n"
        + "kiji client data version: " + clientDataVersion + "\n"
        + "kiji cluster data version: " + clusterDataVersion + "\n"
        + "layout versions supported: "
            + KijiTableLayout.getMinSupportedLayoutVersion()
            + " to " + KijiTableLayout.getMaxSupportedLayoutVersion() + "\n",
        mToolOutputStr);

    assertEquals(0, exitCode);
  }
}
