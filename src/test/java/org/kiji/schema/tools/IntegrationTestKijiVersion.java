// (c) Copyright 2011 WibiData, Inc.

package com.wibidata.core.tools;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.kiji.schema.util.VersionInfo;

import com.wibidata.core.WibiIntegrationTest;
import com.wibidata.core.testutil.ToolResult;

public class IntegrationTestWibiVersion extends WibiIntegrationTest {
  @Test
  public void testVersionTool() throws Exception {
    final String clientVersion = VersionInfo.getClientDataVersion();
    final ToolResult toolResult = runTool(new WibiVersion(), new String[0]);
    assertEquals(0, toolResult.getReturnCode());
    assertEquals(
        "Wibi client software version: " + VersionInfo.getSoftwareVersion() + "\n"
        + "Wibi client data version: " + clientVersion + "\n"
        + "Wibi cluster data version: " + clientVersion + "\n",
        toolResult.getStdoutUtf8());
  }
}
