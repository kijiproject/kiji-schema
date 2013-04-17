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

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestKijiToolLauncher {
  /**
   * Tests that we can lookup a tool where we expect.
   */
  @Test
  public void testGetToolName() {
    KijiTool tool = new KijiToolLauncher().getToolForName("ls");
    assertNotNull("Got a null tool back, should have gotten a real one.", tool);
    assertEquals("Tool does not advertise it responds to 'ls'.", "ls", tool.getName());
    assertTrue("Didn't get the LsTool we expected!", tool instanceof LsTool);
  }

  @Test
  public void testGetMissingTool() {
    KijiTool tool = new KijiToolLauncher().getToolForName("this-tool/can't1`exist");
    assertNull(tool);
  }

  @Test
  public void testSetsOptionsParsedFlag() throws Exception {
    Configuration conf = new Configuration();

    assertFalse(conf.getBoolean("mapred.used.genericoptionsparser", false));
    assertFalse(conf.getBoolean("mapreduce.client.genericoptionsparser.used", false));

    KijiToolLauncher launcher = new KijiToolLauncher();
    launcher.setConf(conf);
    KijiTool tool = launcher.getToolForName("help");
    launcher.run(tool, new String[0]);

    // Make sure Hadoop 1.x and Hadoop 2.x flags are both enabled by the launcher.
    assertTrue(conf.getBoolean("mapred.used.genericoptionsparser", false));
    assertTrue(conf.getBoolean("mapreduce.client.genericoptionsparser.used", false));
  }
}
