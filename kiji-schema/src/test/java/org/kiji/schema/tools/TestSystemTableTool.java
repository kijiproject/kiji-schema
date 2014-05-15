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

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;

public class TestSystemTableTool extends KijiToolTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestSystemTableTool.class);

  @Test
  public void testGetAll() throws Exception {
    final Kiji kiji = getKiji();
    kiji.getSystemTable().putValue("testKey", Bytes.toBytes("testValue"));
    final SystemTableTool st = new SystemTableTool();
    assertEquals(Bytes.toString(kiji.getSystemTable().getValue("testKey")), "testValue");

    assertEquals(BaseTool.SUCCESS, runTool(st, "--kiji=" + kiji.getURI(), "--do=get-all"));
    assertTrue(mToolOutputStr.startsWith("Listing all system table properties:"));
    assertTrue(mToolOutputStr.contains("data-version = "
        + kiji.getSystemTable().getDataVersion().toString()));
    assertTrue(mToolOutputStr, mToolOutputStr.contains("testKey = testValue"));
  }

  @Test
  public void testGet() throws Exception {
    final Kiji kiji = getKiji();
    kiji.getSystemTable().putValue("testGetKey", Bytes.toBytes("testGetValue"));
    final SystemTableTool st = new SystemTableTool();

    assertEquals(BaseTool.SUCCESS, runTool(
        st, "--kiji=" + kiji.getURI(), "--do=get", "testGetKey"));
    assertEquals(mToolOutputStr.trim(), "testGetKey = testGetValue");
  }

  @Test
  public void testPut() throws Exception {
    final Kiji kiji = getKiji();
    final SystemTableTool st = new SystemTableTool();

    assertEquals(BaseTool.SUCCESS, runTool(
        st, "--kiji=" + kiji.getURI(),
        "--do=put", "testPutKey", "testPutValue", "--interactive=false"));
    assertEquals(
        Bytes.toString(kiji.getSystemTable().getValue("testPutKey")), "testPutValue");
  }

  @Test
  public void testGetVersion() throws Exception {
    final Kiji kiji = getKiji();

    final SystemTableTool st = new SystemTableTool();
    assertEquals(BaseTool.SUCCESS, runTool(st, "--kiji=" + kiji.getURI(), "--do=get-version"));
    assertTrue(mToolOutputStr.startsWith("Kiji data version = "));
    assertTrue(mToolOutputStr.contains(kiji.getSystemTable().getDataVersion().toString()));
  }

  @Test
  public void testPutVersion() throws Exception {
    final Kiji kiji = getKiji();
    final SystemTableTool st = new SystemTableTool();

    assertEquals(BaseTool.SUCCESS, runTool(
        st, "--kiji=" + kiji.getURI(), "--do=put-version", "system-1.1", "--interactive=false"));
    assertEquals(kiji.getSystemTable().getDataVersion().toString(), "system-1.1");
  }

  @Test
  public void testUnknownKey() throws Exception {
    final Kiji kiji = getKiji();
    final SystemTableTool st = new SystemTableTool();
    assertEquals(
        BaseTool.FAILURE,
        runTool(st, "--kiji=" + kiji.getURI(), "--do=get", "invalidKey"));
    assertEquals(mToolOutputStr.trim(), "No system table property named 'invalidKey'.");
  }

    @Test
    public void testPutShouldNotPromptWhenOriginalValueIsNull() throws Exception {
        final Kiji kiji = getKiji();
        final SystemTableTool st = new SystemTableTool();

        assertEquals(BaseTool.SUCCESS, runToolWithInput(
                // in case we are prompted to overwrite the existing value for testPutKey we are
                // saying "no" which should cause the assertion below to fail
                st, "no", "--kiji=" + kiji.getURI(),
                "--do=put", "testPutKey", "testPutValue", "--interactive=true"));
        assertEquals(
                Bytes.toString(kiji.getSystemTable().getValue("testPutKey")), "testPutValue");
    }
}

