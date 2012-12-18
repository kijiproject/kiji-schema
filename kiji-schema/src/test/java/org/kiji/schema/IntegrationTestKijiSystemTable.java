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

package org.kiji.schema;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.testutil.AbstractKijiIntegrationTest;

public class IntegrationTestKijiSystemTable extends AbstractKijiIntegrationTest {
  private Kiji mKiji;
  private static final String KEY = "some.system.property";
  private static final byte[] VALUE = Bytes.toBytes("property value I want to store.");

  @Before
  public void setup() throws IOException {
    mKiji = new Kiji(getKijiConfiguration());
  }

  @After
  public void teardown() throws IOException {
    mKiji.close(); // closes derived system table.
  }

  @Test
  public void testStoreVersion() throws IOException {
    KijiSystemTable systemTable = mKiji.getSystemTable();
    String originalDataVersion = systemTable.getDataVersion();
    systemTable.setDataVersion("99");
    assertEquals("99", systemTable.getDataVersion());
    systemTable.setDataVersion(originalDataVersion);
  }

  @Test
  public void testPutGet() throws IOException {
    KijiSystemTable systemTable = mKiji.getSystemTable();
    systemTable.putValue(KEY, VALUE);
    byte[] bytesBack = systemTable.getValue(KEY);
    assertArrayEquals(VALUE, bytesBack);
  }
}
