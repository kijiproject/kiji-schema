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

public class TestLsTool {
  /**
   * Tests for the parsing of Kiji instance names from HBase table names for the
   * LsTool --instances functionality.
   */
  @Test
  public void testParseInstanceName() {
    assertEquals("default", LsTool.parseInstanceName("kiji.default.meta"));
    assertEquals("default", LsTool.parseInstanceName("kiji.default.schema_hash"));
    assertEquals("default", LsTool.parseInstanceName("kiji.default.schema_id"));
    assertEquals("default", LsTool.parseInstanceName("kiji.default.system"));
    assertEquals("default", LsTool.parseInstanceName("kiji.default.table.job_history"));
  }

  @Test
  public void testParseNonKijiTable() {
    String tableName = "hbase_table";
    assertEquals(null, LsTool.parseInstanceName(tableName));
  }

  @Test
  public void testParseMalformedKijiTableName() {
    assertEquals(null, LsTool.parseInstanceName("kiji.default"));
    assertEquals(null, LsTool.parseInstanceName("kiji."));
    assertEquals(null, LsTool.parseInstanceName("kiji"));
  }
}
