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

package org.kiji.schema.impl.hbase;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;

/** Tests for HBaseKiji. */
public class TestHBaseKiji extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseKiji.class);

  /** Tests Kiji.openTable() on a table that doesn't exist. */
  @Test
  public void testOpenUnknownTable() throws Exception {
    final Kiji kiji = getKiji();

    try {
      final KijiTable table = kiji.openTable("unknown");
      Assert.fail("Should not be able to open a table that does not exist!");
    } catch (KijiTableNotFoundException ktnfe) {
      // Expected!
      LOG.debug("Expected error: {}", ktnfe);
      Assert.assertEquals("unknown", ktnfe.getTableURI().getTable());
    }
  }

  @Test
  public void testDeletingKijiTableWithUsersDoesNotFail() throws Exception {
    final Kiji kiji = getKiji();
    final TableLayoutDesc layoutDesc = KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST);
    final String tableName = layoutDesc.getName();
    kiji.createTable(layoutDesc);
    final KijiTable table = kiji.openTable(tableName);
    try {
      kiji.deleteTable(tableName);
    } finally {
      table.release();
    }
  }
}
