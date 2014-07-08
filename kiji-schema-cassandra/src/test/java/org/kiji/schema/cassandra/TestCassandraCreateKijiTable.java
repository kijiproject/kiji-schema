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

package org.kiji.schema.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.VersionInfo;

/** Basic test for creating a simple Kiji table in Cassandra. */
public class TestCassandraCreateKijiTable extends CassandraKijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraCreateKijiTable.class);

  // TODO: Test locality group with max versions = 1 stores only one version.
  // Test creating a table with a locality group where max versions = 1, write multiple
  // versions of the same cell to the table, and then read back from the table with a data request
  // with max versions = infiinity and verify that we get only one result back.

  @Test
  public void testCreateKijiTable() throws Exception {
    LOG.info("Opening an in-memory kiji instance");
    final Kiji kiji = getKiji();

    LOG.info(String.format("Opened fake Kiji '%s'.", kiji.getURI()));

    final KijiSystemTable systemTable = kiji.getSystemTable();
    assertTrue("Client data version should support installed Kiji instance data version",
        VersionInfo.getClientDataVersion().compareTo(systemTable.getDataVersion()) >= 0);

    assertNotNull(kiji.getSchemaTable());
    assertNotNull(kiji.getMetaTable());

    kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE_FORMATTED_EID));

    final KijiTable table = kiji.openTable("table");
    try {

      final KijiTableWriter writer = table.openTableWriter();
      try {
        writer.put(table.getEntityId("row1"), "family", "column", 0L, "Value at timestamp 0.");
        writer.put(table.getEntityId("row1"), "family", "column", 1L, "Value at timestamp 1.");
      } finally {
        writer.close();
      }

      final KijiTableReader reader = table.openTableReader();
      try {
        final KijiDataRequest dataRequest = KijiDataRequest.builder()
            .addColumns(ColumnsDef.create().withMaxVersions(100).add("family", "column"))
            .build();

        // Try this as a get.
        final KijiRowData rowData = reader.get(table.getEntityId("row1"), dataRequest);
        String s = rowData.getValue("family", "column", 0L).toString();
        assertEquals(s, "Value at timestamp 0.");

        // Try this as a scan.
        int rowCounter = 0;
        final KijiRowScanner rowScanner = reader.getScanner(dataRequest);
        try {
          // Should be just one row, with two versions.
          for (KijiRowData kijiRowData : rowScanner) {
            LOG.info("Read from scanner! " + kijiRowData.toString());
            // Should see two versions for qualifier "column".
            assertEquals(
                "Value at timestamp 0.", kijiRowData.getValue("family", "column", 0L).toString());
            assertEquals(
                "Value at timestamp 1.", kijiRowData.getValue("family", "column", 1L).toString());
            rowCounter++;
          }
        } finally {
          rowScanner.close();
        }
        assertEquals(1, rowCounter);
      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }
}
