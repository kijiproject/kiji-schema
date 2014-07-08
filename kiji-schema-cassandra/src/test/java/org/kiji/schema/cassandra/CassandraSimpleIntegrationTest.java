/**
 * (c) Copyright 2014 WibiData, Inc.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.layout.KijiTableLayouts;

/**
 * Simple example Cassandra integration test.
 *
 * Just creates a table and inserts some data into it.
 */
public class CassandraSimpleIntegrationTest extends AbstractCassandraKijiIntegrationTest {
  @Test
  public void testSimpleTable() throws Exception {
    // Create the table.

    // Insert some data.

    // Read it back.
    Kiji kiji = getKiji();
    assertNotNull(kiji);
    kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE_FORMATTED_EID));
    KijiTable mTable = kiji.openTable("table");
    try {
      assertNotNull(mTable);
      // Fill local variables.
      KijiTableReader mReader = mTable.openTableReader();
      try {
        KijiTableWriter mWriter = mTable.openTableWriter();
        try {
          EntityId mEntityId = mTable.getEntityId("eid-foo");

          mWriter.put(mEntityId, "family", "column", 0L, "Value at timestamp 0.");
          mWriter.put(mEntityId, "family", "column", 1L, "Value at timestamp 1.");

          final KijiDataRequest dataRequest = KijiDataRequest.builder()
              .addColumns(
                  KijiDataRequestBuilder.ColumnsDef.create()
                      .withMaxVersions(2)
                      .add("family", "column"))
              .build();

          // Try this as a get.
          KijiRowData rowData = mReader.get(mEntityId, dataRequest);
          String s = rowData.getValue("family", "column", 0L).toString();
          assertEquals(s, "Value at timestamp 0.");

          // Delete the cell and make sure that this value is missing.
          mWriter.deleteCell(mEntityId, "family", "column", 0L);

          rowData = mReader.get(mEntityId, dataRequest);
          assertFalse(rowData.containsCell("family", "column", 0L));
        } finally {
          mWriter.close();
        }
      } finally {
        mReader.close();
      }
    } finally {
      mTable.release();
    }
  }
}
