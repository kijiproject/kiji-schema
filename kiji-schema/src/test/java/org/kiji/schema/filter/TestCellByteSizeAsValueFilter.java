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

package org.kiji.schema.filter;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.SchemaStorage;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

/** Tests the StripValueRowFilter. */
public class TestCellByteSizeAsValueFilter extends KijiClientTest {
  /** Tests that the CellByteSizeAsValueFilter replaces the value by its size, in bytes. */
  @Test
  public void testCellByteSizeAsValueFilter() throws Exception {
    final Kiji kiji = new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE))
            .withRow("row")
                .withFamily("family")
                    .withQualifier("column")
                        .withValue("0123456789")
        .build();

    final KijiTable table = kiji.openTable("table");
    try {
      final EntityId eid = table.getEntityId("row");

      final KijiTableLayout layout = table.getLayout();
      final KijiColumnName column = KijiColumnName.create("family", "column");
      final Map<KijiColumnName, CellSpec> overrides =
          ImmutableMap.<KijiColumnName, CellSpec>builder()
          .put(column, layout.getCellSpec(column)
              .setCellSchema(CellSchema.newBuilder()
                  .setType(SchemaType.INLINE)
                  .setStorage(SchemaStorage.FINAL)
                  .setValue("{\"type\": \"fixed\", \"size\": 4, \"name\": \"Int32\"}")
                  .build()))
          .build();

      final KijiTableReader reader = table.getReaderFactory().openTableReader(overrides);
      try {
        final KijiDataRequest dataRequest = KijiDataRequest.builder()
            .addColumns(ColumnsDef.create().withFilter(new CellByteSizeAsValueFilter()).add(column))
            .build();
        final KijiRowData row = reader.get(eid, dataRequest);
        final GenericData.Fixed fixed32 = row.getMostRecentValue("family", "column");
        final int cellSize = Bytes.toInt(fixed32.bytes());

        // Cell size is: length(MD5-hash) + len(string size) + len(string)
        assertEquals(16 + 1 + 10, cellSize);

      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }
}
