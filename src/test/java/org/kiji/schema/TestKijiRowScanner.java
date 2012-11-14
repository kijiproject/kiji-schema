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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.kiji.schema.util.ScanEquals.eqScan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.impl.HBaseDataRequestAdapter;
import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.impl.HTableFactory;
import org.kiji.schema.layout.ColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestKijiRowScanner extends KijiClientTest {
  @Before
  public void setupLayout() throws Exception {
    getKiji().getMetaTable()
        .updateTableLayout("table", KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE));
  }

  @Test
  public void testScanner() throws Exception {
    // Create a mock htable.
    final HTable htable = createMock(HTable.class);

    // Create the kiji table.
    HBaseKijiTable table = new HBaseKijiTable(getKiji(), "table", new HTableFactory() {
      @Override
      public HTable create(Configuration conf, String hbaseTableName) throws IOException {
        return htable;
      }
    });

    final KijiDataRequest dataRequest = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("family", "column"));

    // Construct the expected get request.
    final HBaseDataRequestAdapter dataRequestAdapter = new HBaseDataRequestAdapter(dataRequest);
    final KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    final Scan expectedScan = dataRequestAdapter.toScan(tableLayout);

    expectedScan.setStartRow(table.getEntityId("foo").getHBaseRowKey());

    final ResultScanner cannedResultScanner = createMock(ResultScanner.class);
    final ArrayList<Result> cannedIterable = new ArrayList<Result>();
    final ColumnNameTranslator columnNameTranslator = new ColumnNameTranslator(tableLayout);
    final KijiColumnName column = new KijiColumnName("family:column");
    final HBaseColumnName hcolumn = columnNameTranslator.toHBaseColumnName(column);

    final KijiCellEncoder encoder = new KijiCellEncoder(getKiji().getSchemaTable());
    final KijiCellFormat format = tableLayout.getCellFormat(column);

    final Result cannedResult1 = new Result(new KeyValue[] {
      new KeyValue(table.getEntityId("foo").getHBaseRowKey(),
          hcolumn.getFamily(),
          hcolumn.getQualifier(),
          encoder.encode(new KijiCell<Integer>(Schema.create(Schema.Type.INT), 2), format)),
    });

    final Result cannedResult2 = new Result(new KeyValue[] {
      new KeyValue(table.getEntityId("foo").getHBaseRowKey(),
          hcolumn.getFamily(),
          hcolumn.getQualifier(),
          encoder.encode(new KijiCell<Integer>(Schema.create(Schema.Type.INT), 4), format)),
    });

    final Result cannedResult3 = new Result(new KeyValue[] {
      new KeyValue(table.getEntityId("foo").getHBaseRowKey(),
          hcolumn.getFamily(),
          hcolumn.getQualifier(),
          encoder.encode(new KijiCell<Integer>(Schema.create(Schema.Type.INT), 6), format)),
    });

    cannedIterable.add(cannedResult1);
    cannedIterable.add(cannedResult2);
    cannedIterable.add(cannedResult3);

    // Set the expectation.
    expect(htable.getScanner(eqScan(expectedScan))).andReturn(cannedResultScanner);
    expect(cannedResultScanner.iterator()).andReturn(cannedIterable.listIterator());
    expect(cannedResultScanner.iterator()).andReturn(cannedIterable.listIterator());
    cannedResultScanner.close();
    replay(cannedResultScanner);
    htable.close();
    replay(htable);

    KijiTableReader reader = table.openTableReader();
    KijiRowScanner scanner = reader.getScanner(dataRequest, table.getEntityId("foo"), null);
    Iterator<KijiRowData> iterator = scanner.iterator();

    assertTrue(iterator.hasNext());
    assertEquals(Integer.valueOf(2), iterator.next().getIntValue("family", "column"));
    assertTrue(iterator.hasNext());
    assertEquals(Integer.valueOf(4), iterator.next().getIntValue("family", "column"));

    // Open another iterator on the scanner.
    int sum = 0;
    for (KijiRowData kijiRowData : scanner) {
      sum += kijiRowData.getIntValue("family", "column");
    }
    assertEquals(12, sum);

    // Test original iterator continues as expected.
    assertTrue(iterator.hasNext());
    assertEquals(Integer.valueOf(6), iterator.next().getIntValue("family", "column"));
    assertTrue(!iterator.hasNext());

    scanner.close();
    reader.close();
    table.close();

    verify(htable);
    verify(cannedResultScanner);
  }
}
