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

import static org.kiji.schema.util.GetEquals.eqGet;
import static org.kiji.schema.util.ListGetEquals.eqListGet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.impl.HTableFactory;
import org.kiji.schema.layout.ColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestHBaseKijiTableReader extends KijiClientTest {
  @Before
  public void setupLayouts() throws Exception {
    getKiji().getMetaTable()
        .updateTableLayout("table", KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE));
    getKiji().getMetaTable()
        .updateTableLayout("user", KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
  }

  @Test
  public void testGet() throws Exception {
    // Create a mock htable.
    final HTable htable = createMock(HTable.class);

    // Create the kiji table.
    HBaseKijiTable table = new HBaseKijiTable(getKiji(), "table", new HTableFactory() {
      @Override
      public HTable create(Configuration conf, String hbaseTableName) throws IOException {
        return htable;
      }
    });

    // Construct the expected get request.
    Get expectedGet = new Get(table.getEntityId("foo").getHBaseRowKey());
    final ColumnNameTranslator columnNameTranslator =
        new ColumnNameTranslator(getKiji().getMetaTable().getTableLayout("table"));
    final KijiColumnName column = new KijiColumnName("family:column");
    final HBaseColumnName hcolumn = columnNameTranslator.toHBaseColumnName(column);
    expectedGet.addColumn(hcolumn.getFamily(), hcolumn.getQualifier());

    // And the canned result response.
    Result cannedResult = new Result(new KeyValue[] {
      new KeyValue(table.getEntityId("foo").getHBaseRowKey(),
          hcolumn.getFamily(),
          hcolumn.getQualifier(),
          new KijiCellEncoder(getKiji().getSchemaTable()).encode(
              new KijiCell<CharSequence>(Schema.create(Schema.Type.STRING), "Bob"),
              columnNameTranslator.getTableLayout().getCellFormat(column))),
    });

    // Set the expectation.
    expect(htable.get(eqGet(expectedGet))).andReturn(cannedResult);
    htable.close();

    replay(htable);

    KijiTableReader reader = table.openTableReader();
    KijiDataRequest dataRequest = new KijiDataRequest();
    dataRequest.addColumn(new KijiDataRequest.Column("family", "column"));
    KijiRowData rowData = reader.get(table.getEntityId("foo"), dataRequest);

    // Verify that the returned row data is as expected.
    assertTrue(rowData.containsColumn("family", "column"));
    assertEquals("Bob", rowData.getStringValue("family", "column").toString());

    reader.close();
    table.close();

    verify(htable);
  }

  @Test
  public void testGetCounter() throws Exception {
    // Create a mock htable so we can verify that the reader delegates the correct get() calls.
    final HTable htable = createMock(HTable.class);

    // Create the kiji table.
    HBaseKijiTable table = new HBaseKijiTable(getKiji(), "user", new HTableFactory() {
      @Override
      public HTable create(Configuration conf, String hbaseTableName) throws IOException {
        return htable;
      }
    });

    // Set the expected calls onto the mock htable.
    Get expectedGet = new Get(table.getEntityId("foo").getHBaseRowKey());
    ColumnNameTranslator columnNameTranslator = new ColumnNameTranslator(
        getKiji().getMetaTable().getTableLayout("user"));
    HBaseColumnName hcolumn = columnNameTranslator.toHBaseColumnName(
        new KijiColumnName("info:visits"));
    expectedGet.addColumn(hcolumn.getFamily(), hcolumn.getQualifier());
    Result cannedResult = new Result(new KeyValue[] {
        new KeyValue(
            table.getEntityId("foo").getHBaseRowKey(),
            hcolumn.getFamily(),
            hcolumn.getQualifier(),
            Bytes.toBytes(42L)),
    });

    expect(htable.get(eqGet(expectedGet))).andReturn(cannedResult);
    htable.close();
    replay(htable);

    // Read the counter!
    KijiTableReader reader = table.openTableReader();
    KijiDataRequest dataRequest = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("info", "visits"));
    KijiRowData rowData = reader.get(table.getEntityId("foo"), dataRequest);

    // Verify that the returned row data is as expected.
    assertTrue(rowData.containsColumn("info", "visits"));
    assertEquals(42L, rowData.getCounterValue("info", "visits"));

    reader.close();
    table.close();

    verify(htable);
  }

  @Test
  public void testBulkGet() throws Exception {
    // Note: HBaseKijiTableReader delegates work to get(EntityId, KijiDataRequest) when
    // only one item is passed in, thus this test must request multiple ids in order
    // to actually test the bulk get method.

    final HTable htable = createMock(HTable.class);
    // Create the kiji table.
    HBaseKijiTable table = new HBaseKijiTable(getKiji(), "table", new HTableFactory() {
      @Override
      public HTable create(Configuration conf, String hbaseTableName) throws IOException {
        return htable;
      }
    });
    KijiTableReader reader = table.openTableReader();
    // 1a- The backing HTable (mocked as 'htable') should expect a single bulk get request:
    ColumnNameTranslator columnNameTranslator = new ColumnNameTranslator(
        getKiji().getMetaTable().getTableLayout("table"));

    List<Get> expectedGets = new ArrayList<Get>(2);
    expectedGets.add(makeHBaseGet("FOO", "family:column", table, columnNameTranslator));
    expectedGets.add(makeHBaseGet("BAR", "family:column", table, columnNameTranslator));

    Result[] cannedResults = new Result[] {
        makeHBaseResult("FOO", "family:column", "foo-val", table, columnNameTranslator),
        makeHBaseResult("BAR", "family:column", "bar-val", table, columnNameTranslator),
    };

    expect(htable.get(eqListGet(expectedGets))).andReturn(cannedResults);
    htable.close();

    replay(htable);

    // Build the bulk get request.
    KijiDataRequest dataRequest = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("family", "column"));
    List<EntityId> entityIds = new ArrayList<EntityId>(2);
    entityIds.add(table.getEntityId("FOO"));
    entityIds.add(table.getEntityId("BAR"));
    List<KijiRowData> listRowData = reader.bulkGet(entityIds, dataRequest);

    assertEquals(2, listRowData.size());
    assertEquals("foo-val", listRowData.get(0).getStringValue("family", "column").toString());
    assertEquals("bar-val", listRowData.get(1).getStringValue("family", "column").toString());

    table.close();
    reader.close();

    verify(htable);
  }

  /**
   * Creates an hbase Get for a single entityId, and column.
   *
   * @param kijiRowKey Kiji row key to request.
   * @param columnName The kiji column name to create a Get for.
   * @param table The table that converts a kiji entity-id into an hbase row-id.
   * @param translator The ColumnNameTranslator to use when converting a kiji-name to an
   *     hbase name.
   * @return A Get for this column.
   */
  private static Get makeHBaseGet(String kijiRowKey, String columnName,
      KijiTable table, ColumnNameTranslator translator) throws IOException {

    final EntityId entityId = table.getEntityIdFactory().fromKijiRowKey(kijiRowKey);
    Get get = new Get(entityId.getHBaseRowKey());
    HBaseColumnName hColumn = translator.toHBaseColumnName(new KijiColumnName(columnName));
    get.addColumn(hColumn.getFamily(), hColumn.getQualifier());
    return get;
  }

  /**
   * Creates an hbase Result for a single entityId, and column, with
   * CharSequence value specified by <code>cellValue</code>.
   *
   * @param kijiRowKey Kiji row key to request.
   * @param columnName The kiji column name to create a Get for.
   * @param cellValue The (String) value of the cell to return.
   * @param table The table that converts a kiji entity-id into an hbase row-id.
   * @param translator The ColumnNameTranslator to use when converting a kiji-name to an
   *     hbase name.
   */
  private Result makeHBaseResult(String kijiRowKey, String columnName, String cellValue,
      KijiTable table, ColumnNameTranslator translator) throws IOException {
    final KijiColumnName column = new KijiColumnName(columnName);
    final HBaseColumnName hColumn = translator.toHBaseColumnName(column);
    final byte[] encodedKijiCell = new KijiCellEncoder(getKiji().getSchemaTable()).encode(
        new KijiCell<CharSequence>(Schema.create(Schema.Type.STRING), cellValue),
        translator.getTableLayout().getCellFormat(column));

    final EntityId entityId = table.getEntityIdFactory().fromKijiRowKey(kijiRowKey);
    return new Result(new KeyValue[] {
        new KeyValue(entityId.getHBaseRowKey(),
            hColumn.getFamily(),
            hColumn.getQualifier(),
            encodedKijiCell),
    });
  }
}
