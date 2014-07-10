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
package org.kiji.schema.impl.hbase;

import java.io.IOException;
import java.util.Iterator;

import junit.framework.Assert;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestHBaseKijiResultPerformance extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseKijiResultPerformance.class);

  private void setupPerformanceTest() throws IOException {
    final InstanceBuilder.TableBuilder tableBuilder = new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(KijiTableLayouts.ROW_DATA_TEST));

    final InstanceBuilder.FamilyBuilder familyBuilder =
        tableBuilder.withRow("foo").withFamily("map");
    for (int j = 0; j < 100; j++) {
      final InstanceBuilder.QualifierBuilder qualifierBuilder =
          familyBuilder.withQualifier(String.valueOf(j));
      for (int k = 1; k <= 1000; k++) {
        qualifierBuilder.withValue(k, 1);
      }
    }
    tableBuilder.build();
  }

  private void warmupPerformanceTest(
      final KijiTable table,
      final HBaseKijiTableReader reader,
      final KijiDataRequest request
  ) throws IOException {
    final KijiRowData warmupRowData = reader.get(table.getEntityId("foo"), request);
    for (KijiCell<Integer> cell : warmupRowData.<Integer>asIterable("map")) {
      cell.getData();
    }
    final KijiResult<Object> warmupResult = reader.getResult(table.getEntityId("foo"), request);
    try {
      final KijiResult<Integer> columnResult =
          warmupResult.narrowView(KijiColumnName.create("map"));
      try {
        for (final KijiCell<Integer> integerKijiCell : columnResult) {
          integerKijiCell.getData();
        }
      } finally {
        columnResult.close();
      }
    } finally {
      warmupResult.close();
    }
  }

  private void resultFromRowData(
      final HBaseKijiTableReader reader,
      final EntityId eid,
      final KijiDataRequest request
  ) throws IOException {
    final long rowDataToResultTime = System.nanoTime();
    final HBaseKijiRowData testRowData =
        (HBaseKijiRowData) reader.get(eid, request);
    final long rowDataEndTime = System.nanoTime();
    testRowData.asKijiResult();
    final long resultEndTime = System.nanoTime();
    LOG.info("built row data in {} milliseconds",
        (double) (rowDataEndTime - rowDataToResultTime) / 1000000);
    LOG.info("built result from row data in {} milliseconds",
        (double) (resultEndTime - rowDataEndTime) / 1000000);
  }

  private void allValuesMapFamily(
      final HBaseKijiTable table,
      final HBaseKijiTableReader reader,
      final KijiDataRequest request
  ) throws IOException {
    {
      final long rawHBaseStartTime = System.nanoTime();
      final HBaseDataRequestAdapter adapter =
          new HBaseDataRequestAdapter(request, table.getColumnNameTranslator());
      final Get get = adapter.toGet(table.getEntityId("foo"), table.getLayout());
      final HTableInterface hTable = table.openHTableConnection();
      try {
        final Result result = hTable.get(get);
      } finally {
        hTable.close();
      }
      LOG.info("raw hbase time = {} milliseconds",
          (double) (System.nanoTime() - rawHBaseStartTime) / 1000000);
    }
    {
      final long rowDataStartTime = System.nanoTime();
      final KijiRowData testRowData = reader.get(table.getEntityId("foo"), request);
      testRowData.containsCell("family", "qualifier", 1);
      LOG.info("built row data in {} milliseconds",
          (double) (System.nanoTime() - rowDataStartTime) / 1000000);
      int seen = 0;
      for (KijiCell<Integer> cell : testRowData.<Integer>asIterable("map")) {
        Object v = cell.getData();
        seen++;
      }
      LOG.info("row data all map family time (saw {} cells) = {} milliseconds",
          seen, (double) (System.nanoTime() - rowDataStartTime) / 1000000);
    }
    {
      final long resultStartTime = System.nanoTime();
      final KijiResult<Integer> testResult = reader.getResult(table.getEntityId("foo"), request);
      try {
        LOG.info(
            "built result in {} milliseconds",
            (double) (System.nanoTime() - resultStartTime) / 1000000);
        final long itstart = System.nanoTime();
        final Iterator<KijiCell<Integer>> it = testResult.iterator();
        LOG.info(
            "built iterator in {} milliseconds",
            (double) (System.nanoTime() - itstart) / 1000000);
        int seen = 0;
        while (it.hasNext()) {
          Object v = it.next().getData();
          seen++;
        }
        LOG.info(
            "result all map family time (saw {} cells) = {} milliseconds",
            seen, (double) (System.nanoTime() - resultStartTime) / 1000000);
      } finally {
        testResult.close();
      }
    }
  }

  private void singleValue(
      final HBaseKijiTable table,
      final HBaseKijiTableReader reader
  ) throws IOException {
    {
      final KijiDataRequest singletonRequest = KijiDataRequest.create("map", "10");
      final long rowDataStartTime = System.nanoTime();
      final KijiRowData testRowData = reader.get(table.getEntityId("foo"), singletonRequest);
      testRowData.containsCell("family", "qualifier", 1);
      final Integer value = testRowData.getMostRecentValue("map", "10");
      LOG.info("row data single value time = {} nanoseconds",
          (double) (System.nanoTime() - rowDataStartTime) / 1000000);

      final long resultStartTime = System.nanoTime();
      final KijiResult<Integer> testResult =
          reader.getResult(table.getEntityId("foo"), singletonRequest);
      try {
        final Integer value2 = KijiResult.Helpers.getFirstValue(testResult);
        LOG.info("result single value time = {} nanoseconds",
            (double) (System.nanoTime() - resultStartTime) / 1000000);

        Assert.assertEquals(value, value2);
      } finally {
        testResult.close();
      }
    }
  }

  private void paged(
      final HBaseKijiTableReader reader,
      final EntityId eid
  ) throws IOException {
    final KijiDataRequest pagedRequest = KijiDataRequest.builder().addColumns(
        ColumnsDef.create().withPageSize(10).withMaxVersions(10000).add("map", null)).build();
    {
      final KijiRowData warmupRowData = reader.get(eid, pagedRequest);
      for (KijiCell<Integer> cell : warmupRowData.<Integer>asIterable("map")) {
        cell.getData();
      }
      final KijiResult<Object> warmupResult = reader.getResult(eid, pagedRequest);
      try {
        for (KijiCell<Object> cell : warmupResult) {
          cell.getData();
        }
      } finally {
        warmupResult.close();
      }
      {
        final long resultStartTime = System.nanoTime();
        final KijiResult<Integer> testResult = reader.getResult(eid, pagedRequest);
        try {
          final Iterator<KijiCell<Integer>> it = testResult.iterator();
          int seen = 0;
          while (it.hasNext()) {
            Integer v = it.next().getData();
            seen++;
          }
          LOG.info(
              "paged result all map family time ({} cells) = {} nanoseconds",
              seen, System.nanoTime() - resultStartTime);
        } finally {
          testResult.close();
        }
      }
    }
  }

  // Disabled by default.
  //@Test
  public void performanceTest() throws IOException {
    setupPerformanceTest();

    final KijiDataRequest request = KijiDataRequest.builder().addColumns(
        ColumnsDef.create().withMaxVersions(10000).add("map", null)).build();
    final HBaseKijiTable table =
        HBaseKijiTable.downcast(getKiji().openTable("row_data_test_table"));
    try {
      final HBaseKijiTableReader reader = (HBaseKijiTableReader) table.openTableReader();
      try {
        warmupPerformanceTest(table, reader, request);

        resultFromRowData(reader, table.getEntityId("foo"), request);

        allValuesMapFamily(table, reader, request);

        singleValue(table, reader);

        paged(reader, table.getEntityId("foo"));
      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }
}
