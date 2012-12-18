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

package org.kiji.schema.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.HBaseScanOptions;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiCellDecoderFactory;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestValidator;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.filter.KijiRowFilter;
import org.kiji.schema.filter.KijiRowFilterApplicator;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Reads from a kiji table by sending the requests directly to the HBase tables.
 */
@ApiAudience.Private
public class HBaseKijiTableReader extends KijiTableReader {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseKijiTableReader.class);

  /** The kiji instance the table is in. */
  private final Kiji mKiji;
  /** The kiji table instance. */
  private final HBaseKijiTable mTable;

  /**
   * Creates a new <code>HBaseKijiTableReader</code> instance that sends the read requests
   * directly to HBase.
   *
   * @param table The kiji table to read from.
   */
  public HBaseKijiTableReader(KijiTable table) {
    super(table);
    mKiji = table.getKiji();
    mTable = HBaseKijiTable.downcast(table);
  }

  /**
   * Retrieve data from a single row in the Kiji table, with no post-processing.
   *
   * @param entityId The entity id for the row to get data from.
   * @param dataRequest Specifies the columns of data to retrieve.
   * @return The requested data. If there is no row for the specified entityId, this
   *     will return an empty KijiRowData. (containsColumn() will return false for all
   *     columns.)
   * @throws IOException If there is an IO error.
   */
  public KijiRowData get(EntityId entityId, KijiDataRequest dataRequest)
      throws IOException {

    // Make sure the request validates against the layout of the table.
    KijiTableLayout tableLayout = getTableLayout(dataRequest);

    // Construct an HBase Get to send to the HTable.
    HBaseDataRequestAdapter hbaseRequestAdapter = new HBaseDataRequestAdapter(dataRequest);
    Get hbaseGet;
    try {
      hbaseGet = hbaseRequestAdapter.toGet(entityId, tableLayout);
    } catch (InvalidLayoutException e) {
      // The table layout should never be invalid at this point, since we got it from a valid
      // opened table.  If it is, there's something seriously wrong.
      throw new InternalKijiError(e);
    }
    // Send the HTable Get.
    LOG.debug("Sending HBase Get: " + hbaseGet);
    Result result = mTable.getHTable().get(hbaseGet);
    // Parse the result.
    HBaseKijiRowData rowData = new HBaseKijiRowData(new HBaseKijiRowData.Options()
        .withEntityId(entityId)
        .withHBaseResult(result)
        .withDataRequest(dataRequest)
        .withTableLayout(tableLayout)
        .withCellDecoderFactory(getKijiCellDecoderFactory())
        .withHTable(mTable.getHTable()));

    return rowData;


  }


  /** {@inheritDoc} */
  @Override
  public List<KijiRowData> bulkGet(List<EntityId> entityIds, KijiDataRequest dataRequest)
      throws IOException {
    // Bulk gets have some overhead associated with them,
    // so delegate work to get(EntityId, KijiDataRequest) if possible.
    if (entityIds.size() == 1) {
      return Collections.singletonList(this.get(entityIds.get(0), dataRequest));
    }

    KijiTableLayout tableLayout = getTableLayout(dataRequest);
    HBaseDataRequestAdapter hbaseRequestAdapter = new HBaseDataRequestAdapter(dataRequest);

    // Construct a list of hbase Gets to send to the HTable.
    List<Get> hbaseGetList = makeGetList(entityIds, tableLayout, hbaseRequestAdapter);

    // Send the HTable Gets.
    Result[] results = mTable.getHTable().get(hbaseGetList);
    assert entityIds.size() == results.length;

    // Parse the results.  If a Result is null, then the corresponding KijiRowData should also
    // be null.  This indicates that there was an error retrieving this row.
    List<KijiRowData> rowDataList = parseResults(results, entityIds, dataRequest, tableLayout);

    return rowDataList;
  }

  /**
   * Parses an array of hbase Results, returned from a bulk get, to a List of
   * KijiRowData.
   *
   * @param results The results to parse.
   * @param entityIds The matching set of EntityIds.
   * @param dataRequest The KijiDataRequest.
   * @param tableLayout The table layout.
   * @return The list of KijiRowData returned by these results.
   * @throws IOException If there is an error.
   */
  private List<KijiRowData> parseResults(Result[] results, List<EntityId> entityIds,
      KijiDataRequest dataRequest, KijiTableLayout tableLayout) throws IOException {
    List<KijiRowData> rowDataList = new ArrayList<KijiRowData>(results.length);
    KijiCellDecoderFactory cellDecoderFactory = getKijiCellDecoderFactory();

    for (int i = 0; i < results.length; i++) {
      Result result = results[i];
      EntityId entityId = entityIds.get(i);

      HBaseKijiRowData rowData;
      if (null == result) {
        rowData = null;
      } else {
         rowData =  new HBaseKijiRowData(new HBaseKijiRowData.Options()
              .withEntityId(entityId)
              .withHBaseResult(result)
              .withDataRequest(dataRequest)
              .withTableLayout(tableLayout)
              .withCellDecoderFactory(cellDecoderFactory)
              .withHTable(mTable.getHTable()));
      }
      rowDataList.add(rowData);
    }
    return rowDataList;
  }

  /**
   * Creates a list of hbase Gets for a set of entityIds.
   *
   * @param entityIds The set of entityIds to collect.
   * @param tableLayout The table layout specifying constraints on what data to return for a row.
   * @param hbaseRequestAdapter The HBaseDataRequestAdapter.
   * @return A list of hbase Gets-- one for each entity id.
   * @throws IOException If there is an error.
   */
  private static List<Get> makeGetList(List<EntityId> entityIds, KijiTableLayout tableLayout,
      HBaseDataRequestAdapter hbaseRequestAdapter)
      throws IOException {
    List<Get> hbaseGetList = new ArrayList<Get>(entityIds.size());
    try {
      for (EntityId entityId : entityIds) {
        hbaseGetList.add(hbaseRequestAdapter.toGet(entityId, tableLayout));
      }
      return hbaseGetList;
    } catch (InvalidLayoutException e) {
      // The table layout should never be invalid at this point, since we got it from a valid
      // opened table.  If it is, there's something seriously wrong.
      throw new InternalKijiError(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowScanner getScanner(
      KijiDataRequest dataRequest, EntityId startRow, EntityId stopRow,
      KijiRowFilter rowFilter, HBaseScanOptions scanOptions)
      throws IOException {
    try {
      HBaseDataRequestAdapter dataRequestAdapter = new HBaseDataRequestAdapter(dataRequest);
      KijiTableLayout tableLayout = getTableLayout(dataRequest);
      Scan scan = dataRequestAdapter.toScan(tableLayout, scanOptions);

      if (null != startRow) {
        scan.setStartRow(startRow.getHBaseRowKey());
      }
      if (null != stopRow) {
        scan.setStopRow(stopRow.getHBaseRowKey());
      }

      if (null != rowFilter) {
        KijiRowFilterApplicator applicator = new KijiRowFilterApplicator(rowFilter,
          mKiji.getSchemaTable(), tableLayout);
        applicator.applyTo(scan);
      }

      return new HBaseKijiRowScanner(new HBaseKijiRowScanner.Options()
          .withHBaseResultScanner(mTable.getHTable().getScanner(scan))
          .withDataRequest(dataRequest)
          .withTable(mTable)
          .withCellDecoderFactory(getKijiCellDecoderFactory()));
    } catch (InvalidLayoutException e) {
      // The table layout should never be invalid at this point, since we got it from a valid
      // opened table.  If it is, there's something seriously wrong.
      throw new InternalKijiError(e);
    }
  }

  /**
   * Helper method to retrieve the KijiTableLayout.
   *
   * @param dataRequest A KijiDataRequest.
   * @return The table layout adapter.
   */
  private KijiTableLayout getTableLayout(KijiDataRequest dataRequest) {
    KijiDataRequestValidator requestValidator = new KijiDataRequestValidator(dataRequest);
    KijiTableLayout tableLayout = mTable.getLayout();
    try {
      requestValidator.validate(tableLayout);
    } catch (InvalidLayoutException e) {
      // The table layout should never be invalid at this point, since we got it from a valid
      // opened table.  If it is, there's something seriously wrong.
      throw new InternalKijiError(e);
    }
    return tableLayout;
  }

}
