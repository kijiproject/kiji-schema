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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.util.ResourceUtils;

/**
 * Command-line tool to explore kiji table data like the 'scan' command of a unix shell.
 *
 * List all data in the columns 'info:email' and 'derived:domain' of a table 'foo' up to max-rows:
 * <pre>
 *   kiji scan \
 *       --kiji=kiji://hbase-address/kiji-instance/table-name/ \
 *       --columns=info:email,derived:domain \
 *       --max-rows=10
 * </pre>
 *
 * List all data in table 'foo' form row start-row to limit-row:
 * <pre>
 *   kiji scan \
 *       --kiji=kiji://hbase-address/kiji-instance/table-name/ \
 *       --columns=info:email,derived:domain \
 *       --start-row=hex:50000000000000000000000000000000 \
 *       --limit-row=hex:e0000000000000000000000000000000
 * </pre>
 */
@ApiAudience.Private
public final class ScanTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(ScanTool.class);

  // TODO: make this a URI with columns, optionally.
  @Flag(name="kiji", usage="URI of the object to list contents from.")
  private String mURIFlag;

  // TODO: remove this flag and make use above URI to specify columns.
  @Flag(name="columns", usage="Comma-delimited columns (family:qualifier), or * for all columns")
  private String mColumns = "*";

  @Flag(name="start-row",
      usage="HBase row to start scanning at (inclusive), "
            + "e.g. --start-row='hex:0088deadbeef', or --start-row='utf8:the row key in UTF8'.")
  private String mStartRowFlag = null;

  @Flag(name="limit-row",
      usage="HBase row to stop scanning at (exclusive), "
            + "e.g. --limit-row='hex:0088deadbeef', or --limit-row='utf8:the row key in UTF8'.")
  private String mLimitRowFlag = null;

  @Flag(name="max-rows", usage="Max number of rows to scan")
  private int mMaxRows = 0;

  @Flag(name="max-versions", usage="Max number of versions per cell to display")
  private int mMaxVersions = 1;

  @Flag(name="min-timestamp", usage="Min timestamp of versions to display")
  private long mMinTimestamp = 0;

  @Flag(name="max-timestamp", usage="Max timestamp of versions to display")
  private long mMaxTimestamp = Long.MAX_VALUE;

  /** URI of the element being inspected. */
  private KijiURI mURI = null;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "scan";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "List rows.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Data";
  }


  /**
   * Scans a table, displaying the data in the given columns, or all data if columns is null.
   *
   * @param reader The reader.
   * @param request The data request.
   * @param startRow The first row to include in this scan.
   * @param limitRow The last row to include in this scan.
   * @param mapTypeFamilies The map type families to print.
   * @param groupTypeColumns The group type columns to print.
   * @return A program exit code (zero on success).
   * @throws IOException If there is an IO error.
   */
  private int scan(
      KijiTableReader reader,
      KijiDataRequest request,
      EntityId startRow,
      EntityId limitRow,
      Map<FamilyLayout, List<String>> mapTypeFamilies,
      Map<FamilyLayout, List<ColumnLayout>> groupTypeColumns)
      throws IOException {
    getPrintStream().println("Scanning kiji table: " + mURI);
    final KijiScannerOptions scannerOptions =
        new KijiScannerOptions()
            .setStartRow(startRow)
            .setStopRow(limitRow);
    final KijiRowScanner scanner = reader.getScanner(request, scannerOptions);
    try {
      int rowsOutput = 0;
      for (KijiRowData row : scanner) {
        if ((mMaxRows != 0) && (++rowsOutput > mMaxRows)) {
          break;
        }
        ToolUtils.printRow(row, mapTypeFamilies, groupTypeColumns, getPrintStream());
      }
    } finally {
      ResourceUtils.closeOrLog(scanner);
    }
    return SUCCESS;
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {

    if (mMaxRows < 0) {
      // TODO: Send this error to a future getErrorStream()
      getPrintStream().printf("--max-rows must be positive, got %d%n", mMaxRows);
      return FAILURE;
    }
    if (null == mURIFlag) {
      // TODO: Send this error to a future getErrorStream()
      getPrintStream().printf("--kiji must be specified, got %s%n", mURIFlag);
      return FAILURE;
    }

    mURI = KijiURI.newBuilder(mURIFlag).build();
    if ((null == mURI.getZookeeperQuorum())
        || (null == mURI.getInstance())
        || (null == mURI.getTable())) {
      // TODO: Send this error to a future getErrorStream()

      getPrintStream().printf("Specify a cluster, instance, and "
          + "table with --kiji=kiji://zkhost/instance/table%n");
      return FAILURE;
    }

    final Kiji kiji = Kiji.Factory.open(mURI);
    try {
      final KijiTable table = kiji.openTable(mURI.getTable());
      try {
        final KijiTableLayout tableLayout = table.getLayout();
        final String[] rawColumnNames =
            (mColumns.equals("*")) ? null : StringUtils.split(mColumns, ",");

        final Map<FamilyLayout, List<String>> mapTypeFamilies =
            ToolUtils.getMapTypeFamilies(rawColumnNames, tableLayout);

        final Map<FamilyLayout, List<ColumnLayout>> groupTypeColumns =
            ToolUtils.getGroupTypeColumns(rawColumnNames, tableLayout);

        final KijiDataRequest request = ToolUtils.getDataRequest(
            mapTypeFamilies, groupTypeColumns, mMaxVersions, mMinTimestamp, mMaxTimestamp);

        final KijiTableReader reader = table.openTableReader();
        try {
          final EntityIdFactory eidFactory = EntityIdFactory.getFactory(table.getLayout());
          // Scan from startRow to limitRow.
          final EntityId startRow = (mStartRowFlag != null)
              ? eidFactory.getEntityIdFromHBaseRowKey(ToolUtils.parseBytesFlag(mStartRowFlag))
              : null;
          final EntityId limitRow = (mLimitRowFlag != null)
              ? eidFactory.getEntityIdFromHBaseRowKey(ToolUtils.parseBytesFlag(mLimitRowFlag))
              : null;
          return scan(reader, request, startRow, limitRow, mapTypeFamilies, groupTypeColumns);
        } finally {
          ResourceUtils.closeOrLog(reader);
        }
      } finally {
        ResourceUtils.releaseOrLog(table);
      }
    } finally {
      ResourceUtils.releaseOrLog(kiji);
    }
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new ScanTool(), args));
  }
}
