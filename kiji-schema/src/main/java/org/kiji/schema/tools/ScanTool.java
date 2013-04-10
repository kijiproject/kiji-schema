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

package org.kiji.schema.tools;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.EntityId;
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
 * Scan through up to 10 rows, starting from the first row,
 * and print columns 'info:email' and 'derived:domain' of table 'table_foo':
 * <pre>
 *   kiji scan \
 *       kiji://.env/default/table_foo/info:email,derived:domain \
 *       --max-rows=10
 * </pre>
 *
 * Scan through table 'table_foo' form row start-row 0x50 (included) to limit-row 0xe0 (excluded):
 * <pre>
 *   kiji scan \
 *       kiji://.env/default/table_foo/info:email,derived:domain \
 *       --start-row=hex:50 \
 *       --limit-row=hex:e0
 * </pre>
 */
@ApiAudience.Private
public final class ScanTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(ScanTool.class);

  @Flag(name="start-row",
      usage="HBase row to start scanning at (inclusive).\n"
          + "\tEither 'kiji=<Kiji row key>' or 'hbase=<HBase row key>'.\n"
          + ("\tHBase row keys are specified as bytes:\n"
              + "\t\tby default as UTF-8 strings, or prefixed as in 'utf8:encoded\\x0astring';\n"
              + "\t\tin hexadecimal as in 'hex:deadbeef';\n"
              + "\t\tas a URL with 'url:this%20URL%00'.\n")
          + "\tOld deprecated Kiji row keys are specified as naked UTF-8 strings.\n"
          + ("\tNew Kiji row keys are specified in JSON, "
              + "as in: --start-row=kiji=\"['component1', 2, 'component3']\"."))
  private String mStartRowFlag = null;

  @Flag(name="limit-row",
      usage="HBase row to stop scanning at (exclusive).\n"
          + "\tEither 'kiji=<Kiji row key>' or 'hbase=<HBase row key>'.\n"
          + ("\tHBase row keys are specified as bytes:\n"
              + "\t\tby default as UTF-8 strings, or prefixed as in 'utf8:encoded\\x0astring';\n"
              + "\t\tin hexadecimal as in 'hex:deadbeef';\n"
              + "\t\tas a URL with 'url:this%20URL%00'.\n")
          + "\tOld deprecated Kiji row keys are specified as naked UTF-8 strings.\n"
          + ("\tNew Kiji row keys are specified in JSON, "
              + "as in: --limit-row=kiji=\"['component1', 2, 'component3']\"."))
  private String mLimitRowFlag = null;

  @Flag(name="max-rows", usage="Max number of rows to scan")
  private int mMaxRows = 0;

  @Flag(name="max-versions", usage="Max number of versions per cell to display")
  private int mMaxVersions = 1;

  @Flag(name="timestamp", usage="Min..Max timestamp interval to display,\n"
      + "\twhere Min and Max represent long-type time in milliseconds since the UNIX Epoch.\n"
      + "\tE.g. '--timestamp=123..1234', '--timestamp=0..', or '--timestamp=..1234'.")
  private String mTimestamp = "0..";

  /**
   * Lazy initialized timestamp intervals.
   */
  private long mMinTimestamp, mMaxTimestamp;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "scan";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Scan through a range of rows in a Kiji table.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Data";
  }

  /** {@inheritDoc} */
  @Override
  public String getUsageString() {
    return
        "Usage:\n"
        + "    kiji scan [flags...] (<table-uri> | <columns-uri>)\n"
        + "\n"
        + "Example:\n"
        + "    kiji scan --max-rows=2 kiji://.env/default/my_table/family:qualifier,map_family\n";
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
        if (hasVerboseDebug() && (!row.getEntityId().toShellString().startsWith("hbase="))) {
          getPrintStream().printf("entity-id=%s%s%n", ToolUtils.HBASE_ROW_KEY_SPEC_PREFIX,
              Bytes.toStringBinary((row.getEntityId().getHBaseRowKey())));
        }
        ToolUtils.printRow(row, mapTypeFamilies, groupTypeColumns, getPrintStream());
      }
    } catch (IOException ioe) {
      LOG.error(ioe.getMessage());
      return FAILURE;
    } finally {
      ResourceUtils.closeOrLog(scanner);
    }
    return SUCCESS;
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    if (nonFlagArgs.isEmpty()) {
      // TODO: Send this error to a future getErrorStream()
      getPrintStream().printf("URI must be specified as an argument%n");
      return FAILURE;
    } else if (nonFlagArgs.size() > 1) {
      getPrintStream().printf("Too many arguments: %s%n", nonFlagArgs);
      return FAILURE;
    }

    final KijiURI argURI = KijiURI.newBuilder(nonFlagArgs.get(0)).build();

    if (mMaxRows < 0) {
      // TODO: Send this error to a future getErrorStream()
      getPrintStream().printf("--max-rows must be nonnegative, got %d%n", mMaxRows);
      return FAILURE;
    }

    if (mMaxVersions < 1) {
      // TODO: Send this error to a future getErrorStream()
      getPrintStream().printf("--max-versions must be positive, got %d%n", mMaxVersions);
      return FAILURE;
    }

    if ((null == argURI.getZookeeperQuorum())
        || (null == argURI.getInstance())
        || (null == argURI.getTable())) {
      // TODO: Send this error to a future getErrorStream()
      getPrintStream().printf("Specify a cluster, instance, and "
          + "table with argument kiji://zkhost/instance/table%n");
      return FAILURE;
    }

    final Pattern timestampPattern = Pattern.compile("([0-9]*)\\.\\.([0-9]*)");
    final Matcher timestampMatcher = timestampPattern.matcher(mTimestamp);
    if (timestampMatcher.matches()) {
      mMinTimestamp = ("".equals(timestampMatcher.group(1))) ? 0
          : Long.parseLong(timestampMatcher.group(1));
      final String rightEndpoint = timestampMatcher.group(2);
      mMaxTimestamp = ("".equals(rightEndpoint)) ? Long.MAX_VALUE : Long.parseLong(rightEndpoint);
    } else {
      // TODO: Send this error to a future getErrorStream()
      getPrintStream().printf("--timestamp must be like [0-9]*..[0-9]*, instead got '%s'%n",
          mTimestamp);
      return FAILURE;
    }

    final Kiji kiji = Kiji.Factory.open(argURI, getConf());
    try {
      final KijiTable table = kiji.openTable(argURI.getTable());
      try {
        final KijiTableLayout tableLayout = table.getLayout();

        final Map<FamilyLayout, List<String>> mapTypeFamilies =
            ToolUtils.getMapTypeFamilies(argURI.getColumns(), tableLayout);

        final Map<FamilyLayout, List<ColumnLayout>> groupTypeColumns =
            ToolUtils.getGroupTypeColumns(argURI.getColumns(), tableLayout);

        final KijiDataRequest request = ToolUtils.getDataRequest(
            mapTypeFamilies, groupTypeColumns, mMaxVersions, mMinTimestamp, mMaxTimestamp);

        final KijiTableReader reader = table.openTableReader();
        try {
          // Scan from startRow to limitRow.
          final EntityId startRow = (mStartRowFlag != null)
              ? ToolUtils.createEntityIdFromUserInputs(mStartRowFlag, tableLayout)
              : null;
          final EntityId limitRow = (mLimitRowFlag != null)
              ? ToolUtils.createEntityIdFromUserInputs(mLimitRowFlag, tableLayout)
              : null;
          getPrintStream().println("Scanning kiji table: " + argURI);

          if (startRow != null && hasVerboseDebug()) {
            getPrintStream().printf("\tstart-row=%s%s%n", ToolUtils.HBASE_ROW_KEY_SPEC_PREFIX,
                Bytes.toStringBinary(startRow.getHBaseRowKey()));
          }
          if (limitRow != null && hasVerboseDebug()) {
            getPrintStream().printf("\tlimit-row=%s%s%n", ToolUtils.HBASE_ROW_KEY_SPEC_PREFIX,
                Bytes.toStringBinary(limitRow.getHBaseRowKey()));
          }

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
