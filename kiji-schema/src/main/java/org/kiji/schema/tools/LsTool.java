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
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.KConstants;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.KijiURI;
import org.kiji.schema.hbase.HBaseFactory;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.util.ResourceUtils;

/**
 * Command-line tool to explore kiji table data like the 'ls' command of a unix shell.
 *
 * List all kiji instances:
 * <pre>
 *   kiji ls --kiji=kiji://hbase-address/
 * </pre>
 *
 * List all kiji tables:
 * <pre>
 *   kiji ls --kiji=kiji://hbase-address/kiji-instance/
 * </pre>
 *
 * List all families in a kiji table foo:
 * <pre>
 *   kiji ls --kiji=kiji://hbase-address/kiji-instance/table-name/
 * </pre>
 *
 * List all data in the info:email and derived:domain columns of a table foo:
 * <pre>
 *   kiji ls \
 *       --kiji=kiji://hbase-address/kiji-instance/table-name/ \
 *       --columns=info:email,derived:domain
 * </pre>
 *
 * List all data in the info:email and derived:domain columns of a table foo in row bar:
 * <pre>
 *   kiji ls \
 *       --kiji=kiji://hbase-address/kiji-instance/table-name/ \
 *       --columns=info:email,derived:domain \
 *       --entity-id=bar
 * </pre>
 */
@ApiAudience.Private
public final class LsTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(LsTool.class);

  @Flag(name="kiji", usage="KijiURI of the object to list contents from.")
  private String mURIFlag = KConstants.DEFAULT_URI;

  @Flag(name="columns", usage="Comma-delimited columns (family:qualifier), or * for all columns")
  private String mColumns = "*";

  @Flag(name="entity-id", usage="ID of a single row to look up. "
      + "Either 'kiji=<Kiji row key>' or 'hbase=<HBase row key>'. "
      + ("HBase row keys are specified as bytes: "
          + "by default as UTF-8 strings, or prefixed as in 'utf8:encoded\\x0astring'; "
          + "in hexadecimal as in 'hex:deadbeed'; "
          + "as a URL with 'url:this%20URL%00'. ")
      + "Old deprecated Kiji row keys are specified as naked UTF-8 strings. "
      + ("New Kiji row keys are specified in JSON, "
          + "as in: --entity-id=kiji=\"['component1', 2, 'component3']\"."))
  private String mEntityIdFlag = null;

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
    return "ls";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "List kiji instances, tables and rows.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Data";
  }

  /**
   * Lists all kiji instances.
   *
   * @param hbaseURI URI of the HBase instance to list the content of.
   * @return A program exit code (zero on success).
   * @throws IOException If there is an error.
   */
  private int listInstances(KijiURI hbaseURI) throws IOException {
    for (String instanceName : getInstanceNames(hbaseURI)) {
      getPrintStream().println(KijiURI.newBuilder(hbaseURI).withInstanceName(instanceName).build());
    }
    return SUCCESS;
  }

  /**
   * Returns a set of instance names.
   *
   * @param hbaseURI URI of the HBase instance to list the content of.
   * @return ordered set of instance names.
   * @throws IOException on I/O error.
   */
  protected static Set<String> getInstanceNames(KijiURI hbaseURI) throws IOException {
    // TODO(SCHEMA-188): Consolidate this logic in a single central place:
    final Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.ZOOKEEPER_QUORUM,
        Joiner.on(",").join(hbaseURI.getZookeeperQuorumOrdered()));
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, hbaseURI.getZookeeperClientPort());
    final HBaseAdmin hbaseAdmin =
        HBaseFactory.Provider.get().getHBaseAdminFactory(hbaseURI).create(conf);

    try {
      final Set<String> instanceNames = Sets.newTreeSet();
      for (HTableDescriptor hTableDescriptor : hbaseAdmin.listTables()) {
        final String instanceName = parseInstanceName(hTableDescriptor.getNameAsString());
        if (null != instanceName) {
          instanceNames.add(instanceName);
        }
      }
      return instanceNames;
    } finally {
      ResourceUtils.closeOrLog(hbaseAdmin);
    }
  }

  /**
   * Parses a table name for a kiji instance name.
   *
   * @param kijiTableName The table name to parse
   * @return instance name (or null if none found)
   */
  protected static String parseInstanceName(String kijiTableName) {
    String[] parts = org.apache.hadoop.util.StringUtils.split(kijiTableName, '\u0000', '.');
    if (parts.length < 3 || !KijiURI.KIJI_SCHEME.equals(parts[0])) {
      return null;
    }
    return parts[1];
  }

  /**
   * Lists all the tables in a kiji instance.
   *
   * @param kiji Kiji instance to list the tables of.
   * @return A program exit code (zero on success).
   * @throws IOException If there is an error.
   */
  private int listTables(Kiji kiji) throws IOException {
    getPrintStream().println("Listing tables in kiji instance: " + mURI);
    for (String name : kiji.getTableNames()) {
      getPrintStream().println(name);
    }
    return 0;
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
    KijiScannerOptions scannerOptions =
        new KijiScannerOptions()
        .setStartRow(startRow)
        .setStopRow(limitRow);
    KijiRowScanner scanner = reader.getScanner(request, scannerOptions);
    try {
      int rowsOutput = 0;
      for (KijiRowData row : scanner) {
        if (mMaxRows != 0 && ++rowsOutput > mMaxRows) {
          break;
        }
        ToolUtils.printRow(row, mapTypeFamilies, groupTypeColumns, getPrintStream());
      }
    } finally {
      scanner.close();
    }
    return 0;
  }

  /**
   * Prints the data for a single entity id.
   *
   * @param reader The reader.
   * @param request The data request.
   * @param entityId The entity id to lookup.
   * @param mapTypeFamilies The map type families to print.
   * @param groupTypeColumns The group type columns to print.
   * @return A program exit code (zero on success).
   */
  private int lookup(
      KijiTableReader reader,
      KijiDataRequest request,
      EntityId entityId,
      Map<FamilyLayout, List<String>> mapTypeFamilies,
      Map<FamilyLayout, List<ColumnLayout>> groupTypeColumns) {
    getPrintStream().println(
        "Looking up entity: " + Bytes.toStringBinary(entityId.getHBaseRowKey())
        + " from kiji table: " + mURI);
    try {
      final KijiRowData row = reader.get(entityId, request);
      ToolUtils.printRow(row, mapTypeFamilies, groupTypeColumns, getPrintStream());
    } catch (IOException ioe) {
      LOG.error(ioe.getMessage());
      return 1;
    }
    return 0;
  }

  @Override
  protected void validateFlags() throws Exception {
    if (mMaxRows < 0) {
      throw new RuntimeException("--max-rows must be positive");
    }
    if (mEntityIdFlag != null) {
      if (mStartRowFlag != null) {
        throw new RuntimeException("--start-row is only relevant when scanning");
      }
      if (mLimitRowFlag != null) {
        throw new RuntimeException("--limit-row is only relevant when scanning");
      }
      if (mMaxRows != 0) {
        throw new RuntimeException("--max-rows is only relevant when scanning");
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() throws Exception {
    mURI = KijiURI.newBuilder(mURIFlag).build();
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    if (mURI.getZookeeperQuorum() == null) {
      LOG.error("Specify an HBase cluster with --kiji=kiji://zookeeper-quorum");
      return 1;
    }

    if (mURI.getInstance() == null) {
      return listInstances(mURI);
    }

    final Kiji kiji = Kiji.Factory.open(mURI);
    try {
      if (mURI.getTable() == null) {
        // List tables in this kiji instance.
        return listTables(kiji);
      }

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
          if (mEntityIdFlag == null) {
            // Scan from startRow to limitRow.
            final EntityId startRow = (mStartRowFlag != null)
                ? eidFactory.getEntityIdFromHBaseRowKey(ToolUtils.parseBytesFlag(mStartRowFlag))
                : null;
            final EntityId limitRow = (mLimitRowFlag != null)
                ? eidFactory.getEntityIdFromHBaseRowKey(ToolUtils.parseBytesFlag(mLimitRowFlag))
                : null;
            return scan(reader, request, startRow, limitRow, mapTypeFamilies, groupTypeColumns);
          } else {
            // Return the specified entity.
            final EntityId entityId =
                ToolUtils.createEntityIdFromUserInputs(mEntityIdFlag, tableLayout);
            return lookup(reader, request, entityId, mapTypeFamilies, groupTypeColumns);
          }
        } finally {
          reader.close();
        }
      } finally {
        table.close();
      }
    } finally {
      kiji.release();
    }
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new LsTool(), args));
  }
}
