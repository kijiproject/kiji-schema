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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.hbase.HBaseFactory;
import org.kiji.schema.impl.FormattedEntityId;
import org.kiji.schema.impl.HBaseEntityId;
import org.kiji.schema.impl.HashPrefixedEntityId;
import org.kiji.schema.impl.HashedEntityId;
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
    // TODO(SCHEMA-188): Move this logic in KijiInstaller
    final Configuration conf = HBaseConfiguration.create();
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
        printRow(row, mapTypeFamilies, groupTypeColumns);
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
      printRow(row, mapTypeFamilies, groupTypeColumns);
    } catch (IOException ioe) {
      LOG.error(ioe.getMessage());
      return 1;
    }
    return 0;
  }

  /**
   * Prints cell data from the <code>row</code> for each column specified on the
   * <code>request</code>.
   *
   * @param row The row to read from.
   * @param mapTypeFamilies The map type families to print.
   * @param groupTypeColumns The group type columns to print.
   * @throws IOException if there is an error retrieving data from the KijiRowData.
   */
  private void printRow(
      KijiRowData row,
      Map<FamilyLayout, List<String>> mapTypeFamilies,
      Map<FamilyLayout, List<ColumnLayout>> groupTypeColumns)
      throws IOException {

    // Unpack and print result for the map type families.
    for (Entry<FamilyLayout, List<String>> entry : mapTypeFamilies.entrySet()) {
      final FamilyLayout family = entry.getKey();
      if (family.getDesc().getMapSchema().getType() == SchemaType.COUNTER) {

        // If this map family of counters has no qualifiers, print entire family.
        if (entry.getValue().isEmpty()) {
          for (String key : row.getQualifiers(family.getName())) {
            KijiCell<Long> counter = row.getMostRecentCell(family.getName(), key);
            if (null != counter) {
              printCell(row.getEntityId(), counter);
            }
          }
        // If this map family of counters has been qualified, print only the given columns.
        } else {
          for (String key : entry.getValue()) {
            KijiCell<Long> counter = row.getMostRecentCell(family.getName(), key);
            if (null != counter) {
              printCell(row.getEntityId(), counter);
            }
          }
        }
      } else {
        // If this map family of non-counters has no qualifiers, print entire family.
        if (entry.getValue().isEmpty()) {
          NavigableMap<String, NavigableMap<Long, Object>> keyTimeseriesMap =
              row.getValues(family.getName());
          for (String key : keyTimeseriesMap.keySet()) {
            for (Entry<Long, Object> timestampedCell : keyTimeseriesMap.get(key).entrySet()) {
              long timestamp = timestampedCell.getKey();
              printCell(row.getEntityId(), timestamp, family.getName(), key,
                  timestampedCell.getValue());
            }
          }
        // If this map family of non-counters has been qualified, print only the given columns.
        } else {
          for (String key : entry.getValue()) {
            NavigableMap<Long, Object> timeseriesMap =
                row.getValues(family.getName(), key);
            for (Entry<Long, Object> timestampedCell : timeseriesMap.entrySet()) {
              long timestamp = timestampedCell.getKey();
              printCell(
                  row.getEntityId(), timestamp, family.getName(), key, timestampedCell.getValue());
            }
          }
        }
      }
    }

    // Unpack and print result for the group type families.
    for (Entry<FamilyLayout, List<ColumnLayout>> entry : groupTypeColumns.entrySet()) {
      String familyName = entry.getKey().getName();
      for (ColumnLayout column : entry.getValue()) {
        final KijiColumnName colName = new KijiColumnName(familyName, column.getName());
        if (column.getDesc().getColumnSchema().getType() == SchemaType.COUNTER) {
          final KijiCell<Long> counter =
              row.getMostRecentCell(colName.getFamily(), colName.getQualifier());
          if (null != counter) {
            printCell(row.getEntityId(), counter);
          }
        } else {
          for (Entry<Long, Object> timestampedCell
              : row.getValues(colName.getFamily(), colName.getQualifier())
                  .entrySet()) {
            long timestamp = timestampedCell.getKey();
            printCell(row.getEntityId(), timestamp, colName.getFamily(),
                colName.getQualifier(), timestampedCell.getValue());
          }
        }
      }
    }
    getPrintStream().println("");
  }

  /**
   * Prints the contents of a single kiji cell to the printstream.
   *
   * @param entityId The entity id.
   * @param timestamp This timestamp of a KijiCell.
   * @param family The family of a KijiCell.
   * @param qualifier The qualifier of a KijiCell.
   * @param cellData The contents of a KijiCell.
   */
  private void printCell(EntityId entityId, Long timestamp,
      String family, String qualifier, Object cellData) {
    getPrintStream().printf("entity-id=%s [%d] %s:%s%n                                 %s%n",
        formatEntityId(entityId),
        timestamp,
        family,
        qualifier,
        cellData);
  }

  /**
   * Prints the contents of a single kiji cell to the printstream.
   *
   * @param entityId The entity id.
   * @param cell The KijiCell.
   */
  private void printCell(EntityId entityId, KijiCell<?> cell) {
    getPrintStream().printf("entity-id=%s [%d] %s:%s%n                                 %s%n",
        formatEntityId(entityId),
        cell.getTimestamp(),
        cell.getFamily(),
        cell.getQualifier(),
        cell.getData());
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
            getMapTypeFamilies(rawColumnNames, tableLayout);

        final Map<FamilyLayout, List<ColumnLayout>> groupTypeColumns =
            getGroupTypeColumns(rawColumnNames, tableLayout);

        final KijiDataRequest request = getDataRequest(
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
   * Returns the list of map-type families specified by <code>rawColumns</code>.
   * If <code>rawColumns</code> is null, then all map-type families are returned.
   *
   * @param rawColumns The raw columns supplied by the user.
   * @param layout The KijiTableLayout.
   * @return A list of map type families specified by the raw columns.
   */
  private Map<FamilyLayout, List<String>> getMapTypeFamilies(
      String[] rawColumns,
      KijiTableLayout layout) {
    final Map<FamilyLayout, List<String>> familyMap = Maps.newHashMap();
    if (null == rawColumns) {
      for (FamilyLayout family : layout.getFamilies()) {
        if (family.isMapType()) {
          familyMap.put(family, new ArrayList<String>());
        }
      }
    } else {
      for (String rawColumn : rawColumns) {
        final KijiColumnName colName = new KijiColumnName(rawColumn);
        final FamilyLayout family = layout.getFamilyMap().get(colName.getFamily());
        if (null == family) {
          throw new RuntimeException(String.format(
              "No family '%s' in table '%s'.", colName.getFamily(), layout.getName()));
        }
        if (family.isMapType()) {
          addColumn(family, colName.getQualifier(), familyMap);
        }
      }
    }
    return familyMap;
  }

  /**
   * Returns the list of group-type columns specified by <code>rawColumns</code>.
   * If <code>rawColumns</code> is null, then all columns in all group-type families are returned.
   * If a raw column specifies a group-type family, but no qualifier, then each column in that
   * family is returned.
   *
   * @param rawColumns The raw columns supplied by the user.
   * @param layout The KijiTableLayout.
   * @return The fully qualified columns specified by the raw columns.
   */
  private Map<FamilyLayout, List<ColumnLayout>> getGroupTypeColumns(
      String[] rawColumns,
      KijiTableLayout layout) {
    final Map<FamilyLayout, List<ColumnLayout>> familyMap = Maps.newHashMap();
    if (null == rawColumns) {
      for (FamilyLayout family : layout.getFamilies()) {
        if (family.isGroupType()) {
          familyMap.put(family, Lists.newArrayList(family.getColumns()));
        }
      }
    } else {
      for (String rawColumn : rawColumns) {
        final KijiColumnName colName = new KijiColumnName(rawColumn);
        final FamilyLayout family = layout.getFamilyMap().get(colName.getFamily());
        if (null == family) {
          throw new RuntimeException(String.format(
              "No family '%s' in table '%s'.", colName.getFamily(), layout.getName()));
        }
        if (family.isGroupType()) {
          // We'll include it.  Is it fully qualified?
          if (!colName.isFullyQualified()) {
            // User specified a group-type family, but no qualifier.  Include all qualifiers.
            for (ColumnLayout column : family.getColumns()) {
              addColumn(family, column, familyMap);
            }
          } else {
            final ColumnLayout column = family.getColumnMap().get(colName.getQualifier());
            if (null == column) {
              throw new RuntimeException(String.format(
                  "No column '%s' in table '%s'.", colName, layout.getName()));
            }
            addColumn(family, column, familyMap);
          }
        }
      }
    }
    return familyMap;
  }

  /**
   * Adds a column to the list of columns mapped from <code>family</code>.
   *
   * @param family The family as a key.
   * @param column The column to add to the list of columns for this family.
   * @param familyColumnMap The map between families and lists of columns.
   */
  private static void addColumn(
      FamilyLayout family,
      ColumnLayout column,
      Map<FamilyLayout, List<ColumnLayout>> familyColumnMap) {
    if (!familyColumnMap.containsKey(family)) {
      familyColumnMap.put(family, new ArrayList<ColumnLayout>());
    }
    familyColumnMap.get(family).add(column);
  }

  /**
   * Adds a column to the list of columns mapped from <code>family</code>.
   *
   * @param mapFamily The map family as a key.
   * @param qualifier The qualifier to add to the list of qualifiers for this map family.
   * @param familyQualifierMap The map between map families and lists of qualifiers.
   */
  private static void addColumn(
      FamilyLayout mapFamily,
      String qualifier,
      Map<FamilyLayout, List<String>> familyQualifierMap) {
    if (!familyQualifierMap.containsKey(mapFamily)) {
      familyQualifierMap.put(mapFamily, new ArrayList<String>());
    }
    if (null != qualifier) {
      familyQualifierMap.get(mapFamily).add(qualifier);
    }
  }

  /**
   * Returns a KijiDataRequest for the specified columns.  If columns is null,
   * returns a request for all columns.
   *
   * @param mapTypeFamilies The list of map type families to include.
   * @param groupTypeColumns The family:qualifier map of group type columns to include.
   * @param maxVersions The max versions to include.
   * @param minTimestamp The min timestamp.
   * @param maxTimestamp The max timestamp.
   * @return The KijiDataRequest.
   */
  private static KijiDataRequest getDataRequest(
      Map<FamilyLayout, List<String>> mapTypeFamilies,
      Map<FamilyLayout, List<ColumnLayout>> groupTypeColumns,
      int maxVersions,
      long minTimestamp,
      long maxTimestamp) {
    final KijiDataRequestBuilder builder = KijiDataRequest.builder()
        .withTimeRange(minTimestamp, maxTimestamp);

    final KijiDataRequestBuilder.ColumnsDef colBuilder =
        builder.addColumns().withMaxVersions(maxVersions);

    for (Entry<FamilyLayout, List<String>> entry : mapTypeFamilies.entrySet()) {
      String familyName = entry.getKey().getName();
      // If the map family is without qualifiers, add entire family.
      if (entry.getValue().isEmpty()) {
        LOG.debug("Adding family to data request: " + familyName);
        colBuilder.addFamily(familyName);
      } else {
        // If the map family is with qualifiers, add only the columns of interest.
        for (String qualifier : entry.getValue()) {
          LOG.debug("Adding column to data request: " + familyName + ":" + qualifier);
          colBuilder.add(familyName, qualifier);
        }
      }
    }

    for (Entry<FamilyLayout, List<ColumnLayout>> entry : groupTypeColumns.entrySet()) {
      String familyName = entry.getKey().getName();
      for (ColumnLayout column : entry.getValue()) {
        LOG.debug("Adding column to data request: " + column.getName());
        colBuilder.add(familyName, column.getName());
      }
    }
    return builder.build();
  }

  /**
   * Formats an entity ID for a command-line user.
   *
   * @param eid Entity ID to format.
   * @return the formatted entity ID as a String to print on the console.
   */
  private String formatEntityId(EntityId eid) {
    final String formattedHBaseRowKey =
        String.format("hbase='%s'", Bytes.toStringBinary(eid.getHBaseRowKey()));

    if (eid instanceof FormattedEntityId) {
      final FormattedEntityId feid = (FormattedEntityId) eid;
      final List<String> components = Lists.newArrayList();
      for (Object component: feid.getComponents()) {
        if (component instanceof Number) {
          components.add(component.toString());
        } else {
          if (component == null) {
            components.add("null");
          } else {
            components.add(String.format("'%s'", component));
          }
        }
      }
      return String.format("\"[%s]\"", Joiner.on(",").join(components));

    } else if (eid instanceof HashedEntityId) {
      final HashedEntityId hashed = (HashedEntityId) eid;
      final byte[] kijiRowKey = hashed.getKijiRowKey();
      if (kijiRowKey != null) {
        return String.format("'%s'", Bytes.toString(kijiRowKey));
      }

    } else if (eid instanceof HBaseEntityId) {
      return formattedHBaseRowKey;

    } else if (eid instanceof HashPrefixedEntityId) {
      return String.format("'%s'",
          Bytes.toString(((HashPrefixedEntityId) eid).getKijiRowKey()));
    }
    return formattedHBaseRowKey;
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
