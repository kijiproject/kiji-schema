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

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;

/**
 * Command-line tool to get a row from a kiji table.
 *
 * In row 'bar', display 'info:email' and 'derived:domain' columns of table 'foo':
 * <pre>
 *   kiji get \
 *       --kiji=kiji://hbase-address/kiji-instance/table-name/ \
 *       --columns=info:email,derived:domain \
 *       --entity-id=bar
 * </pre>
 */
@ApiAudience.Private
public final class GetTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(GetTool.class);

  // TODO: make this a URI with columns, optionally.
  @Flag(name="kiji", usage="URI of the table to dump a row from")
  private String mURIFlag;


  // TODO: remove this flag and make use above URI to specify columns.
  @Flag(name="columns", usage="Comma-delimited columns (family:qualifier), or * for all columns")
  private String mColumns = "*";

  @Flag(name="entity-id", usage="ID of a single row to look up. "
      + "Either 'kiji=<Kiji row key>' or 'hbase=<HBase row key>'. "
      + ("HBase row keys are specified as bytes: "
          + "by default as UTF-8 strings, or prefixed as in 'utf8:encoded\\x0astring'; "
          + "in hexadecimal as in 'hex:deadbeef'; "
          + "as a URL with 'url:this%20URL%00'. ")
      + "Old deprecated Kiji row keys are specified as naked UTF-8 strings. "
      + ("New Kiji row keys are specified in JSON, "
          + "as in: --entity-id=kiji=\"['component1', 2, 'component3']\"."))
  private String mEntityIdFlag = null;

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
    return "get";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Get kiji table row.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Data";
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
    // TODO: Send this through an alternative stream: something like verbose or debug?
    getPrintStream().println(
        "Looking up entity: " + ToolUtils.formatEntityId(entityId)
        + " from kiji table: " + mURI);
    try {
      final KijiRowData row = reader.get(entityId, request);
      ToolUtils.printRow(row, mapTypeFamilies, groupTypeColumns, getPrintStream());
    } catch (IOException ioe) {
      LOG.error(ioe.getMessage());
      return FAILURE;
    }
    return SUCCESS;
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    if (null == mURIFlag) {
      // TODO: Send this error to a future getErrorStream()
      getPrintStream().printf("--kiji must be specified, got %s%n", mURIFlag);
      return FAILURE;
    }

    mURI = KijiURI.newBuilder(mURIFlag).build();
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
          if (mEntityIdFlag == null) {
            // TODO: Send this error to a future getErrorStream()
            getPrintStream().printf("Specify entity with --entity-id=eid%n");
            return FAILURE;
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
    System.exit(new KijiToolLauncher().run(new GetTool(), args));
  }
}
