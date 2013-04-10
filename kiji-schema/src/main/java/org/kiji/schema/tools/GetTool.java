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
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;

/**
 * Command-line tool to get one specific row from a kiji table.
 *
 * Dumps the entity with ID 'bar' from table table 'table_foo',
 * displaying columns 'info:email' and 'derived:domain':
 * <pre>
 *   kiji get kiji://.env/default/table_foo/info:email,derived:domain \
 *       --entity-id=bar
 * </pre>
 */
@ApiAudience.Private
public final class GetTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(GetTool.class);

  @Flag(name="entity-id", usage="ID of a single row to look up.\n"
      + "\tEither 'kiji=<Kiji row key>' or 'hbase=<HBase row key>'.\n"
      + ("\tHBase row keys are specified as bytes:\n"
          + "\t\tby default as UTF-8 strings, or prefixed as in 'utf8:encoded\\x0astring';\n"
          + "\t\tin hexadecimal as in 'hex:deadbeef';\n"
          + "\t\tas a URL with 'url:this%20URL%00'.\n")
      + "\tOld deprecated Kiji row keys are specified as naked UTF-8 strings.\n"
      + ("\tNew Kiji row keys are specified in JSON, "
          + "as in: --entity-id=kiji=\"['component1', 2, 'component3']\"."))
  private String mEntityIdFlag = null;

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
    return "get";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Dump one specific row from a Kiji table.";
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
        + "    kiji get [flags...] (<table-uri> | <columns-uri>)\n"
        + "\n"
        + "Example:\n"
        + "    kiji get kiji://.env/default/my_table \\\n"
        + "        --entity-id=\"the row ID\"\n"
        + "        --max-versions=2\n";
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
    try {
      final KijiRowData row = reader.get(entityId, request);
      if (hasVerboseDebug()
          && (!ToolUtils.formatEntityId(entityId).startsWith("hbase="))) {
        getPrintStream().printf("entity-id=%s%s%n", ToolUtils.HBASE_ROW_KEY_SPEC_PREFIX,
            Bytes.toStringBinary((entityId.getHBaseRowKey())));
      }
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
    if (nonFlagArgs.isEmpty()) {
      // TODO: Send this error to a future getErrorStream()
      getPrintStream().printf("URI must be specified as an argument%n");
      return FAILURE;
    } else if (nonFlagArgs.size() > 1) {
      getPrintStream().printf("Too many arguments: %s%n", nonFlagArgs);
      return FAILURE;
    }

    final KijiURI argURI = KijiURI.newBuilder(nonFlagArgs.get(0)).build();

    if ((null == argURI.getZookeeperQuorum())
        || (null == argURI.getInstance())
        || (null == argURI.getTable())) {
      // TODO: Send this error to a future getErrorStream()
      getPrintStream().printf("Specify a cluster, instance, and "
          + "table with argument kiji://zkhost/instance/table%n");
      return FAILURE;
    }

    if (mMaxVersions < 1) {
      // TODO: Send this error to a future getErrorStream()
      getPrintStream().printf("--max-versions must be positive, got %d%n", mMaxVersions);
      return FAILURE;
    }

    if (mEntityIdFlag == null) {
      // TODO: Send this error to a future getErrorStream()
      getPrintStream().printf("Specify entity with --entity-id=eid%n");
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
      getPrintStream().printf("--timestamp must be like [0-9]*..[0-9]*, instead got %s%n",
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
            // Return the specified entity.
            final EntityId entityId =
                ToolUtils.createEntityIdFromUserInputs(mEntityIdFlag, tableLayout);
            // TODO: Send this through an alternative stream: something like verbose or debug?
            getPrintStream().println(
                "Looking up entity: " + ToolUtils.formatEntityId(entityId)
                + " from kiji table: " + argURI);
            return lookup(reader, request, entityId, mapTypeFamilies, groupTypeColumns);
        } finally {
          reader.close();
        }
      } finally {
        table.release();
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
