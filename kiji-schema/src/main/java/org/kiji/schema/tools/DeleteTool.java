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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.ResourceUtils;

/**
 * Command-line tool to delete kiji tables, rows, and cells.
 *
 * --kiji to specify the URI of the target(s) to delete.
 *
 * --entity-id to select a kiji row by unhashed ID.
 *  required if family or qualifier are specified in URI,
 *  and for timestamp flags to operate.
 *
 * --upto-timestamp to select values with timestamp equal or lesser.
 *
 * --exact-timestamp to select a value with a specific timestamp.
 *
 * --most-recent to select the most recent value in a cell.
 */
@ApiAudience.Private
public final class DeleteTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteTool.class);
  private static final long UNSPECIFIED_TIMESTAMP = Long.MIN_VALUE;

  @Flag(name="kiji", usage="The KijiURI of the element(s) to delete.\n"
      + "Can specify a table, family or families, column or columns")
  private String mKijiURIString;

  @Flag(name="entity-id", usage="The unhashed entity-id from which to delete."
      + " (requires a specified table in --kiji)")
  private String mUnhashedEntityId;

  @Flag(name="entity-hash", usage="The hashed entity-id from which to delete."
      + " (requires a specified table in --kiji)")
  private String mHashedEntityId;

  @Flag(name="upto-timestamp", usage="Delete all values with timestamp lower than or equal to "
      + "specified timestamp. (requires --entity-id or --entity-hash)")
  private long mUpToTimestamp = UNSPECIFIED_TIMESTAMP;

  @Flag(name="exact-timestamp", usage="Delete all values with the specified timestamp.\n"
      + "(requires --entity-id or --entity-hash and at least one family:column"
      + " specified in --kiji)")
  private long mExactTimestamp = UNSPECIFIED_TIMESTAMP;

  @Flag(name="most-recent", usage="If true, delete the most recent value in the specified column(s)"
      + " and entity-id. (requires --entity-id or entity-hash and at least one family:column"
      + " specified in --kiji)")
  private boolean mMostRecent = false;

  /** Kiji table from which to delete cells. */
  private KijiTable mTable;
  /** Kiji table writer used to perform deletes. */
  private KijiTableWriter mWriter;
  /** Opened Kiji to use. */
  private Kiji mKiji;
  /** KijiURI of the delete target(s). */
  private KijiURI mURI;
  /** name of target row for printing tool feedbacik. */
  private String mRowName = "";
  /** EntityId of target row. */
  private EntityId mRowId = null;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "delete";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Delete kiji tables, rows, and cells.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Data";
  }

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    setURI(parseURI(mKijiURIString));
    Preconditions.checkArgument(!getURI().getTable().isEmpty(), "--kiji must include a table");
    if (mUnhashedEntityId == null && mHashedEntityId == null) {
      Preconditions.checkArgument(getURI().getColumns().isEmpty(), "--kiji may not include columns"
          + " without --entity-id or --entity-hash");
      Preconditions.checkArgument(mUpToTimestamp == UNSPECIFIED_TIMESTAMP, "--upto-timestamp "
        + "requires --entity-id or --entity-hash");
      Preconditions.checkArgument(mExactTimestamp == UNSPECIFIED_TIMESTAMP, "--exact-timestamp "
        + "requires --entity-id or --entity-hash");
      Preconditions.checkArgument(!mMostRecent, "--most-recent requires"
          + " --entity-id or --entity-hash");
    }
    if (getURI().getColumns().isEmpty()) {
      Preconditions.checkArgument(mExactTimestamp == UNSPECIFIED_TIMESTAMP, "--exact-timestamp "
        + "requires at least one family:column specified in --kiji");
      Preconditions.checkArgument(!mMostRecent, "--most-recent requires"
         + " at least one family:column specified in --kiji");
    }
  }

  /**
   * Delete the most recent value in specified column or columns.
   *
   * @return return code
   * @throws Exception if there is an exception
   */
  private int deleteMostRecent() throws Exception {
    if (mExactTimestamp != UNSPECIFIED_TIMESTAMP || mUpToTimestamp != UNSPECIFIED_TIMESTAMP) {
      getPrintStream().println("--most-recent overrides timestamp arguments.");
      }
    if (isInteractive()) {
      if (!yesNoPrompt("Are you sure you want to delete the most recent value(s) at:\n"
          + mRowName + " - " + getURI().getColumns().toString() + " ?")) {
        getPrintStream().println("Delete aborted.");
        return 0;
      }
    }
    for (KijiColumnName column : getURI().getColumns()) {
      if (column.getQualifier().isEmpty()) {
        getPrintStream().println("the most recent value in an entire family cannot be"
            + "deleted.\nPlease specify family:column.");
      } else {
        mWriter.deleteCell(mRowId, column.getFamily(), column.getQualifier());
        getPrintStream().printf("Value at %s deleted.", column);
      }
    }
    return 0;
  }

  /**
   * Delete the most recent value in specified column or columns.
   *
   * @return return code
   * @throws Exception if there is an exception
   */
  private int deleteExact() throws Exception {
    if (isInteractive()) {
      if (!yesNoPrompt("Are you sure you want to delete the value(s) at:\n"
        + mRowName + " [" + mExactTimestamp + "] " + getURI().getColumns().toString() + " ?")) {
        getPrintStream().println("Delete aborted.");
        return 0;
      }
    }
    for (KijiColumnName column : getURI().getColumns()) {
      if (column.getQualifier().isEmpty()) {
        getPrintStream().println("values with an exact timestamp cannot be deleted from an"
            + " entire family at once.");
      } else {
        mWriter.deleteCell(mRowId, column.getFamily(), column.getQualifier(), mExactTimestamp);
        getPrintStream().printf("Value at %s deleted.", column);
      }
    }
    return 0;
  }

  /**
   * Delete the most recent value in specified column or columns.
   *
   * @return return code
   * @throws Exception if there is an exception
   */
  private int deleteRow() throws Exception {
    if (isInteractive()) {
      if (!yesNoPrompt("Are you sure you want to delete all cells in row:\n"
        + mRowName + " ?")) {
        getPrintStream().println("Delete aborted.");
        return 0;
      }
    }
    mWriter.deleteRow(mRowId);
    getPrintStream().printf("Row %s deleted.", mRowName);
    return 0;
  }

  /**
   * Delete the most recent value in specified column or columns.
   *
   * @return return code
   * @throws Exception if there is an exception
   */
  private int deleteRowUpTo() throws Exception {
    if (isInteractive()) {
      if (!yesNoPrompt("Are you sure you want to delete all values in row:\n"
          + mRowName + " older than or equal to: " + mUpToTimestamp + " ?")) {
        getPrintStream().println("Delete aborted.");
        return 0;
      }
    }
    mWriter.deleteRow(mRowId, mUpToTimestamp);
    getPrintStream().println("Values deleted.");
    return 0;
  }

  /**
   * Delete the most recent value in specified column or columns.
   *
   * @return return code
   * @throws Exception if there is an exception
   */
  private int deleteCells() throws Exception {
    if (isInteractive()) {
      if (!yesNoPrompt(String.format("Are you sure you want to delete all values in row:\n"
          + mRowName + " in %s", getURI().getColumns()))) {
        getPrintStream().println("Delete aborted.");
        return 0;
      }
    }
    for (KijiColumnName column : getURI().getColumns()) {
      if (!column.isFullyQualified()) {
        mWriter.deleteFamily(mRowId, column.getFamily());
        getPrintStream().printf("Cells in %s deleted.", column.getFamily());
      } else {
        mWriter.deleteColumn(mRowId, column.getFamily(), column.getQualifier());
        getPrintStream().printf("Cells in %s deleted.", column);
      }
    }
    return 0;
  }

  /**
   * Delete the most recent value in specified column or columns.
   *
   * @return return code
   * @throws Exception if there is an exception
   */
  private int deleteCellsUpTo() throws Exception {
    if (isInteractive()) {
      if (!yesNoPrompt("Are you sure you want to delete all cells on row:\n"
          + mRowName + " in: " + getURI().getColumns() + " older than or equal to: "
          + mUpToTimestamp + " ?")) {
        getPrintStream().println("Delete aborted.");
        return 0;
      }
    }
    for (KijiColumnName column : getURI().getColumns()) {
      if (column.getQualifier().isEmpty()) {
        mWriter.deleteFamily(mRowId, column.getFamily(), mUpToTimestamp);
        getPrintStream().printf("Values in %s deleted.", column.getFamily());
      } else {
        mWriter.deleteColumn(
            mRowId, column.getFamily(), column.getQualifier(), mUpToTimestamp);
        getPrintStream().printf("Values in %s deleted.", column);
      }
    }
    return 0;
  }

  /**
   * Delete the most recent value in specified column or columns.
   *
   * @return return code
   * @throws Exception if there is an exception
   */
  private int deleteTable() throws Exception {
    if (isInteractive()) {
      if (!yesNoPrompt("Are you sure you want to delete kiji table: " + getURI().getTable()
        + " ?")) {
        getPrintStream().println("Delete aborted.");
        return 0;
      }
    }
    getKiji().deleteTable(getURI().getTable());
    getPrintStream().println("Kiji table deleted.");
    return 0;
  }


  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    if (null != mHashedEntityId) {
      mRowName = mHashedEntityId;
    }
    if (null != mUnhashedEntityId) {
      mRowName = mUnhashedEntityId;
    }
    final KijiTableLayout tableLayout =
        getKiji().getMetaTable().getTableLayout(getURI().getTable());
    if (null == tableLayout) {
      getPrintStream().println("No such table: " + getURI().getTable());
      return 1;
    }

    // Set EntityId if supplied
    if (mHashedEntityId != null || mUnhashedEntityId != null) {
      mRowId = ToolUtils.createEntityIdFromUserInputs(
          mUnhashedEntityId, mHashedEntityId, tableLayout.getDesc().getKeysFormat());
    }

    // Delete the most recent value from a cell or cells
    if (mMostRecent) {
      return deleteMostRecent();
    }

    // Delete a value from a cell or cells specified by timestamp
    if (mExactTimestamp != UNSPECIFIED_TIMESTAMP) {
      return deleteExact();
    }

    // Delete all cells in a row
    if (!(mUnhashedEntityId == null && mHashedEntityId == null) && getURI().getColumns().isEmpty()
        && mUpToTimestamp == UNSPECIFIED_TIMESTAMP) {
      return deleteRow();
    }

    // Delete all values in a row older than or equal to a given timestamp
    if (!(mUnhashedEntityId == null && mHashedEntityId == null) && getURI().getColumns().isEmpty()
        && mUpToTimestamp != UNSPECIFIED_TIMESTAMP) {
      return deleteRowUpTo();
    }

    // Delete all cells on a row in listed families and columns
    if (!getURI().getColumns().isEmpty() && mUpToTimestamp == UNSPECIFIED_TIMESTAMP
        && mExactTimestamp == UNSPECIFIED_TIMESTAMP && !mMostRecent) {
      return deleteCells();
    }

    // Delete all values on a row in listed families and columns
    // older than or equal to a given timestamp
    if (!getURI().getColumns().isEmpty() && mUpToTimestamp != UNSPECIFIED_TIMESTAMP) {
      return deleteCellsUpTo();
    }

    // Delete a kiji table
    if (null == mHashedEntityId && null == mUnhashedEntityId) {
      return deleteTable();
    }

    // should be unreachable
    return 5;
  }

  /**
   * Opens a kiji instance.
   *
   * @return The opened kiji.
   * @throws IOException if there is an error.
   */
  private Kiji openKiji() throws IOException {
    return Kiji.Factory.open(getURI(), getConf());
  }

  /**
   * Retrieves the kiji instance used by this tool. On the first call to this method,
   * the kiji instance will be opened and will remain open until {@link #cleanup()} is called.
   *
   * @return The kiji instance.
   * @throws IOException if there is an error loading the kiji.
   */
  protected synchronized Kiji getKiji() throws IOException {
    if (null == mKiji) {
      mKiji = openKiji();
    }
    return mKiji;
  }

  /**
   * Returns the kiji URI of the target this tool operates on.
   *
   * @return The kiji URI of the target this tool operates on.
   */
  protected KijiURI getURI() {
    if (null == mURI) {
      getPrintStream().println("No URI specified.");
    }
    return mURI;
  }

  /**
   * Sets the kiji URI of the target this tool operates on.
   *
   * @param uri The kiji URI of the target this tool should operate on.
   */
  protected void setURI(KijiURI uri) {
    if (null == mURI) {
      mURI = uri;
    } else {
      getPrintStream().printf("URI is already set to: %s", mURI.toString());
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() throws Exception {
    super.setup();
    setURI(parseURI(mKijiURIString));
    getConf().setInt(HConstants.ZOOKEEPER_CLIENT_PORT, mURI.getZookeeperClientPort());
    getConf().set(HConstants.ZOOKEEPER_QUORUM,
        Joiner.on(",").join(getURI().getZookeeperQuorumOrdered()));
    setConf(HBaseConfiguration.addHbaseResources(getConf()));
    mTable = getKiji().openTable(getURI().getTable());
    mWriter = mTable.openTableWriter();
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup() throws IOException {
    IOUtils.closeQuietly(mWriter);
    IOUtils.closeQuietly(mTable);
    ResourceUtils.releaseOrLog(mKiji);
    super.cleanup();
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new DeleteTool(), args));
  }
}
