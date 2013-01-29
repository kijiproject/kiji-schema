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

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiAdmin;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;


/**
 * Command-line tool to delete kiji tables, rows, and cells.
 *
 * --instance to select a kiji instance.
 *  if unspecified, uses default instance name
 *
 * --table to select a kiji table.
 *  required for all delete commands
 *
 * --row to select a kiji row.
 *  required for family, qualifier, and timestamp flags to operate
 *
 * --family to select a kiji family.
 *  required for qualifier flag to operate
 *
 * --qualifer to select a kiji column.
 *  required for exact-timestamp and most-recent flags to operate
 *
 * --upto-timestamp to select values with timestamp equal or lesser.
 *
 * --exact-timestamp to select a value with a specific timestamp.
 *
 * --most-recent to select the most recent value in a cell.
 */
@ApiAudience.Private
public final class DeleteTool extends VersionValidatedTool {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteTool.class);

  private static final long UNSPECIFIED_TIMESTAMP = Long.MIN_VALUE;

  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableWriter mWriter;
  private KijiAdmin mAdmin;

  @Flag(name="instance", usage="The name of the Kiji instance to use.")
  private String mInstanceName = KijiConfiguration.DEFAULT_INSTANCE_NAME;

  @Flag(name="table", usage="The name of the Kiji table to delete or delete from.")
  private String mTableName = "";

  @Flag(name="row", usage="The name of the row from which to delete. (requires --table)")
  private String mRowName = "";

  @Flag(name="family", usage="The name of the family from which to delete. (requires --row)")
  private String mFamilyName = "";

  @Flag(name="qualifier", usage="The name of the column from which to delete."
    + " (requires --family)")
  private String mQualifier = "";

  @Flag(name="upto-timestamp", usage="Delete all values with timestamp lower than or equal to "
    + "specified timestamp. (requires --row)")
  private long mUpToTimestamp = UNSPECIFIED_TIMESTAMP;

  @Flag(name="exact-timestamp", usage="Delete a value with a specified timestamp. (requires"
    + " --row, --family, --qualifier)")
  private long mExactTimestamp = UNSPECIFIED_TIMESTAMP;

  @Flag(name="most-recent", usage="If true, delete the most recent value in a specified column"
    + " and row. (requires --row, --family, --qualifier)")
  private boolean mMostRecent = false;

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
    Preconditions.checkArgument(!mTableName.equals(""), "--table is required");
    if (mRowName.equals("")) {
      Preconditions.checkArgument(mFamilyName.equals(""), "--family requires --row");
      Preconditions.checkArgument(mQualifier.equals(""), "--qualifier requires --row");
      Preconditions.checkArgument(mUpToTimestamp == UNSPECIFIED_TIMESTAMP, "--upto-timestamp "
        + "requires --row");
      Preconditions.checkArgument(mExactTimestamp == UNSPECIFIED_TIMESTAMP, "--exact-timestamp "
        + "requires --row");
      Preconditions.checkArgument(!mMostRecent, "--most-recent requires --row");
    }
    if (mFamilyName.equals("")) {
      Preconditions.checkArgument(mQualifier.equals(""), "--qualifier requires --family");
      Preconditions.checkArgument(mExactTimestamp == UNSPECIFIED_TIMESTAMP, "--exact-timestamp "
        + "requires --family");
      Preconditions.checkArgument(!mMostRecent, "--most-recent requires --family");
    }
    if (mQualifier.equals("")) {
      Preconditions.checkArgument(mExactTimestamp == UNSPECIFIED_TIMESTAMP,
        "--exact-timestamp requires"
        + " --qualifier");
      Preconditions.checkArgument(!mMostRecent, "--most-recent requires --qualifier");
    }
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {

    EntityId mRowId = mTable.getEntityId(mRowName);

    // Delete the most recent value from a cell
    if (mMostRecent) {
      if (mExactTimestamp != UNSPECIFIED_TIMESTAMP || mUpToTimestamp != UNSPECIFIED_TIMESTAMP) {
        getPrintStream().println("--most-recent overrides timestamp arguments.");
      }
      if (isInteractive()) {
        if (!yesNoPrompt("Are you sure you want to delete the most recent value at:\n"
          + mRowName + " - " + mFamilyName + ":" + mQualifier + " ?")) {
          getPrintStream().println("Delete aborted.");
          return 0;
        }
      }
      mWriter.deleteCell(mRowId, mFamilyName, mQualifier);
      getPrintStream().println("Value deleted.");
      return 0;
    }

    // Delete a value from a cell specified by timestamp
    if (mExactTimestamp != UNSPECIFIED_TIMESTAMP) {
      if (isInteractive()) {
        if (!yesNoPrompt("Are you sure you want to delete the value at:\n"
          + mRowName + " [" + mExactTimestamp + "] " + mFamilyName + ":" + mQualifier + " ?")) {
          getPrintStream().println("Delete aborted.");
          return 0;
        }
      }
      mWriter.deleteCell(mRowId, mFamilyName, mQualifier, mExactTimestamp);
      getPrintStream().println("Value deleted.");
      return 0;
    }

    // Delete all cells in a row
    if (!mRowName.equals("") && mFamilyName.equals("") && mUpToTimestamp == UNSPECIFIED_TIMESTAMP) {
      if (isInteractive()) {
        if (!yesNoPrompt("Are you sure you want to delete all cells in row:\n"
          + mRowName + " ?")) {
          getPrintStream().println("Delete aborted.");
          return 0;
        }
      }
      mWriter.deleteRow(mRowId);
      getPrintStream().println("Row deleted.");
      return 0;
    }

    // Delete all values in a row older than or equal to a given timestamp
    if (!mRowName.equals("") && mFamilyName.equals("") && mUpToTimestamp != UNSPECIFIED_TIMESTAMP) {
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

    // Delete all cells in a row and family
    if (!mFamilyName.equals("") && mQualifier.equals("")
        && mUpToTimestamp == UNSPECIFIED_TIMESTAMP) {
      if (isInteractive()) {
        if (!yesNoPrompt("Are you sure you want to delete all cells on row:\n"
          + mRowName + " in family: " + mFamilyName + " ?")) {
          getPrintStream().println("Delete aborted.");
          return 0;
        }
      }
      mWriter.deleteFamily(mRowId, mFamilyName);
      getPrintStream().println("Cells deleted.");
      return 0;
    }

    // Delete all values in a row and family older than or equal to a given timestamp
    if (!mFamilyName.equals("") && mQualifier.equals("")
        && mUpToTimestamp != UNSPECIFIED_TIMESTAMP) {
      if (isInteractive()) {
        if (!yesNoPrompt("Are you sure you want to delete all cells on row:\n"
          + mRowName + " in family: " + mFamilyName + " older than or equal to: " + mUpToTimestamp
          + " ?")) {
          getPrintStream().println("Delete aborted.");
          return 0;
        }
      }
      mWriter.deleteFamily(mRowId, mFamilyName, mUpToTimestamp);
      getPrintStream().println("Cells deleted.");
      return 0;
    }

    // Delete a cell
    if (!mQualifier.equals("") && mUpToTimestamp == UNSPECIFIED_TIMESTAMP) {
      if (isInteractive()) {
        if (!yesNoPrompt("Are you sure you want to delete the cell on row:\n"
          + mRowName + " in column: " + mFamilyName + ":" + mQualifier + " ?")) {
          getPrintStream().println("Delete aborted.");
          return 0;
        }
      }
      mWriter.deleteColumn(mRowId, mFamilyName, mQualifier);
      getPrintStream().println("Cell deleted.");
      return 0;
    }

    // Delete values from a cell older than or equal to a given timestamp
    if (!mQualifier.equals("") && mUpToTimestamp != UNSPECIFIED_TIMESTAMP) {
      if (isInteractive()) {
        if (!yesNoPrompt("Are you sure you want to delete all values on row:\n"
          + mRowName + " in column: " + mFamilyName + ":" + mQualifier + " older than or equal to: "
          + mUpToTimestamp + " ?")) {
          getPrintStream().println("Delete aborted.");
          return 0;
        }
      }
      mWriter.deleteColumn(mRowId, mFamilyName, mQualifier, mUpToTimestamp);
      getPrintStream().println("Values deleted.");
      return 0;
    }

    // Delete a kiji table
    if (mRowName.equals("")) {
      if (isInteractive()) {
        if (!yesNoPrompt("Are you sure you want to delete kiji table: " + mTableName
          + " ?")) {
          getPrintStream().println("Delete aborted.");
          return 0;
        }
      }
      mAdmin.deleteTable(mTableName);
      getPrintStream().println("Kiji table deleted.");
      return 0;
    }

    // should be unreachable
    return 3;
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() throws Exception {
    super.setup();
    mKiji = Kiji.Factory.open(new KijiConfiguration(getConf(), mInstanceName));
    mTable = mKiji.openTable(mTableName);
    mWriter = mTable.openTableWriter();
    mAdmin = mKiji.getAdmin();
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup() throws IOException {
    IOUtils.closeQuietly(mWriter);
    IOUtils.closeQuietly(mTable);
    mKiji.release();
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
