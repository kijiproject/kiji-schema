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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.ToolRunner;

import org.kiji.common.flags.Flag;
import org.kiji.schema.KijiAdmin;

/**
 * Command-line tool for deleting kiji tables from kiji instances.
 */
public class DeleteTableTool extends VersionValidatedTool {
  @Flag(name="table", usage="The name of the kiji table to delete.")
  private String mTableName = "";

  @Flag(name="confirm", usage="If true, delete will be performed without prompt.")
  private boolean mConfirm = false;

  private HBaseAdmin mHBaseAdmin;

  @Override
  protected void validateFlags() throws Exception {
    super.validateFlags();
    if (mTableName.isEmpty()) {
      throw new RequiredFlagException("table");
    }
  }

  @Override
  protected void setup() throws Exception {
    super.setup();
    mHBaseAdmin = new HBaseAdmin(getConf());
  }

  @Override
  protected void cleanup() throws IOException {
    IOUtils.closeQuietly(mHBaseAdmin);
    super.cleanup();
  }

  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    setURI(getURI().setTableName(mTableName));
    getPrintStream().println("Deleting kiji table: " + getURI().toString());
    if (!mConfirm) {
      getPrintStream().println("Are you sure? This action will remove this table and all its data"
          + " from kiji and cannot be undone!");
      if (!yesNoPrompt()) {
        getPrintStream().println("Delete aborted.");
        return 0;
      }
    }

    KijiAdmin admin = new KijiAdmin(mHBaseAdmin, getKiji());

    admin.deleteTable(mTableName);
    getPrintStream().println("Deleted kiji table: " + getURI().toString());
    return 0;
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new DeleteTableTool(), args));
  }
}
