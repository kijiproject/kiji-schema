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

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.KConstants;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiInstaller;
import org.kiji.schema.KijiInvalidNameException;
import org.kiji.schema.KijiNotInstalledException;
import org.kiji.schema.KijiURI;

/**
 * A command-line tool for uninstalling kiji instances from an hbase cluster.
 */
@ApiAudience.Private
public final class UninstallTool extends BaseTool {

  @Flag(name="kiji", usage="URI of the Kiji instance to uninstall.")
  private String mKijiURIFlag = KConstants.DEFAULT_INSTANCE_URI;

  /** URI of the Kiji instance to uninstall. */
  private KijiURI mKijiURI = null;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "uninstall";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Remove a kiji instance from a running HBase cluster.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Admin";
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() throws Exception {
    super.setup();
    Preconditions.checkArgument((mKijiURIFlag != null) && !mKijiURIFlag.isEmpty(),
        "Specify the Kiji instance to uninstall with --kiji=kiji://hbase-address/kiji-instance");
    mKijiURI = KijiURI.newBuilder(mKijiURIFlag).build();
    Preconditions.checkArgument(mKijiURI.getInstance() != null,
        "Specify the Kiji instance to uninstall with --kiji=kiji://hbase-address/kiji-instance");
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    Preconditions.checkArgument(nonFlagArgs.isEmpty(),
        "Unexpected command-line argument: [%s]", Joiner.on(",").join(nonFlagArgs));
    getPrintStream().println("Deleting kiji instance: " + mKijiURI.toString());
    if (isInteractive())  {
      final Kiji kiji = Kiji.Factory.open(mKijiURI, getConf());
      try {
        getPrintStream().println("WARNING: This instance contains the table(s):");
        for (String name : kiji.getTableNames()) {
          getPrintStream().println(name);
        }
      } finally {
        kiji.release();
      }

      getPrintStream().println();
      if (!inputConfirmation("Are you sure? This action will delete all meta and user data "
          + "from hbase and cannot be undone!", mKijiURI.getInstance())) {
        getPrintStream().println("Delete aborted.");
        return FAILURE;
      }
    }
    try {
      KijiInstaller.get().uninstall(mKijiURI, getConf());
      getPrintStream().println("Deleted kiji instance: " + mKijiURI.toString());
      return SUCCESS;
    } catch (IOException ioe) {
      getPrintStream().println("Error performing I/O during uninstall: " + ioe.getMessage());
      return FAILURE;
    } catch (KijiInvalidNameException kine) {
      getPrintStream().println("Invalid Kiji instance: " + kine.getMessage());
      return FAILURE;
    } catch (KijiNotInstalledException knie) {
      getPrintStream().printf("Kiji instance '%s' is not installed.%n", mKijiURI);
      return FAILURE;
    }
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new UninstallTool(), args));
  }
}
