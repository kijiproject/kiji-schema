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

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.KijiInstaller;
import org.kiji.schema.KijiInvalidNameException;

/**
 * A command-line tool for uninstalling kiji instances from an hbase cluster.
 */
@ApiAudience.Private
public final class UninstallTool extends BaseTool {

  @Flag(name="confirm", usage="If true, uninstall will be performed without prompt.")
  private boolean mConfirm = false;

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
  protected int run(List<String> nonFlagArgs) throws Exception {
    getPrintStream().println("Deleting kiji instance: " + getURI().toString());
    if (!mConfirm)  {
      getPrintStream().println("Are you sure? This action will delete all meta and user data "
          + "from hbase and cannot be undone!");
      if (!yesNoPrompt()) {
        getPrintStream().println("Delete aborted.");
        return 0;
      }
    }
    try {
      KijiInstaller.uninstall(getURI(), getConf());
      getPrintStream().println("Deleted kiji instance: " + getURI().toString());
      return 0;
    } catch (IOException e) {
      getPrintStream().println("Error performing I/O during uninstall: " + e.getMessage());
      return 1;
    } catch (KijiInvalidNameException e) {
      getPrintStream().println("Invalid Kiji instance: " + e.getMessage());
      return 1;
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
