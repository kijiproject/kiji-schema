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

import java.util.List;

import org.apache.hadoop.util.ToolRunner;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiInstaller;

/**
 * A command-line tool for uninstalling kiji instances from an hbase cluster.
 */
@ApiAudience.Private
public final class UninstallTool extends BaseTool {

  @Flag(name="confirm", usage="If true, uninstall will be performed without prompt.")
  private boolean mConfirm = false;

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
      KijiInstaller installer = new KijiInstaller();
      installer.uninstall(new KijiConfiguration(getConf(), getURI().getInstance()));
      getPrintStream().println("Deleted kiji instance: " + getURI().toString());
      return 0;
    } catch (Exception e) {
      getPrintStream().println("Error during uninstall: " + e.getMessage());
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
    System.exit(ToolRunner.run(new UninstallTool(), args));
  }
}
