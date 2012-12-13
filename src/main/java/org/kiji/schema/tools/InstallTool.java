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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiInstaller;
import org.kiji.schema.impl.DefaultHTableInterfaceFactory;

/**
 * A command-line tool for installing kiji instances on hbase clusters.
 */
@ApiAudience.Private
public final class InstallTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(InstallTool.class);

  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {

    getPrintStream().println("Creating kiji instance: " + getURI());
    getPrintStream().println("Creating meta tables for kiji instance in hbase...");

    KijiConfiguration kijiConf = new KijiConfiguration(getConf(), getURI().getInstance());
    new KijiInstaller().install(kijiConf, DefaultHTableInterfaceFactory.get());

    getPrintStream().println("Successfully created kiji instance: " + getURI());

    return 0;
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new InstallTool(), args));
  }
}
