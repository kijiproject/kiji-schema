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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.KConstants;
import org.kiji.schema.KijiAlreadyExistsException;
import org.kiji.schema.KijiInstaller;
import org.kiji.schema.KijiURI;

/**
 * A command-line tool for installing kiji instances on hbase clusters.
 */
@ApiAudience.Private
public final class InstallTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(InstallTool.class);

  @Flag(name="kiji", usage="URI of the Kiji instance to install.")
  private String mKijiURIFlag = KConstants.DEFAULT_URI;

  /** URI of the Kiji instance to install. */
  private KijiURI mKijiURI = null;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "install";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Install a kiji instance onto a running HBase cluster.";
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
        "Specify the Kiji instance to install with --kiji=kiji://hbase-address/kiji-instance");
    mKijiURI = KijiURI.newBuilder(mKijiURIFlag).build();
    Preconditions.checkArgument(mKijiURI.getInstance() != null,
        "Specify the Kiji instance to install with --kiji=kiji://hbase-address/kiji-instance");
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    getPrintStream().println("Creating kiji instance: " + mKijiURI);
    getPrintStream().println("Creating meta tables for kiji instance in hbase...");
    try {
      KijiInstaller.get().install(mKijiURI, getConf());
      getPrintStream().println("Successfully created kiji instance: " + mKijiURI);
      return SUCCESS;
    } catch (KijiAlreadyExistsException kaee) {
      getPrintStream().printf("Kiji instance '%s' already exists.%n", mKijiURI);
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
    System.exit(new KijiToolLauncher().run(new InstallTool(), args));
  }
}
