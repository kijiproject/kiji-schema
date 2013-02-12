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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.KijiAlreadyExistsException;
import org.kiji.schema.KijiInstaller;
import org.kiji.schema.KijiURI;
import org.kiji.schema.hbase.HBaseFactory;

/**
 * A command-line tool for installing kiji instances on hbase clusters.
 */
@ApiAudience.Private
public final class InstallTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(InstallTool.class);

  @Flag(name="kiji", usage="KijiURI of the instance to install.")
  private String mKijiURIString;

  /** The KijiURI of the instance to install. */
  private KijiURI mURI;

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
  protected int run(List<String> nonFlagArgs) throws Exception {
    getPrintStream().println("Creating kiji instance: " + getURI());
    getPrintStream().println("Creating meta tables for kiji instance in hbase...");
    try {
      KijiInstaller.get().install(getURI(), HBaseFactory.Provider.get(), getConf());
      getPrintStream().println("Successfully created kiji instance: " + getURI());
      return 0;
    } catch (KijiAlreadyExistsException kaee) {
      getPrintStream().printf("Kiji instance '%s' already exists.%n", getURI());
      return 1;
    }
  }

  /** Sets the KijiURI of the instance to install.
   *
   * @param uri The KijiURI to set for this tool
   */
  protected void setURI(KijiURI uri) {
    if (null == mURI) {
      mURI = uri;
    } else {
      getPrintStream().println("URI is already set.");
    }
  }

  /** Gets the KijiURI of the instance to install.
   *
   * @return The KijiURI of the instance to install.
   */
  protected KijiURI getURI() {
    if (null == mURI) {
      getPrintStream().println("No URI set.");
    }
    return mURI;
  }

  /** {@inheritDoc} */
  @Override
  public void setup() throws Exception {
    super.setup();
    setURI(parseURI(mKijiURIString));
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
