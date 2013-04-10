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

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.KConstants;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.schema.util.VersionInfo;

/**
 * Command-line tool for displaying the kiji software version running and the kiji data version
 * in use for a specified kiji instance.
 */
@ApiAudience.Private
public final class VersionTool extends BaseTool {

  @Flag(name="kiji", usage="URI of the Kiji instance to print the version of.")
  private String mKijiURIFlag = KConstants.DEFAULT_INSTANCE_URI;

  private KijiURI mKijiURI = null;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "version";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Print the kiji distribution and data versions in use.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Help";
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() throws Exception {
    Preconditions.checkArgument((mKijiURIFlag != null) && !mKijiURIFlag.isEmpty(),
        "Specify the Kiji instance to uninstall with --kiji=kiji://hbase-address/kiji-instance");
    mKijiURI = KijiURI.newBuilder(mKijiURIFlag).build();
    Preconditions.checkArgument(mKijiURI.getInstance() != null,
        "Specify the Kiji instance to uninstall with --kiji=kiji://hbase-address/kiji-instance");
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    final String clientSoftwareVersion = VersionInfo.getSoftwareVersion();
    getPrintStream().println("kiji client software version: " + clientSoftwareVersion);

    final ProtocolVersion clientDataVersion = VersionInfo.getClientDataVersion();
    getPrintStream().println("kiji client data version: " + clientDataVersion);

    final Kiji kiji = Kiji.Factory.open(mKijiURI, getConf());
    try {
      final ProtocolVersion clusterDataVersion = VersionInfo.getClusterDataVersion(kiji);
      getPrintStream().println("kiji cluster data version: " + clusterDataVersion);
    } finally {
      kiji.release();
    }

    ProtocolVersion minimumLayoutVersion = KijiTableLayout.getMinSupportedLayoutVersion();
    ProtocolVersion maximumLayoutVersion = KijiTableLayout.getMaxSupportedLayoutVersion();
    getPrintStream().println("layout versions supported: "
        + minimumLayoutVersion + " to " + maximumLayoutVersion);

    return SUCCESS;
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new VersionTool(), args));
  }
}
