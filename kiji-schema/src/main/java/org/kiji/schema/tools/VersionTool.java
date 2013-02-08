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

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.schema.util.VersionInfo;


/**
 * Command-line tool for displaying the kiji software version running and the kiji data version
 * in use for a specified kiji instance.
 */
@ApiAudience.Private
public final class VersionTool extends OpenedKijiTool {
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
  protected int run(List<String> nonFlagArgs) throws Exception {
    String clientSoftwareVersion = VersionInfo.getSoftwareVersion();
    getPrintStream().println("kiji client software version: " + clientSoftwareVersion);

    ProtocolVersion clientDataVersion = VersionInfo.getClientDataVersion();
    getPrintStream().println("kiji client data version: " + clientDataVersion);

    ProtocolVersion clusterDataVersion = VersionInfo.getClusterDataVersion(getKiji());
    getPrintStream().println("kiji cluster data version: " + clusterDataVersion);

    return 0;
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
