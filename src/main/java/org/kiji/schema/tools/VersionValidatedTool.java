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

import org.kiji.schema.IncompatibleKijiVersionException;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiConfiguration;

/**
 * Base class for command-line tools that require compatibility with the data format version of the
 * kiji instance used by the tool.  Subclasses of this tool will first check that the data format
 * version of the kiji instance in hbase matches that of the client.  If not, the tool
 * will exit early.
 */
public abstract class VersionValidatedTool extends KijiTool {
  /**
   * Constructor.
   */
  protected VersionValidatedTool() {
    super();
  }

  /** {@inheritDoc} */
  @Override
  protected Kiji openKiji() throws IOException {
    KijiConfiguration kijiConf = new KijiConfiguration(getConf(), getURI().getInstance());
    try {
      return Kiji.open(kijiConf);
    } catch (IncompatibleKijiVersionException iwve) {
      System.err.println("Error: " + iwve.getMessage());
      System.err.println("If this is a new client package, try to update your table layout.");
      throw iwve;
    }
  }
}
