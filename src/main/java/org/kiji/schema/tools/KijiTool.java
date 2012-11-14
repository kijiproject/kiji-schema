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

import org.apache.commons.io.IOUtils;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiConfiguration;


/**
 * A command-line tool that supplies an opened kiji instance in addition to the facilities
 * provided by {@link BaseTool}.
 *
 * The kiji instance is lazily opened on the first call to {@link #getKiji()} and closed in
 * {@link #cleanup()}.
 */
public abstract class KijiTool extends BaseTool {
  /** The kiji instance. */
  private Kiji mKiji = null;

  /**
   * Opens a kiji instance.
   * Subclasses should override this method with validation logic as needed.
   * Subclasses should write a useful message to getPrintStream() if they generate
   * an IOException.
   *
   * @return The opened kiji.
   * @throws IOException if there is an error.
   */
  protected Kiji openKiji() throws IOException {
    KijiConfiguration kijiConf = new KijiConfiguration(getConf(), getURI().getInstance());
    return new Kiji(kijiConf, false);
  }

  /**
   * Retrieves the kiji instance used by this tool. On the first call to this method,
   * the kiji instance will be opened and will remain open until {@link #cleanup()} is called.
   *
   * @return The kiji instance.
   * @throws IOException if there is an error loading the kiji.
   */
  protected Kiji getKiji() throws IOException {
    if (null == mKiji) {
      mKiji = openKiji();
    }
    return mKiji;
  }

  @Override
  protected void cleanup() throws IOException {
    IOUtils.closeQuietly(mKiji);
    mKiji = null;
    super.cleanup();
  }
}
