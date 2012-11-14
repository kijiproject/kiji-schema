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

package org.kiji.schema.impl;

import java.io.IOException;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableFactory;

/**
 * The default implementation of a KijiTableFactory that creates KijiTables via KijiTable.open().
 */
public class DefaultKijiTableFactory implements KijiTableFactory {
  /** Kiji instance. */
  private final Kiji mKiji;

  /**
   * Constructs a KijiTableFactory that opens KijiTables from within a Kiji instance.
   *
   * @param kiji The kiji instance containing the tables to open.
   */
  public DefaultKijiTableFactory(Kiji kiji) {
    mKiji = kiji;
  }

  /** {@inheritDoc} */
  @Override
  public KijiTable openTable(String tableName) throws IOException {
    return mKiji.openTable(tableName);
  }
}
