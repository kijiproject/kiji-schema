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

import org.apache.hadoop.hbase.HBaseConfiguration;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiSystemTable;

/**
 * An in-memory implementation of a kiji instance for testing.
 *
 * This class should <i>not</i> be used outside of testing.
 */
public class InMemoryKiji extends Kiji {
  private KijiSchemaTable mSchemaTable;
  private KijiSystemTable mSystemTable;
  private KijiMetaTable mMetaTable;

  /**
   * Constructor.
   *
   * @throws IOException If there is an error.
   */
  public InMemoryKiji() throws IOException {
    super(new KijiConfiguration(HBaseConfiguration.create(),
        KijiConfiguration.DEFAULT_INSTANCE_NAME));
  }

  /**
   * Gets the schema table for this Kiji instance.
   *
   * @return The kiji schema table.
   * @throws IOException If there is an error.
   */
  public synchronized KijiSchemaTable getSchemaTable() throws IOException {
    if (null == mSchemaTable) {
      mSchemaTable = new InMemorySchemaTable();
    }
    return mSchemaTable;
  }

  /**
   * Gets the system table for this Kiji instance.
   *
   * @return The kiji system table.
   * @throws IOException If there is an error.
   */
  public synchronized KijiSystemTable getSystemTable() throws IOException {
    if (null == mSystemTable) {
      mSystemTable = new InMemorySystemTable();
    }
    return mSystemTable;
  }

  /**
   * Gets the meta table for this Kiji instance.
   *
   * @return The kiji meta table.
   * @throws IOException If there is an error.
   */
  public synchronized KijiMetaTable getMetaTable() throws IOException {
    if (null == mMetaTable) {
      mMetaTable = new InMemoryMetaTable();
    }
    return mMetaTable;
  }
}
