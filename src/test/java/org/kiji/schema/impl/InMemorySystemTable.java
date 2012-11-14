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
import java.util.HashMap;
import java.util.Map;

import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.util.VersionInfo;

/**
 * An in-memory system table useful for testing.
 */
public class InMemorySystemTable extends KijiSystemTable {
  /** The data version stored in this system table. */
  private String mDataVersion;

  /** A map for putting and getting values. */
  private Map<String, byte[]> mValueMap = new HashMap<String, byte[]>();

  /**
   * Constructs a system table with the client data version as the installed data version.
   */
  public InMemorySystemTable() {
    mDataVersion = VersionInfo.getClientDataVersion();
  }

  /** {@inheritDoc} */
  @Override
  public String getDataVersion() throws IOException {
    return mDataVersion;
  }

  /** {@inheritDoc} */
  @Override
  public void setDataVersion(String version) throws IOException {
    mDataVersion = version;
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getValue(String key) throws IOException {
    return mValueMap.get(key);
  }

  /** {@inheritDoc} */
  @Override
  public void putValue(String key, byte[] value) throws IOException {
    mValueMap.put(key, value);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
  }
}
