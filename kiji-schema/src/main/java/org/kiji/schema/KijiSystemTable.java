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

package org.kiji.schema;

import java.io.Closeable;
import java.io.IOException;

import org.kiji.annotations.ApiAudience;

/**
 * The Kiji system table, which stores system information such as the version, ready state, and
 * locks.
 *
 * @see KijiMetaTable
 * @see KijiSystemTable
 */
@ApiAudience.Framework
public abstract class KijiSystemTable implements Closeable {
  /**
   * Gets the version of kiji installed.  This refers to the version of
   * the meta tables and other administrative kiji info installed, not
   * the client code.
   *
   * @return the version string.
   * @throws IOException If there is an error.
   */
  public abstract String getDataVersion() throws IOException;

  /**
   * Sets the version of kiji installed.  This refers to the version of
   * the meta tables and other administrative kiji info installed, not
   * the client code.
   *
   * @param version the version string.
   * @throws IOException If there is an error.
   */
  public abstract void setDataVersion(String version) throws IOException;

  /**
   * Gets the value associated with a property key.
   *
   * @param key The property key to look up.
   * @return The value in the system table with the given key, or null if the key doesn't exist.
   * @throws IOException If there is an error.
   */
  public abstract byte[] getValue(String key) throws IOException;

  /**
   * Sets a value for a property key, which creates it if it doesn't exist.
   *
   * @param key The property key to set.
   * @param value The value of the property.
   * @throws IOException If there is an error.
   */
  public abstract void putValue(String key, byte[] value) throws IOException;

}
