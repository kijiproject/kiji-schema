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

import org.apache.hadoop.conf.Configuration;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.delegation.Lookups;

/**
 * <p>Provides a handle to a Kiji instance that contains configuration and
 * table information.</p>
 *
 * <p>An installed Kiji instance contains several tables:
 *   <ul>
 *     <li>System table: Kiji system and version information.</li>
 *     <li>Meta table: Metadata about the existing Kiji tables.</li>
 *     <li>Schema table: Avro Schemas of the cells in Kiji tables.</li>
 *     <li>User tables: The user-space Kiji tables that you create.</li>
 *   </ul>
 * </p>
 * <p>The default Kiji instance name is <em>default</em>.</p>
 */
@ApiAudience.Public
@Inheritance.Sealed
public interface Kiji extends KijiTableFactory, Closeable {
  /**
   * Provider for the default Kiji factory.
   *
   * Ensures that there is only one KijiFactory instance.
   */
  public static final class Factory {
    /** KijiFactory instance. */
    private static KijiFactory mInstance;

    /** @return the default KijiFactory. */
    public static KijiFactory get() {
      synchronized (Kiji.Factory.class) {
        if (null == mInstance) {
          mInstance = Lookups.getPriority(KijiFactory.class).lookup();
        }
        return mInstance;
      }
    }

    /**
     * Opens a Kiji instance by URI.
     *
     * @param uri URI specifying the Kiji instance to open.
     * @return the specified Kiji instance.
     * @throws IOException on I/O error.
     */
    public static Kiji open(KijiURI uri) throws IOException {
      return get().open(uri);
    }

    /**
     * Opens a Kiji instance by URI.
     *
     * @param uri URI specifying the Kiji instance to open.
     * @param conf Hadoop configuration.
     * @return the specified Kiji instance.
     * @throws IOException on I/O error.
     */
    public static Kiji open(KijiURI uri, Configuration conf) throws IOException {
      return get().open(uri, conf);
    }

    /**
     * Opens a Kiji instance. This method of opening a Kiji instance has been deprecated
     * in favor of a method that doesn't use KijiConfiguration.
     *
     * @param kijiConf The configuration.
     * @return An opened kiji instance.
     * @throws IOException If there is an error.
     */
    @Deprecated
    public static Kiji open(KijiConfiguration kijiConf) throws IOException {
      return get().open(kijiConf);
    }

    /** Utility class may not be instantiated. */
    private Factory() {
    }
  }

  /** @return The hadoop configuration. */
  @Deprecated
  Configuration getConf();

  /** @return The address of this kiji instance. */
  KijiURI getURI();

  /**
   * Gets the schema table for this Kiji instance.
   *
   * @return The kiji schema table.
   * @throws IOException If there is an error.
   */
  KijiSchemaTable getSchemaTable() throws IOException;

  /**
   * Gets the system table for this Kiji instance.
   *
   * @return The kiji system table.
   * @throws IOException If there is an error.
   */
  KijiSystemTable getSystemTable() throws IOException;

  /**
   * Gets the meta table for this Kiji instance.
   *
   * @return The kiji meta table.
   * @throws IOException If there is an error.
   */
  KijiMetaTable getMetaTable() throws IOException;

  /**
   * @return a KijiAdmin for this Kiji instance.
   * @throws IOException on I/O error.
   */
  KijiAdmin getAdmin() throws IOException;
}
