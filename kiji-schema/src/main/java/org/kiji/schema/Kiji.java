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

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.delegation.Lookups;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.ReferenceCountable;

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
public interface Kiji extends KijiTableFactory, ReferenceCountable<Kiji> {
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
     * <p> Caller does not need to call Kiji.retain(),
     *     but must call Kiji.release() when done with it.
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
     * <p> Caller does not need to call Kiji.retain(),
     *     but must call Kiji.release() when done with it.
     *
     * @param uri URI specifying the Kiji instance to open.
     * @param conf Hadoop configuration.
     * @return the specified Kiji instance.
     * @throws IOException on I/O error.
     */
    public static Kiji open(KijiURI uri, Configuration conf) throws IOException {
      return get().open(uri, conf);
    }

    /** Utility class may not be instantiated. */
    private Factory() {
    }
  }

  /** @return The hadoop configuration. */
  @Deprecated
  Configuration getConf();

  /** @return The address of this kiji instance, trimmed to the Kiji instance path component. */
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
   * Creates a Kiji table in an HBase instance.
   *
   * @param name The name of the table to create.
   * @param layout The initial layout of the table (with unassigned column ids).
   * @throws IOException on I/O error.
   * @throws KijiAlreadyExistsException if the table already exists.
   */
  void createTable(String name, KijiTableLayout layout) throws IOException;

  /**
   * Creates a Kiji table in an HBase instance.
   *
   * @param name The name of the table to create.
   * @param layout The initial layout of the table (with unassigned column ids).
   * @param numRegions The initial number of regions to create.
   * @throws IllegalArgumentException If numRegions is non-positive, or if numRegions is
   *     more than 1 and row key hashing on the table layout is disabled.
   * @throws IOException on I/O error.
   * @throws KijiAlreadyExistsException if the table already exists.
   */
  void createTable(String name, KijiTableLayout layout, int numRegions) throws IOException;

  /**
   * Creates a Kiji table in an HBase instance.
   *
   * @param name The name of the table to create.
   * @param layout The initial layout of the table (with unassigned column ids).
   * @param splitKeys The initial key boundaries between regions.  There will be splitKeys
   *     + 1 regions created.  Pass null to specify the default single region.
   * @throws IOException on I/O error.
   * @throws KijiAlreadyExistsException if the table already exists.
   */
  void createTable(String name, KijiTableLayout layout, byte[][] splitKeys) throws IOException;

  /**
   * Deletes a Kiji table.  Removes it from HBase.
   *
   * @param name The name of the Kiji table to delete.
   * @throws IOException If there is an error.
   */
  void deleteTable(String name) throws IOException;

  /**
   * Gets the list of Kiji table names.
   *
   * @return A list of the names of Kiji tables installed in the Kiji instance.
   * @throws IOException If there is an error.
   */
  List<String> getTableNames() throws IOException;

  /**
   * Sets the layout of a table.
   *
   * @param name The name of the Kiji table to affect.
   * @param update Layout update.
   * @return the updated layout.
   * @throws IOException If there is an error.
   */
  KijiTableLayout modifyTableLayout(String name, TableLayoutDesc update) throws IOException;

  /**
   * Sets the layout of a table, or print the results of setting the table layout if
   * dryRun is true.
   *
   * @param name The name of the Kiji table to affect.
   * @param update Layout update.
   * @param dryRun true if this is a 'dry run', false if changes should be made.
   * @param printStream the print stream to use for information if dryRun is true.
   *     If null, stdout will be used.
   * @return the updated layout.
   * @throws IOException If there is an error.
   */
  KijiTableLayout modifyTableLayout(String name, TableLayoutDesc update, boolean dryRun,
      PrintStream printStream) throws IOException;
}
