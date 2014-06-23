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
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.impl.hbase.HBaseKijiFactory;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.security.KijiSecurityManager;
import org.kiji.schema.util.ReferenceCountable;

/**
 * <p>Provides a handle to a Kiji instance that contains table information and access to
 * Kiji administrative functionality.  Kiji instances are identified by a {@link KijiURI}.
 * The default KijiURI is: <em>kiji://.env/default/</em></p>
 *
 * <h2>Kiji instance lifecycle:</h2>
 * An instance to Kiji can be retrieved using {@link Kiji.Factory#open(KijiURI)}.  Cleanup is
 * requested with the inherited {@link #release()} method.  Thus the lifecycle of a Kiji
 * object looks like:
 * <pre><code>
 *   Kiji kiji = Kiji.Factory.open(kijiURI);
 *   // Do some magic
 *   kiji.release();
 * </code></pre>
 *
 * <h2>Base Kiji tables within an instance:</h2>
 * <p>
 *   Upon installation, a Kiji instance contains a {@link KijiSystemTable}, {@link KijiMetaTable},
 *   and {@link KijiSchemaTable} that each contain information relevant to the operation of Kiji.
 * </p>
 * <ul>
 *   <li>System table: Kiji system and version information.</li>
 *   <li>Meta table: Metadata about the existing Kiji tables.</li>
 *   <li>Schema table: Avro Schemas of the cells in Kiji tables.</li>
 * </ul>
 * <p>
 *   These tables can be accessed via the {@link #getSystemTable()}, {@link #getMetaTable()},
 *   and {@link #getSchemaTable()} methods respectively.
 * </p>
 *
 * <h2>User tables:</h2>
 * <p>
 *   User tables can be accessed using {@link #openTable(String)}.  Note that they must be closed
 *   after they are no longer necessary.
 * </p>
 * <pre><code>
 *   KijiTable kijiTable = kiji.openTable("myTable");
 *   // Do some table operations
 *   kijiTable.close();
 * </code></pre>
 *
 * <h2>Administration of user tables:</h2>
 * <p>
 *   The Kiji administration interface contains methods to create, modify, and delete tables from a
 *   Kiji instance:
 * </p>
 * <ul>
 *   <li>{@link #createTable(TableLayoutDesc)} - creates a Kiji table with a specified layout.</li>
 *   <li>{@link #modifyTableLayout(TableLayoutDesc)} - updates a Kiji table with a new layout.</li>
 *   <li>{@link #deleteTable(String)} - removes a Kiji table from HBase.</li>
 * </ul>
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
public interface Kiji extends KijiTableFactory, ReferenceCountable<Kiji> {
  /**
   * Provider for the default Kiji factory.
   *
   * Ensures that there is only one KijiFactory instance.
   */
  public static final class Factory {

    /**
     * Get an HBase-backed KijiFactory instance.
     *
     * @return a default HBase KijiFactory.
     * @deprecated use {@link #get(KijiURI)} instead.
     */
    @Deprecated
    public static KijiFactory get() {
      return new HBaseKijiFactory();
    }

    /**
     * Returns a KijiFactory for the appropriate Kiji type of the URI.
     *
     * @param uri for the Kiji instance to build with the factory.
     * @return the default KijiFactory.
     */
    public static KijiFactory get(KijiURI uri) {
      return uri.getKijiFactory();
    }

    /**
     * Opens a Kiji instance by URI.
     * <p>
     * Caller does not need to call {@code Kiji.retain()}, but must call {@code Kiji.release()}
     * after the returned {@code Kiji} will no longer be used.
     *
     * @param uri URI specifying the Kiji instance to open.
     * @return the specified Kiji instance.
     * @throws IOException on I/O error.
     */
    public static Kiji open(KijiURI uri) throws IOException {
      return get(uri).open(uri);
    }

    /**
     * Opens a Kiji instance by URI.
     * <p>
     * Caller does not need to call {@code Kiji.retain()}, but must call {@code Kiji.release()}
     * after the returned {@code Kiji} will no longer be used.
     *
     * @param uri URI specifying the Kiji instance to open.
     * @param conf Hadoop configuration.
     * @return the specified Kiji instance.
     * @throws IOException on I/O error.
     */
    public static Kiji open(KijiURI uri, Configuration conf) throws IOException {
      return get(uri).open(uri, conf);
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
   * <p> The schema table is owned by the Kiji instance : do not close it. </p>.
   *
   * @return The kiji schema table.
   * @throws IOException If there is an error.
   */
  KijiSchemaTable getSchemaTable() throws IOException;

  /**
   * Gets the system table for this Kiji instance.
   *
   * <p> The system table is owned by the Kiji instance : do not close it. </p>.
   *
   * @return The kiji system table.
   * @throws IOException If there is an error.
   */
  KijiSystemTable getSystemTable() throws IOException;

  /**
   * Gets the meta table for this Kiji instance.
   *
   * <p> The meta table is owned by the Kiji instance : do not close it. </p>.
   *
   * @return The kiji meta table.
   * @throws IOException If there is an error.
   */
  KijiMetaTable getMetaTable() throws IOException;

  /**
   * Returns whether security is enabled for this Kiji.
   *
   * @return whether security is enabled for this Kiji.
   * @throws IOException If there is an error.
   */
  boolean isSecurityEnabled() throws IOException;

  /**
   * Gets the security manager for this Kiji.  This method creates a new KijiSecurityManager if one
   * doesn't exist for this instance already.
   *
   * <p>Throws KijiSecurityException if the Kiji security version is not compatible with a
   * KijiSecurityManager.  Use {@link #isSecurityEnabled()} to check whether security version
   * is compatible first.</p>
   *
   * @return The KijiSecurityManager for this Kiji.
   * @throws IOException If there is an error opening the KijiSecurityManager.
   */
  KijiSecurityManager getSecurityManager() throws IOException;

  /**
   * Creates a Kiji table in an HBase instance.
   *
   * @param name The name of the table to create.
   * @param layout The initial layout of the table (with unassigned column ids).
   * @throws IOException on I/O error.
   * @throws KijiAlreadyExistsException if the table already exists.
   * @deprecated Replaced by {@link #createTable(TableLayoutDesc)}
   */
  @Deprecated
  void createTable(String name, KijiTableLayout layout) throws IOException;

  /**
   * Creates a Kiji table in an HBase instance.
   *
   * @param layout The initial layout of the table (with unassigned column ids).
   * @throws IOException on I/O error.
   * @throws KijiAlreadyExistsException if the table already exists.
   */
  void createTable(TableLayoutDesc layout) throws IOException;

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
   * @deprecated Replaced by {@link #createTable(TableLayoutDesc, int)}
   */
  @Deprecated
  void createTable(String name, KijiTableLayout layout, int numRegions) throws IOException;

  /**
   * Creates a Kiji table in an HBase instance.
   *
   * @param layout The initial layout of the table (with unassigned column ids).
   * @param numRegions The initial number of regions to create.
   * @throws IllegalArgumentException If numRegions is non-positive, or if numRegions is
   *     more than 1 and row key hashing on the table layout is disabled.
   * @throws IOException on I/O error.
   * @throws KijiAlreadyExistsException if the table already exists.
   */
  void createTable(TableLayoutDesc layout, int numRegions) throws IOException;

  /**
   * Creates a Kiji table in an HBase instance.
   *
   * @param name The name of the table to create.
   * @param layout The initial layout of the table (with unassigned column ids).
   * @param splitKeys The initial key boundaries between regions.  There will be splitKeys
   *     + 1 regions created.  Pass null to specify the default single region.
   * @throws IOException on I/O error.
   * @throws KijiAlreadyExistsException if the table already exists.
   * @deprecated Replaced by {@link #createTable(TableLayoutDesc, byte[][])}
   */
  @Deprecated
  void createTable(String name, KijiTableLayout layout, byte[][] splitKeys) throws IOException;

  /**
   * Creates a Kiji table in an HBase instance.
   *
   * @param layout The initial layout of the table (with unassigned column ids).
   * @param splitKeys The initial key boundaries between regions.  There will be splitKeys
   *     + 1 regions created.  Pass null to specify the default single region.
   * @throws IOException on I/O error.
   * @throws KijiAlreadyExistsException if the table already exists.
   */
  void createTable(TableLayoutDesc layout, byte[][] splitKeys) throws IOException;

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
   * @param name The name of the Kiji table to change the layout of.
   * @param update The new layout for the table.
   * @return the updated layout.
   * @throws IOException If there is an error.
   * @deprecated Replaced by {@link #modifyTableLayout(TableLayoutDesc)}.
   */
  @Deprecated
  KijiTableLayout modifyTableLayout(String name, TableLayoutDesc update) throws IOException;

  /**
   * Sets the layout of a table.
   *
   * @param update The new layout for the table.
   * @return the updated layout.
   * @throws IOException If there is an error.
   */
  KijiTableLayout modifyTableLayout(TableLayoutDesc update) throws IOException;

  /**
   * Sets the layout of a table, or print the results of setting the table layout if
   * dryRun is true.
   *
   * @param name The name of the Kiji table to affect.
   * @param update The new layout for the table.
   * @param dryRun true if this is a 'dry run', false if changes should be made.
   * @param printStream the print stream to use for information if dryRun is true.
   *     If null, stdout will be used.
   * @return the updated layout.
   * @throws IOException If there is an error.
   * @deprecated Replaced by {@link #modifyTableLayout(TableLayoutDesc, boolean, PrintStream)}.
   */
  @Deprecated
  KijiTableLayout modifyTableLayout(
      String name, TableLayoutDesc update, boolean dryRun, PrintStream printStream)
      throws IOException;

  /**
   * Sets the layout of a table, or print the results of setting the table layout if
   * dryRun is true.
   *
   * @param update The new layout for the table.
   * @param dryRun true if this is a 'dry run', false if changes should be made.
   * @param printStream the print stream to use for information if dryRun is true.
   *     If null, stdout will be used.
   * @return the updated layout.
   * @throws IOException If there is an error.
   */
  KijiTableLayout modifyTableLayout(
      TableLayoutDesc update, boolean dryRun, PrintStream printStream)
      throws IOException;
}
