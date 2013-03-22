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

package org.kiji.schema.hbase;

import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.NotAKijiManagedTableException;

/**
 * <p>Multiple instances of Kiji can be installed on a single HBase
 * cluster.  Within a Kiji instance, several HBase tables are created
 * to manage system, metadata, schemas, and user-space tables.  This
 * class represents the name of one of those HBase tables that are
 * created and managed by Kiji.  This class should only be used internally
 * in Kiji modules, or by framework application developers who need
 * direct access to HBase tables managed by Kiji.</p>
 *
 * <p>
 * The names of tables in HBase created and managed by Kiji are
 * made of a list of delimited components.  There are at least 3
 * components of a name:
 *
 * <ol>
 *   <li>
 *     Prefix: a literal string "kiji" used to mark that this table
 *     is managed by Kiji.
 *   </li>
 *   <li>
 *     KijiInstance: the name of kiji instance managing this table.
 *   </li>
 *   <li>
 *     Type: the type of table (system, schema, meta, table).
 *   </li>
 * </ol>
 *
 * If the type of the table is "table", then it's name (the name users
 * of Kiji would use to refer to it) is the fourth and final component.
 * </p>
 *
 * <p>
 * For example, an HBase cluster might have the following tables:
 * <pre>
 * devices
 * kiji.default.meta
 * kiji.default.schema
 * kiji.default.schema_hash
 * kiji.default.schema_id
 * kiji.default.system
 * kiji.default.table.foo
 * kiji.default.table.bar
 * kiji.experimental.meta
 * kiji.experimental.schema
 * kiji.experimental.schema_hash
 * kiji.experimental.schema_id
 * kiji.experimental.system
 * kiji.experimental.table.baz
 * </pre>
 *
 * In this example, there is an HBase table completely unrelated to
 * kiji called "devices."  There are two kiji installations, one
 * called "default" and another called "experimental."  Within the
 * "default" installation, there are two Kiji tables, "foo" and
 * "bar."  Within the "experimental" installation, there is a single
 * Kiji table "baz."
 * </p>
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class KijiManagedHBaseTableName {
  /** The delimited used to separate the components of an HBase table name. */
  private static final char DELIMITER = '.';

  private static final Joiner DELIMITER_JOINER = Joiner.on(DELIMITER);

  /** The first component of all HBase table names managed by Kiji. */
  public static final String KIJI_COMPONENT = "kiji";

  /** Regexp matching Kiji system tables. */
  public static final Pattern KIJI_SYSTEM_TABLES_REGEX =
      Pattern.compile("kiji[.](.*)[.](meta|system|schema_hash|schema_id)");

  /** The name component used for the Kiji meta table. */
  private static final String KIJI_META_COMPONENT = "meta";

  /** The name component used for the Kiji schema hash table. */
  private static final String KIJI_SCHEMA_HASH_COMPONENT = "schema_hash";

  /** The name component used for the Kiji schema IDs table. */
  private static final String KIJI_SCHEMA_ID_COMPONENT = "schema_id";

  /** The name component used for the Kiji system table. */
  private static final String KIJI_SYSTEM_COMPONENT = "system";

  /** The name component used for all user-space Kiji tables. */
  private static final String KIJI_TABLE_COMPONENT = "table";

  /** The HBase table name. */
  private final String mHBaseTableName;

  /** The Kiji instance name. */
  private final String mKijiInstanceName;

  /** The Kiji table name, or null if it is not a user-space Kiji table. */
  private final String mKijiTableName;

  /**
   * Constructs a Kiji-managed HBase table name.
   *
   * @param kijiInstanceName The kiji instance name.
   * @param type The type component of the HBase table name (meta, schema, system, table).
   */
  private KijiManagedHBaseTableName(String kijiInstanceName, String type) {
    mHBaseTableName = DELIMITER_JOINER.join(KIJI_COMPONENT, kijiInstanceName, type);
    mKijiInstanceName = kijiInstanceName;
    mKijiTableName = null;
  }

  /**
   * Constructs a Kiji-managed HBase table name.
   *
   * @param kijiInstanceName The kiji instance name.
   * @param type The type component of the HBase table name.
   * @param kijiTableName The name of the user-space Kiji table.
   */
  private KijiManagedHBaseTableName(String kijiInstanceName, String type, String kijiTableName) {
    mHBaseTableName = DELIMITER_JOINER.join(KIJI_COMPONENT, kijiInstanceName, type, kijiTableName);
    mKijiInstanceName = kijiInstanceName;
    mKijiTableName = kijiTableName;
  }

  /**
   * Constructs using an HBase HTable name.
   *
   * @param hbaseTableName The HBase HTable name.
   * @return A new kiji-managed HBase table name.
   * @throws NotAKijiManagedTableException If the HBase table is not managed by kiji.
   */
  public static KijiManagedHBaseTableName get(String hbaseTableName)
      throws NotAKijiManagedTableException {
    // Split it into components.
    String[] components = StringUtils.splitPreserveAllTokens(
        hbaseTableName, Character.toString(DELIMITER), 4);

    // Make sure the first component is 'kiji'.
    if (!components[0].equals(KIJI_COMPONENT)) {
      throw new NotAKijiManagedTableException(
          hbaseTableName, "Doesn't start with kiji name component.");
    }

    if (components.length == 3) {
      // It's a managed kiji meta/schema/system table.
      return new KijiManagedHBaseTableName(components[1], components[2]);
    } else if (components.length == 4) {
      // It's a user-space kiji table.
      return new KijiManagedHBaseTableName(components[1], components[2], components[3]);
    } else {
      // Wrong number of components... must not be a kiji table.
      throw new NotAKijiManagedTableException(
          hbaseTableName, "Invalid number of name components.");
    }
  }

  /**
   * Gets a new instance of a Kiji-managed HBase table that holds the Kiji meta table.
   *
   * @param kijiInstanceName The name of the Kiji instance.
   * @return The name of the HBase table used to store the Kiji meta table.
   */
  public static KijiManagedHBaseTableName getMetaTableName(String kijiInstanceName) {
    return new KijiManagedHBaseTableName(kijiInstanceName, KIJI_META_COMPONENT);
  }

  /**
   * Gets a new instance of a Kiji-managed HBase table that holds the Kiji schema hash table.
   *
   * @param kijiInstanceName The name of the Kiji instance.
   * @return The name of the HBase table used to store the Kiji schema hash table.
   */
  public static KijiManagedHBaseTableName getSchemaHashTableName(String kijiInstanceName) {
    return new KijiManagedHBaseTableName(kijiInstanceName, KIJI_SCHEMA_HASH_COMPONENT);
  }

  /**
   * Gets a new instance of a Kiji-managed HBase table that holds the Kiji schema IDs table.
   *
   * @param kijiInstanceName The name of the Kiji instance.
   * @return The name of the HBase table used to store the Kiji schema IDs table.
   */
  public static KijiManagedHBaseTableName getSchemaIdTableName(String kijiInstanceName) {
    return new KijiManagedHBaseTableName(kijiInstanceName, KIJI_SCHEMA_ID_COMPONENT);
  }

  /**
   * Gets a new instance of a Kiji-managed HBase table that holds the Kiji system table.
   *
   * @param kijiInstanceName The name of the Kiji instance.
   * @return The name of the HBase table used to store the Kiji system table.
   */
  public static KijiManagedHBaseTableName getSystemTableName(String kijiInstanceName) {
    return new KijiManagedHBaseTableName(kijiInstanceName, KIJI_SYSTEM_COMPONENT);
  }

  /**
   * Gets a new instance of a Kiji-managed HBase table that holds a user-space Kiji table.
   *
   * @param kijiInstanceName The name of the Kiji instance.
   * @param kijiTableName The name of the user-space Kiji table.
   * @return The name of the HBase table used to store the user-space Kiji table.
   */
  public static KijiManagedHBaseTableName getKijiTableName(
      String kijiInstanceName, String kijiTableName) {
    return new KijiManagedHBaseTableName(kijiInstanceName, KIJI_TABLE_COMPONENT, kijiTableName);
  }

  /**
   * Gets the name of the Kiji instance this named table belongs to.
   *
   * @return The name of the kiji instance.
   */
  public String getKijiInstanceName() {
    return mKijiInstanceName;
  }

  /**
   * Gets the name of the Kiji table.
   * A user defined kiji table named "foo" in the default kiji instance will be stored in HBase
   * with the KijiManaged name "kiji.default.table.foo".  This method will return only "foo".
   *
   * @return The name of the kiji table, or null if this is not a user-space Kiji table.
   */
  public String getKijiTableName() {
    return mKijiTableName;
  }

  /**
   * Gets the name of the HBase table that stores the data for this Kiji table.
   *
   * @return The HBase table name as a UTF-8 encoded byte array.
   */
  public byte[] toBytes() {
    return Bytes.toBytes(mHBaseTableName);
  }

  @Override
  public String toString() {
    return mHBaseTableName;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof KijiManagedHBaseTableName)) {
      return false;
    }
    return toString().equals(other.toString());
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }
}
