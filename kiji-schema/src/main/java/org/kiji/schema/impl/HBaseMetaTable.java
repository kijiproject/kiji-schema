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
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiTableKeyValueDatabase;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.KeyValueBackupEntry;
import org.kiji.schema.avro.TableBackup;
import org.kiji.schema.avro.TableLayoutBackupEntry;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayoutDatabase;
import org.kiji.schema.layout.impl.HBaseTableLayoutDatabase;

/**
 * An implementation of the KijiMetaTable that uses the 'kiji-meta' HBase table as the backing
 * store.
 */
@ApiAudience.Private
public class HBaseMetaTable extends KijiMetaTable {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseMetaTable.class);
  /** The HBase column family that will store table layout specific metadata. */
  private static final String LAYOUT_COLUMN_FAMILY = "layout";
  /** The HBase column family that will store user defined metadata. */
  private static final String META_COLUMN_FAMILY = "meta";

  /**  The HBase table that stores Kiji metadata. */
  private final HTableInterface mTable;

  /** The layout table that we delegate the work of storing table layout metadata to. */

  private final KijiTableLayoutDatabase mTableLayoutDatabase;
  /** The table we delegate storing per table meta data, in the form of key value pairs.  */
  private final KijiTableKeyValueDatabase mTableKeyValueDatabase;
  // TODO: Make KijiTableLayoutDatabase thread-safe,
  //     so we can call HBaseMetaTable thread-safe, too.

  /**
   * Creates an HTableInterface for the specified table.
   *
   * @param kijiConf Kiji configuration.
   * @param factory HTableInterface factory to use.
   * @return a new HTableInterface for the specified table.
   * @throws IOException on I/O error.
   */
  public static HTableInterface newMetaTable(
      KijiConfiguration kijiConf,
      HTableInterfaceFactory factory)
      throws IOException {
    return factory.create(
        kijiConf.getConf(),
        KijiManagedHBaseTableName.getMetaTableName(kijiConf.getName()).toString());
  }

  /**
   * Create a connection to a Kiji meta table backed by an HTable within HBase.
   *
   * @param kijiConf The Kiji configuration.
   * @param schemaTable The Kiji schema table.
   * @param factory HTableInterface factory.
   * @throws IOException If there is an error.
   */
  public HBaseMetaTable(
      KijiConfiguration kijiConf,
      KijiSchemaTable schemaTable,
      HTableInterfaceFactory factory)
      throws IOException {
    this(newMetaTable(kijiConf, factory), schemaTable);
  }

  /**
   * Create a connection to a Kiji meta table backed by an HTable within HBase.
   *
   * <p>This class takes ownership of the HTable. It will be closed when this instance is
   * closed.</p>
   *
   * @param htable The HTable to use for storing Kiji meta data.
   * @param schemaTable The Kiji schema table.
   * @throws IOException If there is an error.
   */
  public HBaseMetaTable(HTableInterface htable, KijiSchemaTable schemaTable) throws IOException {
    this(htable,
        new HBaseTableLayoutDatabase(htable, LAYOUT_COLUMN_FAMILY, schemaTable),
        new HBaseTableKeyValueDatabase(htable, META_COLUMN_FAMILY));
  }

  /**
   * Create a connection to a Kiji meta table backed by an HTable within HBase.
   *
   * <p>This class takes ownership of the HTable. It will be closed when this instance is
   * closed.</p>
   *
   * @param htable The HTable to use for storing Kiji meta data.
   * @param tableLayoutDatabase A database of table layouts to delegate layout storage to.
   * @param tableKeyValueDatabase A database of key-value pairs to delegate metadata storage to.
   */
  public HBaseMetaTable(HTableInterface htable, KijiTableLayoutDatabase tableLayoutDatabase,
    KijiTableKeyValueDatabase tableKeyValueDatabase) {
    mTable = htable;
    mTableLayoutDatabase = tableLayoutDatabase;
    mTableKeyValueDatabase = tableKeyValueDatabase;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void deleteTable(String table) throws IOException {
    mTableLayoutDatabase.removeAllTableLayoutVersions(table);
    mTableKeyValueDatabase.removeAllValues(table);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized KijiTableLayout updateTableLayout(String table, TableLayoutDesc layoutUpdate)
    throws IOException {
    return mTableLayoutDatabase.updateTableLayout(table, layoutUpdate);
  }
  /** {@inheritDoc} */
  @Override
  public synchronized KijiTableLayout getTableLayout(String table) throws IOException {
    return mTableLayoutDatabase.getTableLayout(table);
  }
  /** {@inheritDoc} */
  @Override
  public synchronized List<KijiTableLayout> getTableLayoutVersions(String table, int numVersions)
    throws IOException {
    return mTableLayoutDatabase.getTableLayoutVersions(table, numVersions);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized NavigableMap<Long, KijiTableLayout> getTimedTableLayoutVersions(String table,
    int numVersions) throws IOException {
    return mTableLayoutDatabase.getTimedTableLayoutVersions(table, numVersions);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void removeAllTableLayoutVersions(String table) throws IOException {
    mTableLayoutDatabase.removeAllTableLayoutVersions(table);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void removeRecentTableLayoutVersions(String table, int numVersions)
    throws IOException {
    mTableLayoutDatabase.removeRecentTableLayoutVersions(table, numVersions);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized List<String> listTables() throws IOException {
    return mTableLayoutDatabase.listTables();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void close() throws IOException {
    mTable.close();
    super.close();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized byte[] getValue(String table, String key) throws IOException {
    return mTableKeyValueDatabase.getValue(table, key);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized KijiTableKeyValueDatabase putValue(String table, String key, byte[] value)
    throws IOException {
    return mTableKeyValueDatabase.putValue(table, key, value);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void removeValues(String table, String key) throws IOException {
    mTableKeyValueDatabase.removeValues(table, key);
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> tableSet() throws IOException {
    return mTableKeyValueDatabase.tableSet();
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> keySet(String table) throws IOException {
    return mTableKeyValueDatabase.keySet(table);
  }

  /** {@inheritDoc} */
  @Override
  public void removeAllValues(String table) throws IOException {
    mTableKeyValueDatabase.removeAllValues(table);
  }

  /**
   * Install the meta table into a Kiji instance.
   *
   * @param admin The HBase Admin interface for the HBase cluster to install into.
   * @param uri The uri of the Kiji instance to install.
   * @throws IOException If there is an error.
   */
  public static void install(HBaseAdmin admin, KijiURI uri)
    throws IOException {
    HTableDescriptor tableDescriptor = new HTableDescriptor(
      KijiManagedHBaseTableName.getMetaTableName(uri.getInstance()).toString());
    tableDescriptor.addFamily(
      HBaseTableLayoutDatabase.getHColumnDescriptor(LAYOUT_COLUMN_FAMILY));
    tableDescriptor.addFamily(
      HBaseTableLayoutDatabase.getHColumnDescriptor(META_COLUMN_FAMILY));
    admin.createTable(tableDescriptor);
  }

  /**
   * Removes the meta table from HBase.
   *
   * @param admin The HBase admin object.
   * @param uri The uri of the Kiji instance to uninstall.
   * @throws IOException If there is an error.
   */
  public static void uninstall(HBaseAdmin admin, KijiURI uri)
    throws IOException {
    String tableName = KijiManagedHBaseTableName.getMetaTableName(uri.getInstance()).toString();
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, TableBackup> toBackup() throws IOException {
    Map<String, TableBackup> metadataBackup = new HashMap<String, TableBackup>();
    List<String> tables = listTables();
    for (String table : tables) {
      List<TableLayoutBackupEntry> layouts = mTableLayoutDatabase.layoutsToBackup(table);
      List<KeyValueBackupEntry> keyValues = mTableKeyValueDatabase.keyValuesToBackup(table);
      final TableBackup tableBackup = TableBackup.newBuilder()
          .setName(table)
          .setLayouts(layouts)
          .setKeyValues(keyValues)
          .build();
      metadataBackup.put(table, tableBackup);
    }
    return metadataBackup;
  }

  /** {@inheritDoc} */
  @Override
  public void fromBackup(Map<String, TableBackup> backup) throws IOException {
    LOG.info(String.format("Restoring meta table from backup with %d entries.",
        backup.size()));
    for (Map.Entry<String, TableBackup> tableEntry: backup.entrySet()) {
      final String tableName = tableEntry.getKey();
      final TableBackup tableBackup = tableEntry.getValue();
      Preconditions.checkState(tableName.equals(tableBackup.getName()), String.format(
          "Inconsistent table backup: entry '%s' does not match table name '%s'.",
          tableName, tableBackup.getName()));
      layoutsFromBackup(tableName, tableBackup.getLayouts());
      keyValuesFromBackup(tableName, tableBackup.getKeyValues());
    }
    mTable.flushCommits();
    LOG.info("Flushing commits to table '{}'", Bytes.toString(mTable.getTableName()));
  }

  /** {@inheritDoc} */
  @Override
  public List<TableLayoutBackupEntry> layoutsToBackup(String table) throws IOException {
    return mTableLayoutDatabase.layoutsToBackup(table);
  }

  /** {@inheritDoc} */
  @Override
  public List<byte[]> getValues(String table, String key, int numVersions) throws IOException {
    return mTableKeyValueDatabase.getValues(table, key, numVersions);
  }

  /** {@inheritDoc} */
  @Override
  public NavigableMap<Long, byte[]> getTimedValues(String table, String key, int numVersions)
    throws IOException {
    return mTableKeyValueDatabase.getTimedValues(table, key, numVersions);
  }

  /** {@inheritDoc} */
  @Override
  public List<KeyValueBackupEntry> keyValuesToBackup(String table) throws IOException {
    return mTableKeyValueDatabase.keyValuesToBackup(table);
  }

  /** {@inheritDoc} */
  @Override
  public void keyValuesFromBackup(String table, List<KeyValueBackupEntry> tableBackup) throws
      IOException {
    mTableKeyValueDatabase.keyValuesFromBackup(table, tableBackup);
  }

  @Override
  public void layoutsFromBackup(String tableName, List<TableLayoutBackupEntry> tableBackup) throws
      IOException {
    mTableLayoutDatabase.layoutsFromBackup(tableName, tableBackup);
  }

}
