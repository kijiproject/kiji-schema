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
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiManagedHBaseTableName;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.TableKeyNotFoundException;

/**
 * <p>The Kiji system table that is stored in HBase.</p>
 *
 * <p>The system table (a Kiji system table) is a simple key-value store for system-wide
 * properties of a Kiji installation.  There is a single column family "value".  For a
 * key-value property (K,V), the key K is stored as the row key in the HTable,
 * and the value V is stored in the "value:" column.<p>
 */
@ApiAudience.Private
public class HBaseSystemTable extends KijiSystemTable {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseSystemTable.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger(HBaseSystemTable.class.getName() + ".Cleanup");

  /** The HBase column family that stores the value of the properties. */
  public static final String VALUE_COLUMN_FAMILY = "value";

  /** The HBase row key that stores the installed Kiji data format version. */
  public static final String KEY_DATA_VERSION = "data-version";

  /**
   * The name of the file that stores the current system table defaults that are loaded
   * at installation time.
   */
  public static final String DEFAULTS_PROPERTIES_FILE =
      "org/kiji/schema/system-default.properties";

  /** The HTable that stores the Kiji instance properties. */
  private final HTableInterface mTable;

  /** Whether the table is open. */
  private boolean mIsOpen;

  /** Used for testing finalize() behavior. */
  private String mConstructorStack = "";

  /**
   * Creates a new HTableInterface for the Kiji system table.
   *
   * @param kijiConf Kiji configuration.
   * @param factory HTableInterface factory.
   * @return a new HTableInterface for the Kiji system table.
   * @throws IOException on I/O error.
   */
  public static HTableInterface newSystemTable(
      KijiConfiguration kijiConf,
      HTableInterfaceFactory factory)
      throws IOException {
    final String tableName =
        KijiManagedHBaseTableName.getSystemTableName(kijiConf.getName()).toString();
    return factory.create(kijiConf.getConf(), tableName);
  }

  /**
   * Connect to the HBase system table inside a Kiji instance.
   *
   * @param kijiConf The Kiji configuration.
   * @param factory HTableInterface factory.
   * @throws IOException If there is an error.
   */
  public HBaseSystemTable(
      KijiConfiguration kijiConf,
      HTableInterfaceFactory factory)
      throws IOException {
    this(newSystemTable(kijiConf, factory));
  }

  /**
   * Wrap an existing HTable connection that is assumed to be the table that stores the
   * Kiji instance properties.
   *
   * @param htable An HTable to wrap.
   */
  public HBaseSystemTable(HTableInterface htable) {
    mTable = htable;
    mIsOpen = true;

    if (CLEANUP_LOG.isDebugEnabled()) {
      try {
        throw new Exception();
      } catch (Exception e) {
        mConstructorStack = StringUtils.stringifyException(e);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public synchronized String getDataVersion() throws IOException {
    byte[] result = getValue(KEY_DATA_VERSION);
    return result == null ? null : Bytes.toString(result);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void setDataVersion(String version) throws IOException {
    putValue(KEY_DATA_VERSION, Bytes.toBytes(version));
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void close() throws IOException {
    if (!mIsOpen) {
      LOG.warn("close() called on a KijiSystemTable that was already closed.");
      return;
    }
    mTable.close();
    mIsOpen = false;
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    if (mIsOpen) {
      CLEANUP_LOG.warn("Closing KijiSystemTable in finalize(). You should close it explicitly");
      CLEANUP_LOG.debug("Stack when HBaseSystemTable was constructed:\n" + mConstructorStack);
      close();
    }
    super.finalize();
  }

  /**
   * Gets the value associated with a property key.
   *
   * @param key The property key to look up.
   * @return The value in the HBase table with the given key, or null if the key doesn't exist.
   * @throws IOException If there is an error.
   */
  public byte[] getValue(String key) throws IOException {
    Get get = new Get(Bytes.toBytes(key));
    get.addColumn(Bytes.toBytes(VALUE_COLUMN_FAMILY), new byte[0]);
    Result result = mTable.get(get);
    if (result == null) {
      throw new TableKeyNotFoundException(String.format(
          "The key '%s' was not found in the system table.", key));
    }
    return result.getValue(Bytes.toBytes(VALUE_COLUMN_FAMILY), new byte[0]);
  }

  /**
   * Sets a value for a property key, which creates it if it doesn't exist.
   *
   * @param key The property key to set.
   * @param value The value of the property.
   * @throws IOException If there is an error.
   */
  public void putValue(String key, byte[] value) throws IOException {
    Put put = new Put(Bytes.toBytes(key));
    put.add(Bytes.toBytes(VALUE_COLUMN_FAMILY), new byte[0], value);
    mTable.put(put);
  }

  /**
   * Load the system table with the key/value pairs from the properties file named by resource.
   *
   * @param resource The name of the properties resource holding the defaults.
   * @throws IOException If there is an error.
   */
  protected void loadDefaults(String resource) throws IOException {
    final Properties defaults = new Properties();
    defaults.load(getClass().getClassLoader().getResourceAsStream(resource));
    for (Map.Entry<Object, Object> item : defaults.entrySet()) {
      final String key = item.getKey().toString();
      final String value = item.getValue().toString();
      putValue(key, Bytes.toBytes(value));
    }
  }

  /**
   * Installs a Kiji system table into a running HBase instance.
   *
   * @param admin The HBase cluster to install into.
   * @param kijiConf The Kiji configuration.
   * @param factory HTableInterface factory.
   * @throws IOException If there is an error.
   */
  public static void install(
      HBaseAdmin admin,
      KijiConfiguration kijiConf,
      HTableInterfaceFactory factory)
      throws IOException {
    // Install the table.
    HTableDescriptor tableDescriptor = new HTableDescriptor(
        KijiManagedHBaseTableName.getSystemTableName(kijiConf.getName()).toString());
    HColumnDescriptor columnDescriptor = new HColumnDescriptor(
        Bytes.toBytes(VALUE_COLUMN_FAMILY),  // family name.
        1,  // max versions
        Compression.Algorithm.NONE.toString(),  // compression
        false,  // in-memory
        true,  // block-cache
        HConstants.FOREVER,  // tts
        HColumnDescriptor.DEFAULT_BLOOMFILTER);
    tableDescriptor.addFamily(columnDescriptor);
    admin.createTable(tableDescriptor);

    HBaseSystemTable systemTable = new HBaseSystemTable(kijiConf, factory);
    try {
      systemTable.loadDefaults(DEFAULTS_PROPERTIES_FILE);
    } finally {
      IOUtils.closeQuietly(systemTable);
    }
  }

  /**
   * Disables and delete the system table from HBase.
   *
   * @param admin The HBase admin object.
   * @param kijiConf The configuration for the kiji instance to remove.
   * @throws IOException If there is an error.
   */
  public static void uninstall(HBaseAdmin admin, KijiConfiguration kijiConf)
      throws IOException {
    final String tableName =
        KijiManagedHBaseTableName.getSystemTableName(kijiConf.getName()).toString();
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }
}
