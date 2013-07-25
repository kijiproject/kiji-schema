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
import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiNotInstalledException;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.SystemTableBackup;
import org.kiji.schema.avro.SystemTableEntry;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.util.CloseableIterable;
import org.kiji.schema.util.Debug;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.schema.util.ResourceUtils;

/**
 * <p>The Kiji system table that is stored in HBase.</p>
 *
 * <p>The system table (a Kiji system table) is a simple key-value store for system-wide
 * properties of a Kiji installation.  There is a single column family "value".  For a
 * key-value property (K,V), the key K is stored as the row key in the HTable,
 * and the value V is stored in the "value:" column.<p>import org.kiji.schema.KijiURI;
 */
@ApiAudience.Private
public class HBaseSystemTable implements KijiSystemTable {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseSystemTable.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger("cleanup." + HBaseSystemTable.class.getName());

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
   * @param kijiURI The KijiURI.
   * @param conf The Hadoop configuration.
   * @param factory HTableInterface factory.
   * @return a new HTableInterface for the Kiji system table.
   * @throws IOException on I/O error.
   *     <p> Throws KijiNotInstalledException if the Kiji instance does not exist. </p>
   */
  public static HTableInterface newSystemTable(
      KijiURI kijiURI,
      Configuration conf,
      HTableInterfaceFactory factory)
      throws IOException {
    final String tableName =
        KijiManagedHBaseTableName.getSystemTableName(kijiURI.getInstance()).toString();
    try {
      return factory.create(conf, tableName);
    } catch (TableNotFoundException tnfe) {
      throw new KijiNotInstalledException(
          String.format("Kiji instance %s is not installed.", kijiURI),
          kijiURI.toString());
    }
  }

  /**
   * Connect to the HBase system table inside a Kiji instance.
   *
   * @param kijiURI The KijiURI.
   * @param conf the Hadoop configuration.
   * @param factory HTableInterface factory.
   * @throws IOException If there is an error.
   *     <p> Throws KijiNotInstalledException if the Kiji instance does not exist. </p>
   */
  public HBaseSystemTable(
      KijiURI kijiURI,
      Configuration conf,
      HTableInterfaceFactory factory)
      throws IOException {
    this(newSystemTable(kijiURI, conf, factory));
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
      mConstructorStack = Debug.getStackTrace();
    }
  }

  /** {@inheritDoc} */
  @Override
  public synchronized ProtocolVersion getDataVersion() throws IOException {
    byte[] result = getValue(KEY_DATA_VERSION);
    return result == null ? null : ProtocolVersion.parse(Bytes.toString(result));
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void setDataVersion(ProtocolVersion version) throws IOException {
    putValue(KEY_DATA_VERSION, Bytes.toBytes(version.toString()));
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

  /** {@inheritDoc} */
  @Override
  public byte[] getValue(String key) throws IOException {
    Get get = new Get(Bytes.toBytes(key));
    get.addColumn(Bytes.toBytes(VALUE_COLUMN_FAMILY), new byte[0]);
    Result result = mTable.get(get);
    return result.getValue(Bytes.toBytes(VALUE_COLUMN_FAMILY), new byte[0]);
  }

  /** {@inheritDoc} */
  @Override
  public void putValue(String key, byte[] value) throws IOException {
    Put put = new Put(Bytes.toBytes(key));
    put.add(Bytes.toBytes(VALUE_COLUMN_FAMILY), new byte[0], value);
    mTable.put(put);
  }

  /** {@inheritDoc} */
  @Override
  public CloseableIterable<SimpleEntry<String, byte[]>> getAll() throws IOException {
    return new HBaseSystemTableIterable(mTable.getScanner(Bytes.toBytes(VALUE_COLUMN_FAMILY)));
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
   * @param kijiURI The KijiURI.
   * @param conf The Hadoop configuration.
   * @param factory HTableInterface factory.
   * @throws IOException If there is an error.
   */
  public static void install(
      HBaseAdmin admin,
      KijiURI kijiURI,
      Configuration conf,
      HTableInterfaceFactory factory)
      throws IOException {
    // Install the table.
    HTableDescriptor tableDescriptor = new HTableDescriptor(
        KijiManagedHBaseTableName.getSystemTableName(kijiURI.getInstance()).toString());
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

    HBaseSystemTable systemTable = new HBaseSystemTable(kijiURI, conf, factory);
    try {
      systemTable.loadDefaults(DEFAULTS_PROPERTIES_FILE);
    } finally {
      ResourceUtils.closeOrLog(systemTable);
    }
  }

  /**
   * Disables and delete the system table from HBase.
   *
   * @param admin The HBase admin object.
   * @param kijiURI The URI for the kiji instance to remove.
   * @throws IOException If there is an error.
   */
  public static void uninstall(HBaseAdmin admin, KijiURI kijiURI)
      throws IOException {
    final String tableName =
        KijiManagedHBaseTableName.getSystemTableName(kijiURI.getInstance()).toString();
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  /** {@inheritDoc} */
  @Override
  public SystemTableBackup toBackup() throws IOException {
    ArrayList<SystemTableEntry> backupEntries = new ArrayList<SystemTableEntry>();
    CloseableIterable<SimpleEntry<String, byte[]>> entries = getAll();
    for (SimpleEntry<String, byte[]> entry : entries) {
      backupEntries.add(SystemTableEntry.newBuilder()
          .setKey(entry.getKey())
          .setValue(ByteBuffer.wrap(entry.getValue()))
          .build());
    }

    return SystemTableBackup.newBuilder().setEntries(backupEntries).build();
  }

  /** {@inheritDoc} */
  @Override
  public void fromBackup(SystemTableBackup backup) throws IOException {
    LOG.info(String.format("Restoring system table from backup with %d entries.",
        backup.getEntries().size()));
    for (SystemTableEntry entry : backup.getEntries()) {
      putValue(entry.getKey(), entry.getValue().array());
    }
    mTable.flushCommits();
  }

  /** Private class for providing a CloseableIterable over system table key, value pairs. */
  private static class HBaseSystemTableIterable
      implements CloseableIterable<SimpleEntry<String, byte[]>> {

    /** Uderlying source of system table parameters. */
    private ResultScanner mResultScanner;

    /** Iterator returned by iterator(). */
    private Iterator<SimpleEntry<String, byte[]>> mIterator;

    /**
     * Create a new HBaseSystemTableIterable across system table properties.
     *
     * @param resultScanner scanner across the target cells.
     */
    public HBaseSystemTableIterable(ResultScanner resultScanner) {
      mIterator = new HBaseSystemTableIterator(resultScanner.iterator());
      mResultScanner = resultScanner;
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<SimpleEntry<String, byte[]>> iterator() {
      return mIterator;
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      mResultScanner.close();
    }
  }

  /** Private calss for providing an Iterator to HBaseSystemTableIterable. */
  private static class HBaseSystemTableIterator
      implements Iterator<SimpleEntry<String, byte[]>> {

    /**
     * Iterator across result scanner results.
     * Used to build next() for HBaseSystemTableIterator
     */
    private Iterator<Result> mResultIterator;

    /**
     * Create an HBaseSystemTableIterator across the results of a ResultScanner.
     *
     * @param resultScannerIterator iterator across the scanned cells.
     */
    public HBaseSystemTableIterator(Iterator<Result> resultScannerIterator) {
      mResultIterator = resultScannerIterator;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
      return mResultIterator.hasNext();
    }

    /** {@inheritDoc} */
    @Override
    public SimpleEntry<String, byte[]> next() {
      Result next = mResultIterator.next();
      String key = Bytes.toString(next.getRow());
      byte[] value = next.getValue(Bytes.toBytes(VALUE_COLUMN_FAMILY), new byte[0]);
      return new SimpleEntry<String, byte[]>(key, value);
    }

    /** {@inheritDoc} */
    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
