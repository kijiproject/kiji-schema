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

package org.kiji.schema.impl.hbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiNotInstalledException;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.SystemTableBackup;
import org.kiji.schema.avro.SystemTableEntry;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.impl.HTableInterfaceFactory;
import org.kiji.schema.impl.Versions;
import org.kiji.schema.platform.SchemaPlatformBridge;
import org.kiji.schema.util.CloseableIterable;
import org.kiji.schema.util.DebugResourceTracker;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.schema.util.ResourceUtils;

/**
 * <p>The Kiji system table that is stored in HBase.</p>
 *
 * <p>The system table (a Kiji system table) is a simple key-value store for system-wide
 * properties of a Kiji installation.  There is a single column family "value".  For a
 * key-value property (K,V), the key K is stored as the row key in the HTable,
 * and the value V is stored in the "value:" column.<p>
 */
@ApiAudience.Private
public final class HBaseSystemTable implements KijiSystemTable {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseSystemTable.class);

  /** The HBase column family that stores the value of the properties. */
  public static final String VALUE_COLUMN_FAMILY = "value";

  /** The HBase row key that stores the installed Kiji data format version. */
  public static final String KEY_DATA_VERSION = "data-version";

  /** The HBase row key that stores the Kiji security version. */
  public static final String SECURITY_PROTOCOL_VERSION = "security-version";

  /**
   * The name of the file that stores the current system table defaults that are loaded
   * at installation time.
   */
  public static final String DEFAULTS_PROPERTIES_FILE =
      "org/kiji/schema/system-default.properties";

  /** URI of the Kiji instance this system table belongs to. */
  private final KijiURI mURI;

  /** The HTable that stores the Kiji instance properties. */
  private final HTableInterface mTable;

  /** States of a SystemTable instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this SystemTable instance. */
  private AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /**
   * Creates a new HTableInterface for the Kiji system table.
   *
   * @param kijiURI The KijiURI.
   * @param conf The Hadoop configuration.
   * @param factory HTableInterface factory.
   * @return a new HTableInterface for the Kiji system table.
   * @throws IOException on I/O error.
   * @throws KijiNotInstalledException if the Kiji instance does not exist.
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
          kijiURI);
    }
  }

  /**
   * Connect to the HBase system table inside a Kiji instance.
   *
   * @param kijiURI The KijiURI.
   * @param conf the Hadoop configuration.
   * @param factory HTableInterface factory.
   * @throws IOException If there is an error.
   * @throws KijiNotInstalledException if the Kiji instance does not exist.
   */
  public HBaseSystemTable(
      KijiURI kijiURI,
      Configuration conf,
      HTableInterfaceFactory factory)
      throws IOException {
    this(kijiURI, newSystemTable(kijiURI, conf, factory));
  }

  /**
   * Wrap an existing HTable connection that is assumed to be the table that stores the
   * Kiji instance properties.
   *
   * @param uri URI of the Kiji instance this table belongs to.
   * @param htable An HTable to wrap.
   */
  public HBaseSystemTable(KijiURI uri, HTableInterface htable) {
    mURI = uri;
    mTable = htable;

    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open SystemTable instance in state %s.", oldState);
    DebugResourceTracker.get().registerResource(this);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized ProtocolVersion getDataVersion() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get data version from SystemTable instance in state %s.", state);
    byte[] result = getValue(KEY_DATA_VERSION);
    return result == null ? null : ProtocolVersion.parse(Bytes.toString(result));
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void setDataVersion(ProtocolVersion version) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot set data version in SystemTable instance in state %s.", state);
    putValue(KEY_DATA_VERSION, Bytes.toBytes(version.toString()));
  }

  /** {@inheritDoc} */
  @Override
  public synchronized ProtocolVersion getSecurityVersion() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get security version from SystemTable instance in state %s.", state);
    byte[] result = getValue(SECURITY_PROTOCOL_VERSION);
    return result == null
        ? Versions.UNINSTALLED_SECURITY_VERSION
        : ProtocolVersion.parse(Bytes.toString(result));
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void setSecurityVersion(ProtocolVersion version) throws IOException {
    Preconditions.checkNotNull(version);
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot set security version in SystemTable instance in state %s.", state);
    Kiji.Factory.open(mURI).getSecurityManager().checkCurrentGrantAccess();
    putValue(SECURITY_PROTOCOL_VERSION, Bytes.toBytes(version.toString()));
  }

  /** {@inheritDoc} */
  @Override
  public KijiURI getKijiURI() {
    return mURI;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void close() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close KijiSystemTable instance in state %s.", oldState);
    DebugResourceTracker.get().unregisterResource(this);
    mTable.close();
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getValue(String key) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get value from SystemTable instance in state %s.", state);
    Get get = new Get(Bytes.toBytes(key));
    get.addColumn(Bytes.toBytes(VALUE_COLUMN_FAMILY), new byte[0]);
    Result result = mTable.get(get);
    return result.getValue(Bytes.toBytes(VALUE_COLUMN_FAMILY), new byte[0]);
  }

  /** {@inheritDoc} */
  @Override
  public void putValue(String key, byte[] value) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot put value into SystemTable instance in state %s.", state);
    Put put = new Put(Bytes.toBytes(key));
    put.add(Bytes.toBytes(VALUE_COLUMN_FAMILY), new byte[0], value);
    mTable.put(put);
  }

  /** {@inheritDoc} */
  @Override
  public CloseableIterable<SimpleEntry<String, byte[]>> getAll() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get all from SystemTable instance in state %s.", state);
    return new HBaseSystemTableIterable(mTable.getScanner(Bytes.toBytes(VALUE_COLUMN_FAMILY)));
  }

  /**
   * Loads a map of properties from the properties file named by resource.
   *
   * @param resource The name of the properties resource holding the defaults.
   * @return The properties in the file as a Map.
   * @throws IOException If there is an error.
   */
  public static Map<String, String> loadPropertiesFromFileToMap(String resource)
      throws IOException {
    final Properties defaults = new Properties();
    defaults.load(HBaseSystemTable.class.getClassLoader().getResourceAsStream(resource));
    return Maps.fromProperties(defaults);
  }

  /**
   * Load the system table with the key/value pairs specified in properties.  Default properties are
   * loaded for any not specified.
   *
   * @param properties The properties to load into the system table.
   * @throws IOException If there is an I/O error.
   */
  protected void loadSystemTableProperties(Map<String, String> properties) throws IOException {
    final Map<String, String> defaults = loadPropertiesFromFileToMap(DEFAULTS_PROPERTIES_FILE);
    final Map<String, String> newProperties = Maps.newHashMap(defaults);
    newProperties.putAll(properties);
    for (Map.Entry<String, String> entry : newProperties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
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
    install(admin, kijiURI, conf, Collections.<String, String>emptyMap(), factory);
  }

  /**
   * Installs a Kiji system table into a running HBase instance.
   *
   * @param admin The HBase cluster to install into.
   * @param kijiURI The KijiURI.
   * @param conf The Hadoop configuration.
   * @param properties The initial system properties to be used in addition to the defaults.
   * @param factory HTableInterface factory.
   * @throws IOException If there is an error.
   */
  public static void install(
      HBaseAdmin admin,
      KijiURI kijiURI,
      Configuration conf,
      Map<String, String> properties,
      HTableInterfaceFactory factory)
      throws IOException {
    // Install the table.
    HTableDescriptor tableDescriptor = new HTableDescriptor(
        KijiManagedHBaseTableName.getSystemTableName(kijiURI.getInstance()).toString());
    HColumnDescriptor columnDescriptor = SchemaPlatformBridge.get()
        .createHColumnDescriptorBuilder(Bytes.toBytes(VALUE_COLUMN_FAMILY))
        .setMaxVersions(1)
        .setCompressionType("none")
        .setInMemory(false)
        .setBlockCacheEnabled(true)
        .setTimeToLive(HConstants.FOREVER)
        .setBloomType("NONE")
        .build();
    tableDescriptor.addFamily(columnDescriptor);
    admin.createTable(tableDescriptor);

    HBaseSystemTable systemTable = new HBaseSystemTable(kijiURI, conf, factory);
    try {
      systemTable.loadSystemTableProperties(properties);
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
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot backup SystemTable instance in state %s.", state);
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
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot restore backup to SystemTable instance in state %s.", state);
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

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(HBaseSystemTable.class)
        .add("uri", mURI)
        .add("state", mState.get())
        .toString();
  }
}
