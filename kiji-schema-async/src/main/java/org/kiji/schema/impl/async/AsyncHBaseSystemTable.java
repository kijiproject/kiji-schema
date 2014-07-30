/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.schema.impl.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.hbase.async.TableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiNotInstalledException;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.SystemTableBackup;
import org.kiji.schema.avro.SystemTableEntry;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.impl.Versions;
import org.kiji.schema.platform.SchemaPlatformBridge;
import org.kiji.schema.util.CloseableIterable;
import org.kiji.schema.util.Debug;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

/**
 * <p>The Kiji system table that is stored in HBase.</p>
 *
 * <p>The system table (a Kiji system table) is a simple key-value store for system-wide
 * properties of a Kiji installation.  There is a single column family "value".  For a
 * key-value property (K,V), the key K is stored as the row key in the HTable,
 * and the value V is stored in the "value:" column.<p>
 */
@ApiAudience.Private
public final class AsyncHBaseSystemTable implements KijiSystemTable {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncHBaseSystemTable.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger("cleanup." + AsyncHBaseSystemTable.class.getName());

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

  /** The HBaseClient of the Kiji instance this system table belongs to. */
  private final HBaseClient mHBClient;

  /** HBase table name */
  private final byte[] mTableName;

  /** States of a SystemTable instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this SystemTable instance. */
  private AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** Used for testing finalize() behavior. */
  private String mConstructorStack = "";

  /**
   * Gets the name of the table for the Kiji system table.
   *
   * @param kijiURI The KijiURI.
   * @param hbClient HBaseClient for this Kiji.
   * @return a byte array for the name of the Kiji system table.
   * @throws IOException on I/O error.
   * @throws KijiNotInstalledException if the Kiji instance does not exist.
   */
  public static byte[] getSystemTableName(
      KijiURI kijiURI,
      HBaseClient hbClient) {
    final byte[] tableName =
        KijiManagedHBaseTableName.getSystemTableName(kijiURI.getInstance()).toBytes();
    try {
      hbClient.ensureTableExists(tableName);
      return tableName;
    } catch (TableNotFoundException tnfe) {
      throw new KijiNotInstalledException(
          String.format("Kiji instance %s is not installed.", kijiURI),
          kijiURI);
    }
  }

/**
 * Wrap an existing HTable connection that is assumed to be the table that stores the
 * Kiji instance properties.
 *
 * @param uri URI of the Kiji instance this table belongs to.
 * @param hbClient An HTable to wrap.
 */
public AsyncHBaseSystemTable(KijiURI uri, HBaseClient hbClient) {
  mURI = uri;
  mHBClient = hbClient;
  mTableName = getSystemTableName(mURI, mHBClient);

  if (CLEANUP_LOG.isDebugEnabled()) {
    mConstructorStack = Debug.getStackTrace();
  }
  final State oldState = mState.getAndSet(State.OPEN);
  Preconditions.checkState(oldState == State.UNINITIALIZED,
      "Cannot open SystemTable instance in state %s.", oldState);
}

/** {@inheritDoc} */
  @Override
  public synchronized ProtocolVersion getDataVersion() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get data version from SystemTable instance in state %s.", state);
    byte[] result = getValue(KEY_DATA_VERSION);
    return result == null ? null : ProtocolVersion.parse(new String(result));
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void setDataVersion(ProtocolVersion version) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot set data version in SystemTable instance in state %s.", state);
    putValue(KEY_DATA_VERSION, Bytes.UTF8(version.toString()));
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
        : ProtocolVersion.parse(new String(result));
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void setSecurityVersion(ProtocolVersion version) throws IOException {
    Preconditions.checkNotNull(version);
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot set security version in SystemTable instance in state %s.", state);
    Kiji.Factory.open(mURI).getSecurityManager().checkCurrentGrantAccess();
    putValue(SECURITY_PROTOCOL_VERSION, Bytes.UTF8(version.toString()));
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
    Preconditions.checkState(
        oldState == State.OPEN,
        "Cannot close KijiSystemTable instance in state %s.", oldState);
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    final State state = mState.get();
    if (state != State.CLOSED) {
      CLEANUP_LOG.warn("Finalizing unclosed Kiji HBaseSystemTable instance {} in state {}.",
          this, state);
      CLEANUP_LOG.debug("Stack when HBaseSystemTable was constructed:\n" + mConstructorStack);
      close();
    }
    super.finalize();
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getValue(String key) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get value from SystemTable instance in state %s.", state);

    final GetRequest get = new GetRequest(
        mTableName,
        Bytes.UTF8(key),
        Bytes.UTF8(VALUE_COLUMN_FAMILY),
        new byte[0]);
    try {
      final ArrayList<KeyValue> results = mHBClient.get(get).join();
      return (results.size() == 0) ? null : results.get(0).value();
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public void putValue(String key, byte[] value) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot put value into SystemTable instance in state %s.", state);
    PutRequest put = new PutRequest(
        mTableName,
        Bytes.UTF8(key),
        Bytes.UTF8(VALUE_COLUMN_FAMILY),
        new byte[0],
        value);
    try {
      mHBClient.put(put).join();
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public CloseableIterable<SimpleEntry<String, byte[]>> getAll() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get all from SystemTable instance in state %s.", state);
    final Scanner scanner = mHBClient.newScanner(mTableName);
    return new AsyncSystemTableIterable(scanner);
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
    defaults.load(AsyncHBaseSystemTable.class.getClassLoader().getResourceAsStream(resource));
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
      putValue(key, Bytes.UTF8(value));
    }
  }

  /**
   * Installs a Kiji system table into a running HBase instance.
   *
   * @param admin The HBase cluster to install into.
   * @param kijiURI The KijiURI.
   * @param conf The Hadoop configuration.
   * @param hbClient HBaseClient for this Kiji.
   * @throws IOException If there is an error.
   */
  public static void install(
      HBaseAdmin admin,
      KijiURI kijiURI,
      Configuration conf,
      HBaseClient hbClient)
      throws IOException {
    install(admin, kijiURI, conf, Collections.<String, String>emptyMap(), hbClient);
  }

  /**
   * Installs a Kiji system table into a running HBase instance.
   *
   * @param admin The HBase cluster to install into.
   * @param kijiURI The KijiURI.
   * @param conf The Hadoop configuration.
   * @param properties The initial system properties to be used in addition to the defaults.
   * @param hbClient HBaseClient for this Kiji.
   * @throws IOException If there is an error.
   */
  public static void install(
      HBaseAdmin admin,
      KijiURI kijiURI,
      Configuration conf,
      Map<String, String> properties,
      HBaseClient hbClient)
      throws IOException {
    // Install the table.
    HTableDescriptor tableDescriptor = new HTableDescriptor(
        KijiManagedHBaseTableName.getSystemTableName(kijiURI.getInstance()).toString());
    HColumnDescriptor columnDescriptor = SchemaPlatformBridge.get()
        .createHColumnDescriptorBuilder(Bytes.UTF8(VALUE_COLUMN_FAMILY))
        .setMaxVersions(1)
        .setCompressionType("none")
        .setInMemory(false)
        .setBlockCacheEnabled(true)
        .setTimeToLive(HConstants.FOREVER)
        .setBloomType("NONE")
        .build();
    tableDescriptor.addFamily(columnDescriptor);
    admin.createTable(tableDescriptor);

    AsyncHBaseSystemTable systemTable = new AsyncHBaseSystemTable(kijiURI, hbClient);
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
    mHBClient.flush();
  }

  /** Private class for providing a CloseableIterable over system table key, value pairs. */
  private static class AsyncSystemTableIterable
      implements CloseableIterable<SimpleEntry<String, byte[]>> {

    /** Underlying source of system table parameters. */
    private Scanner mScanner;

    /** Iterator returned by iterator(). */
    private Iterator<SimpleEntry<String, byte[]>> mIterator;

    /**
     * Create a new AsyncSystemTableIterable across system table properties.
     *
     * @param scanner scanner across the target cells.
     */
    public AsyncSystemTableIterable(Scanner scanner) {
      mIterator = new AsyncSystemTableIterator(scanner);
      mScanner = scanner;
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<SimpleEntry<String, byte[]>> iterator() {
      return mIterator;
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      mScanner.close();
    }

  }

  /** Private calss for providing an Iterator to HBaseSystemTableIterable. */
  private static class AsyncSystemTableIterator
      implements Iterator<SimpleEntry<String, byte[]>> {

    /**
     * Iterator across result scanner results.
     * Used to build next() for AsyncSystemTableIterator
     */
    private Scanner mScanner;
    private KeyValue mNext;

    /**
     * Create an AsyncSystemTableIterator across the KeyValues of a Scanner.
     *
     * @param scanner Scanner to scan over the rows of the SystemTable.
     */
    public AsyncSystemTableIterator(Scanner scanner) {
      mScanner = scanner;
      mScanner.setFamily(Bytes.UTF8(VALUE_COLUMN_FAMILY));
      mScanner.setMaxVersions(1);
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
      return (null != mNext);
    }

    /** {@inheritDoc} */
    @Override
    public SimpleEntry<String, byte[]> next() {
      KeyValue next = mNext;
      if (null == next) {
        throw new NoSuchElementException();
      } else {
        try {
          mNext = mScanner.nextRows(1).join().get(0).get(0);
        } catch (Exception e) {
          if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          } else if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
          } else {
            throw new KijiIOException(e);
          }
        }
        String key = new String(next.key());
        byte[] value = next.value();
        return new SimpleEntry<String, byte[]>(key, value);
      }
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
    return Objects.toStringHelper(AsyncHBaseSystemTable.class)
        .add("uri", mURI)
        .add("state", mState.get())
        .toString();
  }
}
