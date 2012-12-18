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

import static org.kiji.schema.util.ByteStreamArray.longToVarInt64;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiManagedHBaseTableName;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.avro.MD5Hash;
import org.kiji.schema.avro.MetadataBackup;
import org.kiji.schema.avro.SchemaTableEntry;
import org.kiji.schema.util.ByteStreamArray;
import org.kiji.schema.util.ByteStreamArray.EncodingException;
import org.kiji.schema.util.BytesKey;
import org.kiji.schema.util.Hasher;
import org.kiji.schema.util.Lock;
import org.kiji.schema.util.LockFactory;

/**
 * <p>
 * Mapping between schema IDs, hashes and Avro schema objects.
 * This class is thread-safe.
 * </p>
 *
 * <p>
 * Schemas are stored in two tables with a single column family named "schema" and that contains
 * SchemaTableEntry records. One table is indexed by schema hashes (128-bit MD5 hashes of the
 * schema JSON representation). Other table is indexed by schema IDs (integers >= 0).
 *
 * There may be multiple schema IDs for a single schema.
 * </p>
 */
@ApiAudience.Private
public class HBaseSchemaTable extends KijiSchemaTable {
  /** ZooKeeper watcher that simply discards events. */
  @ApiAudience.Private
  private static class ZooKeeperNoopWatcher implements Watcher {
    /** {@inheritDoc} */
    @Override
    public void process(WatchedEvent event) {
      // Ignore
    }
  }

  /** ZooKeeper watcher that simply discards events. */
  private static final Watcher ZOOKEEPER_NOOP_WATCHER = new ZooKeeperNoopWatcher();

  private static final Logger LOG = LoggerFactory.getLogger(HBaseSchemaTable.class);

  /** The column family in HBase used to store schema entries. */
  public static final String SCHEMA_COLUMN_FAMILY = "schema";
  private static final byte[] SCHEMA_COLUMN_FAMILY_BYTES = Bytes.toBytes(SCHEMA_COLUMN_FAMILY);

  /** The column qualifier in HBase used to store schema entries. */
  public static final String SCHEMA_COLUMN_QUALIFIER = "";
  private static final byte[] SCHEMA_COLUMN_QUALIFIER_BYTES =
      Bytes.toBytes(SCHEMA_COLUMN_QUALIFIER);

  /** Schema IDs are generated using a counter. The counter is stored in the schema ID table. */
  public static final String SCHEMA_COUNTER_ROW_NAME = "counter";
  private static final byte[] SCHEMA_COUNTER_ROW_NAME_BYTES =
      Bytes.toBytes(SCHEMA_COUNTER_ROW_NAME);

  /** HTable used to map schema hash to schema entries. */
  private final HTableInterface mSchemaHashTable;

  /** HTable used to map schema IDs to schema entries. */
  private final HTableInterface mSchemaIdTable;

  /** ZooKeeper lock for the kiji instance schema table. */
  private final Lock mZKLock;

  /** Maps schema MD5 hashes to schema entries. */
  private final Map<BytesKey, SchemaEntry> mSchemaHashMap = new HashMap<BytesKey, SchemaEntry>();

  /** Maps schema IDs to schema entries. */
  private final Map<Long, SchemaEntry> mSchemaIdMap = new HashMap<Long, SchemaEntry>();

  /** Whether this schema table is open. */
  private boolean mIsOpen = false;

  /** Used for testing finalize() behavior. */
  private String mConstructorStack = "";

  /**
   * Creates an HTable handle to the schema V5 table.
   *
   * @param conf Kiji/HBase configuration to open the table.
   * @param factory HTableInterface factory.
   * @return a new interface for the table storing the schemas up until data layout v5.
   * @throws IOException on I/O error.
   */
  public static HTableInterface newSchemaV5Table(
      KijiConfiguration conf,
      HTableInterfaceFactory factory)
      throws IOException {
    return factory.create(conf.getConf(),
        KijiManagedHBaseTableName.getSchemaV5TableName(conf.getName()).toString());
  }

  /**
   * Creates an HTable handle to the schema hash table.
   *
   * @param conf Kiji/HBase configuration to open the table.
   * @param factory HTableInterface factory.
   * @return a new interface for the table storing the mapping from schema hash to schema entry.
   * @throws IOException on I/O error.
   */
  public static HTableInterface newSchemaHashTable(
      KijiConfiguration conf,
      HTableInterfaceFactory factory)
      throws IOException {
    return factory.create(conf.getConf(),
        KijiManagedHBaseTableName.getSchemaHashTableName(conf.getName()).toString());
  }

  /**
   * Creates an HTable handle to the schema ID table.
   *
   * @param conf Kiji/HBase configuration to open the table.
   * @param factory HTableInterface factory.
   * @return a new interface for the table storing the mapping from schema ID to schema entry.
   * @throws IOException on I/O error.
   */
  public static HTableInterface newSchemaIdTable(
      KijiConfiguration conf,
      HTableInterfaceFactory factory)
      throws IOException {
    return factory.create(conf.getConf(),
        KijiManagedHBaseTableName.getSchemaIdTableName(conf.getName()).toString());
  }

  /**
   * Creates a ZooKeeper schema lock object for the specified Kiji instance.
   *
   * @param conf Kiji instance configuration.
   * @return ZooKeeper lock protecting the schema table for the specified Kiji instance.
   * @throws IOException on I/O error.
   */
  public static Lock newZooKeeperLock(KijiConfiguration conf) throws IOException {
    final String zkQuorum = conf.getConf().get("hbase.zookeeper.quorum");
    final String zkClientPort = conf.getConf().get("hbase.zookeeper.property.clientPort");
    final String zkConnStr = String.format("%s:%s", zkQuorum, zkClientPort);
    final ZooKeeper zkClient = new ZooKeeper(zkConnStr, 60000, ZOOKEEPER_NOOP_WATCHER);
    final LockFactory zkLockFactory = new ZooKeeperLockFactory(zkClient);
    final File lockPath = new File("/kiji", conf.getName());
    return zkLockFactory.create(lockPath.getPath());
  }

  /** Avro decoder factory. */
  private static final DecoderFactory DECODER_FACTORY = DecoderFactory.get();

  /** Avro encoder factory. */
  private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();

  /** Avro reader for a schema entry. */
  private static final DatumReader<SchemaTableEntry> SCHEMA_ENTRY_READER =
      new SpecificDatumReader<SchemaTableEntry>(SchemaTableEntry.SCHEMA$);

  /** Avro writer for a schema entry. */
  private static final DatumWriter<SchemaTableEntry> SCHEMA_ENTRY_WRITER =
      new SpecificDatumWriter<SchemaTableEntry>(SchemaTableEntry.SCHEMA$);

  /**
   * Decodes a binary-encoded Avro schema entry.
   *
   * @param bytes Binary-encoded Avro schema entry.
   * @return Decoded Avro schema entry.
   * @throws IOException on I/O error.
   */
  public static SchemaTableEntry decodeSchemaEntry(final byte[] bytes) throws IOException {
    final SchemaTableEntry entry = new SchemaTableEntry();
    final Decoder decoder =
        DECODER_FACTORY.directBinaryDecoder(new ByteArrayInputStream(bytes), null);
    return SCHEMA_ENTRY_READER.read(entry, decoder);
  }

  /**
   * Encodes an Avro schema entry into binary.
   *
   * @param avroEntry Avro schema entry to encode.
   * @return Binary-encoded Avro schema entry.
   * @throws IOException on I/O error.
   */
  public static byte[] encodeSchemaEntry(final SchemaTableEntry avroEntry) throws IOException {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream(4096);
    final Encoder encoder = ENCODER_FACTORY.directBinaryEncoder(bytes, null);
    SCHEMA_ENTRY_WRITER.write(avroEntry, encoder);
    return bytes.toByteArray();
  }

  /**
   * Open a connection to the HBase schema table for a Kiji instance.
   *
   * @param kijiConf The kiji configuration.
   * @param tableFactory HTableInterface factory.
   * @throws IOException on I/O error.
   */
  public HBaseSchemaTable(
      KijiConfiguration kijiConf,
      HTableInterfaceFactory tableFactory)
      throws IOException {
    this(newSchemaHashTable(kijiConf, tableFactory),
        newSchemaIdTable(kijiConf, tableFactory),
        newZooKeeperLock(kijiConf));
  }

  /**
   * Wrap an existing HBase table assumed to be where the schema data is stored.
   *
   * @param hashTable The HTable that maps schema hashes to schema entries.
   * @param idTable The HTable that maps schema IDs to schema entries.
   * @param zkLock ZooKeeper lock protecting the schema tables.
   * @throws IOException on I/O error.
   */
  public HBaseSchemaTable(HTableInterface hashTable, HTableInterface idTable, Lock zkLock)
      throws IOException {
    mSchemaHashTable = Preconditions.checkNotNull(hashTable);
    mSchemaIdTable = Preconditions.checkNotNull(idTable);
    mZKLock = Preconditions.checkNotNull(zkLock);

    mIsOpen = true;

    if (LOG.isDebugEnabled()) {
      try {
        throw new Exception();
      } catch (Exception e) {
        mConstructorStack = StringUtils.stringifyException(e);
      }
    }
  }

  /**
   * Looks up a schema entry given an Avro schema object.
   *
   * Looks first in-memory. If the schema is not known in-memory, looks in the HTables.
   *
   * @param schema Avro schema to look up.
   * @return Either the pre-existing entry for the specified schema, or a newly created entry.
   * @throws IOException on I/O error.
   */
  private synchronized SchemaEntry getOrCreateSchemaEntry(final Schema schema) throws IOException {
    Preconditions.checkState(mIsOpen, "Schema tables are closed");

    final BytesKey schemaHash = getSchemaHash(schema);
    final SchemaEntry knownEntry = getSchemaEntry(schemaHash);
    if (knownEntry != null) {
      return knownEntry;
    }

    // Schema is unknown, both in-memory and in-table.
    // Allocate a new schema ID and write it down to the tables:
    return storeInMemory(registerNewSchemaInTable(schema, schemaHash));
  }

  /** {@inheritDoc} */
  @Override
  public long getOrCreateSchemaId(final Schema schema) throws IOException {
    return getOrCreateSchemaEntry(schema).getId();
  }

  /** {@inheritDoc} */
  @Override
  public BytesKey getOrCreateSchemaHash(final Schema schema) throws IOException {
    return getOrCreateSchemaEntry(schema).getHash();
  }

  /**
   * Registers a new schema into the schema tables.
   *
   * The following things happen atomically, while holding a lock on the counter row:
   *   <li> look up the schema from the hash table, returning the entry if it is found; </li>
   *   <li> allocate a new unique ID for the schema (by incrementing the schema counter); </li>
   *   <li> write the new schema entry to the hash table and the ID table. </li>
   *
   * @param schema Avro schema to register
   * @param schemaHash hash of the schema
   * @return Fully populated SchemaEntry
   * @throws IOException on I/O error.
   */
  private SchemaEntry registerNewSchemaInTable(final Schema schema, final BytesKey schemaHash)
      throws IOException {
    mZKLock.lock();
    try {
      final SchemaTableEntry existingAvroEntry = loadFromHashTable(schemaHash);
      if (existingAvroEntry != null) {
        return fromAvroEntry(existingAvroEntry);
      }

      // Here we know the schema is unknown from the schema tables and no other process can
      // update the schema table.
      final long schemaId = mSchemaIdTable.incrementColumnValue(SCHEMA_COUNTER_ROW_NAME_BYTES,
          SCHEMA_COLUMN_FAMILY_BYTES, SCHEMA_COLUMN_QUALIFIER_BYTES, 1) - 1;

      final SchemaEntry entry = new SchemaEntry(schemaId, schemaHash, schema);
      storeInTable(toAvroEntry(entry));
      return entry;

    } finally {
      mZKLock.unlock();
    }
  }


  /**
   * Writes the given schema entry to the ID and hash tables.
   *
   * This is not protected from concurrent writes. Caller must ensure consistency.
   *
   * @param avroEntry Schema entry to write.
   * @throws IOException on I/O error.
   */
  private void storeInTable(final SchemaTableEntry avroEntry)
      throws IOException {
    storeInTable(avroEntry, HConstants.LATEST_TIMESTAMP, true);
  }

  /**
   * Writes the given schema entry to the ID and hash tables.
   *
   * This is not protected from concurrent writes. Caller must ensure consistency.
   *
   * @param avroEntry Schema entry to write.
   * @param timestamp Write entries with this timestamp.
   * @param flush Whether to flush tables synchronously.
   * @throws IOException on I/O error.
   */
  private void storeInTable(final SchemaTableEntry avroEntry, long timestamp, boolean flush)
      throws IOException {
    final byte[] entryBytes = encodeSchemaEntry(avroEntry);

    // Writes the ID mapping first: if the hash table write fails, we just lost one schema ID.
    // The hash table write must not happen before the ID table write has been persisted.
    // Otherwise, another client may see the hash entry, write cells with the schema ID that cannot
    // be decoded (since the ID mapping has not been written yet).

    final Put putId = new Put(longToVarInt64(avroEntry.getId()))
        .add(SCHEMA_COLUMN_FAMILY_BYTES, SCHEMA_COLUMN_QUALIFIER_BYTES, timestamp, entryBytes);
    mSchemaIdTable.put(putId);
    if (flush) {
      mSchemaIdTable.flushCommits();
    }

    final Put putHash = new Put(avroEntry.getHash().bytes())
        .add(SCHEMA_COLUMN_FAMILY_BYTES, SCHEMA_COLUMN_QUALIFIER_BYTES, timestamp, entryBytes);
    mSchemaHashTable.put(putHash);
    if (flush) {
      mSchemaHashTable.flushCommits();
    }
  }

  /**
   * Fetches a schema entry from the tables given a schema ID.
   *
   * @param schemaId schema ID
   * @return Avro schema entry, or null if the schema ID does not exist in the table
   * @throws IOException on I/O error.
   */
  private SchemaTableEntry loadFromIdTable(long schemaId) throws IOException {
    final Get get = new Get(longToVarInt64(schemaId));
    final Result result = mSchemaIdTable.get(get);
    return result.isEmpty() ? null : decodeSchemaEntry(result.value());
  }

  /**
   * Fetches a schema entry from the tables given a schema hash.
   *
   * @param schemaHash schema hash
   * @return Avro schema entry, or null if the schema hash does not exist in the table
   * @throws IOException on I/O error.
   */
  private SchemaTableEntry loadFromHashTable(BytesKey schemaHash) throws IOException {
    final Get get = new Get(schemaHash.getBytes());
    final Result result = mSchemaHashTable.get(get);
    return result.isEmpty() ? null : decodeSchemaEntry(result.value());
  }

  /**
   * Converts an Avro SchemaTableEntry into a SchemaEntry.
   *
   * @param avroEntry Avro SchemaTableEntry
   * @return an equivalent SchemaEntry
   */
  public static SchemaEntry fromAvroEntry(final SchemaTableEntry avroEntry) {
    final String schemaJson = avroEntry.getAvroSchema();
    final Schema schema = new Schema.Parser().parse(schemaJson);
    return new SchemaEntry(avroEntry.getId(), new BytesKey(avroEntry.getHash().bytes()), schema);
  }

  /**
   * Converts a SchemaEntry into an Avro SchemaTableEntry.
   *
   * @param entry a SchemaEntry.
   * @return an equivalent Avro SchemaTableEntry.
   */
  public static SchemaTableEntry toAvroEntry(final SchemaEntry entry) {
    return SchemaTableEntry.newBuilder().setId(entry.getId())
        .setHash(new MD5Hash(entry.getHash().getBytes()))
        .setAvroSchema(entry.getSchema().toString()).build();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized Schema getSchema(long schemaId) throws IOException {
    final SchemaEntry entry = getSchemaEntry(schemaId);
    return (entry == null) ? null : entry.getSchema();
  }

  /**
   * Looks up a schema entry by ID, first in-memory, or then in-table.
   *
   * Updates the in-memory maps if the schema entry is found in the tables.
   *
   * @param schemaId Schema ID
   * @return Corresponding SchemaEntry, or null if the schema ID does not exist.
   * @throws IOException on I/O error.
   */
  private synchronized SchemaEntry getSchemaEntry(long schemaId) throws IOException {
    Preconditions.checkState(mIsOpen, "Schema table is closed");

    final SchemaEntry existingEntry = mSchemaIdMap.get(schemaId);
    if (existingEntry != null) {
      return existingEntry;
    }

    // On a lookup miss from the local schema cache, check to see if we can get the schema
    // from the original HBase table, cache it locally, and return it.
    final SchemaTableEntry avroEntry = loadFromIdTable(schemaId);
    if (avroEntry == null) {
      return null;
    }
    return storeInMemory(avroEntry);
  }

  /** {@inheritDoc} */
  @Override
  public Schema getSchema(BytesKey schemaHash) throws IOException {
    final SchemaEntry entry = getSchemaEntry(schemaHash);
    return (entry == null) ? null : entry.getSchema();
  }

  /**
   * Looks up a schema entry by hash, first in-memory, or then in-table.
   *
   * Updates the in-memory maps if the schema entry is found in the tables.
   *
   * @param schemaHash Schema hash
   * @return Corresponding SchemaEntry, or null if the schema hash does not exist.
   * @throws IOException on I/O error.
   */
  private synchronized SchemaEntry getSchemaEntry(BytesKey schemaHash) throws IOException {
    Preconditions.checkState(mIsOpen, "Schema table is closed");

    final SchemaEntry existingEntry = mSchemaHashMap.get(schemaHash);
    if (existingEntry != null) {
      return existingEntry;
    }

    // On a lookup miss from the local schema cache, check to see if we can get the schema
    // from the original HBase table, cache it locally, and return it.
    final SchemaTableEntry avroEntry = loadFromHashTable(schemaHash);
    if (null == avroEntry) {
      return null;
    }
    final SchemaEntry entry = storeInMemory(avroEntry);
    Preconditions.checkState(schemaHash.equals(entry.getHash()));
    return entry;
  }

  /**
   * Stores the specified schema entry in memory.
   *
   * External synchronization required.
   *
   * @param avroEntry Avro schema entry.
   * @return the SchemaEntry stored in memory.
   */
  private SchemaEntry storeInMemory(final SchemaTableEntry avroEntry) {
    return storeInMemory(fromAvroEntry(avroEntry));
  }

  /**
   * Stores the specified schema entry in memory.
   *
   * External synchronization required.
   *
   * @param entry the SchemaEntry to store in memory.
   * @return the SchemaEntry stored in memory.
   */
  private SchemaEntry storeInMemory(final SchemaEntry entry) {
    // Replacing an hash-mapped entry may happen, if two different IDs were assigned to one schema.
    final SchemaEntry oldHashEntry = mSchemaHashMap.put(entry.getHash(), entry);
    if (oldHashEntry != null) {
      LOG.info(String.format(
          "Replacing hash-mapped schema entry:%n%s%nwith:%n%s", oldHashEntry, entry));
    }

    // Replacing an ID-mapped entry should never happen:
    // IDs are associated to at most one schema/hash.
    final SchemaEntry oldIdEntry = mSchemaIdMap.put(entry.getId(), entry);
    if (oldIdEntry != null) {
      throw new AssertionError(String.format(
          "Attempting to replace ID-mapped schema entry:%n%s%nwith:%n%s", oldIdEntry, entry));
    }
    return entry;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void flush() throws IOException {
    Preconditions.checkState(mIsOpen, "Schema table are closed");
    mSchemaIdTable.flushCommits();
    mSchemaHashTable.flushCommits();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void close() throws IOException {
    if (!mIsOpen) {
      LOG.warn("close() was called on a schema table that was already closed.");
      return;
    }
    flush();
    mSchemaHashTable.close();
    mSchemaIdTable.close();
    mZKLock.close();
    mIsOpen = false;
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    if (mIsOpen) {
      LOG.debug("Stack: " + mConstructorStack);
      LOG.warn("Closing schema table from finalize(). You should close it explicitly.");
      close();
    }
    super.finalize();
  }

  /**
   * Install the schema table into a Kiji instance.
   *
   * @param admin The HBase Admin interface for the HBase cluster to install into.
   * @param kijiConf The Kiji configuration.
   * @param tableFactory HTableInterface factory.
   * @throws IOException If there is an error.
   */
  public static void install(
      HBaseAdmin admin,
      KijiConfiguration kijiConf,
      HTableInterfaceFactory tableFactory)
      throws IOException {
    // Keep all versions of schema entries:
    //  - entries of the ID table should never be written more than once.
    //  - entries of the hash table could be written more than once:
    //      - with different schema IDs in some rare cases, for example when a client crashes
    //        while writing an entry.
    //      - with different schemas on MD5 hash collisions.
    final int maxVersions = Integer.MAX_VALUE;

    final HTableDescriptor hashTableDescriptor = new HTableDescriptor(
        KijiManagedHBaseTableName.getSchemaHashTableName(kijiConf.getName()).toString());
    final HColumnDescriptor hashColumnDescriptor = new HColumnDescriptor(
        SCHEMA_COLUMN_FAMILY_BYTES, // family name.
        maxVersions, // max versions
        Compression.Algorithm.NONE.toString(), // compression
        false, // in-memory
        true, // block-cache
        HConstants.FOREVER, // tts
        HColumnDescriptor.DEFAULT_BLOOMFILTER);
    hashTableDescriptor.addFamily(hashColumnDescriptor);
    admin.createTable(hashTableDescriptor);

    final HTableDescriptor idTableDescriptor = new HTableDescriptor(
        KijiManagedHBaseTableName.getSchemaIdTableName(kijiConf.getName()).toString());
    final HColumnDescriptor idColumnDescriptor = new HColumnDescriptor(
        SCHEMA_COLUMN_FAMILY_BYTES, // family name.
        maxVersions, // max versions
        Compression.Algorithm.NONE.toString(), // compression
        false, // in-memory
        true, // block-cache
        HConstants.FOREVER, // tts
        HColumnDescriptor.DEFAULT_BLOOMFILTER);
    idTableDescriptor.addFamily(idColumnDescriptor);
    admin.createTable(idTableDescriptor);

    final HBaseSchemaTable schemaTable = new HBaseSchemaTable(
        newSchemaHashTable(kijiConf, tableFactory),
        newSchemaIdTable(kijiConf, tableFactory),
        newZooKeeperLock(kijiConf));
    try {
      schemaTable.setSchemaIdCounter(0L);
      schemaTable.registerPrimitiveSchemas();
    } finally {
      schemaTable.close();
    }
  }

  /**
   * Sets the schema ID counter.
   *
   * @param counter New value for the schema ID counter.
   * @throws IOException on I/O error.
   */
  private void setSchemaIdCounter(long counter) throws IOException {
    mSchemaIdTable.put(new Put(SCHEMA_COUNTER_ROW_NAME_BYTES)
        .add(SCHEMA_COLUMN_FAMILY_BYTES, SCHEMA_COLUMN_QUALIFIER_BYTES, Bytes.toBytes(counter)));
  }

  /**
   * Deletes an HBase table.
   *
   * @param admin HBase admin client.
   * @param tableName Name of the table to delete.
   */
  private static void deleteTable(HBaseAdmin admin, String tableName) {
    try {
      if (admin.tableExists(tableName)) {
        if (admin.isTableEnabled(tableName)) {
          admin.disableTable(tableName);
        }
        admin.deleteTable(tableName);
      }
    } catch (IOException ioe) {
      LOG.error(String.format("Unable to delete table '%s': %s", tableName, ioe.toString()));
    }
  }

  /**
   * Disables and removes the schema table from HBase.
   *
   * @param admin The HBase Admin object.
   * @param kijiConf The configuration for the kiji instance to remove.
   * @throws IOException If there is an error.
   */
  public static void uninstall(HBaseAdmin admin, KijiConfiguration kijiConf)
      throws IOException {
    final String hashTableName =
        KijiManagedHBaseTableName.getSchemaHashTableName(kijiConf.getName()).toString();
    deleteTable(admin, hashTableName);

    final String idTableName =
        KijiManagedHBaseTableName.getSchemaIdTableName(kijiConf.getName()).toString();
    deleteTable(admin, idTableName);

    final String v5TableName =
        KijiManagedHBaseTableName.getSchemaV5TableName(kijiConf.getName()).toString();
    deleteTable(admin, v5TableName);
  }

  /** {@inheritDoc} */
  @Override
  public List<SchemaTableEntry> toBackup() throws IOException {
    Preconditions.checkState(mIsOpen, "Schema tables are closed");
    mZKLock.lock();
    List<SchemaTableEntry> entries = Lists.newArrayList();
    try {
      /** Entries from the schema hash table. */
      final Set<SchemaEntry> hashTableEntries = loadSchemaHashTable(mSchemaHashTable);
      if (!checkConsistency(hashTableEntries)) {
        LOG.error("Schema hash table is inconsistent");
      }

      /** Entries from the schema ID table. */
      final Set<SchemaEntry> idTableEntries = loadSchemaIdTable(mSchemaIdTable);
      if (!checkConsistency(idTableEntries)) {
        LOG.error("Schema hash table is inconsistent");
      }

      final Set<SchemaEntry> mergedEntries = new HashSet<SchemaEntry>(hashTableEntries);
      mergedEntries.addAll(idTableEntries);
      if (!checkConsistency(mergedEntries)) {
        LOG.error("Merged schema hash and ID tables are inconsistent");
      }
      for (SchemaEntry entry : mergedEntries) {
        entries.add(toAvroEntry(entry));
      }
    } finally {
      mZKLock.unlock();
    }
    return entries;
  }

  /** {@inheritDoc} */
  @Override
  public void restoreFromBackup(final MetadataBackup backup) throws IOException {
    Preconditions.checkState(mIsOpen, "Schema tables are closed");
    mZKLock.lock();
    try {
      /** Entries from the schema hash table. */
      final Set<SchemaEntry> hashTableEntries = loadSchemaHashTable(mSchemaHashTable);

      /** Entries from the schema ID table. */
      final Set<SchemaEntry> idTableEntries = loadSchemaIdTable(mSchemaIdTable);

      final Set<SchemaEntry> mergedEntries = new HashSet<SchemaEntry>(hashTableEntries);
      mergedEntries.addAll(idTableEntries);
      if (!checkConsistency(mergedEntries)) {
        LOG.error("Merged schema hash and ID tables are inconsistent");
      }

      final Set<SchemaEntry> backupEntries =
          new HashSet<SchemaEntry>(backup.getSchemaTable().size());
      for (SchemaTableEntry avroEntry : backup.getSchemaTable()) {
        backupEntries.add(fromAvroEntry(avroEntry));
      }
      if (!checkConsistency(backupEntries)) {
        LOG.error("Backup schema entries are inconsistent");
      }

      mergedEntries.addAll(backupEntries);
      if (!checkConsistency(backupEntries)) {
        LOG.error("Backup schema entries are inconsistent with already existing schema entries");
      }

      long maxSchemaId = -1L;
      for (SchemaEntry entry : mergedEntries) {
        maxSchemaId = Math.max(maxSchemaId, entry.getId());
      }
      final long nextSchemaId = maxSchemaId + 1;

      flush();
      mSchemaIdTable.setWriteBufferSize(backupEntries.size() + 1);
      mSchemaHashTable.setWriteBufferSize(backupEntries.size());

      // Restored schema entries share the same timestamp:
      final long timestamp = System.currentTimeMillis();
      for (SchemaEntry entry : backupEntries) {
        storeInTable(toAvroEntry(entry), timestamp, false);  // do not flush
      }
      setSchemaIdCounter(nextSchemaId);
      flush();
    } finally {
      mZKLock.unlock();
    }
  }

  /**
   * Checks the consistency of a collection of schema entries.
   *
   * @param entries Collection of schema entries.
   * @return whether the entries are consistent.
   */
  private static boolean checkConsistency(Set<SchemaEntry> entries) {
    final Map<Long, SchemaEntry> idMap = new HashMap<Long, SchemaEntry>(entries.size());
    final Map<BytesKey, SchemaEntry> hashMap = new HashMap<BytesKey, SchemaEntry>(entries.size());
    boolean isConsistent = true;

    for (SchemaEntry entry : entries) {
      final SchemaEntry existingEntryWithId = idMap.put(entry.getId(), entry);
      if ((existingEntryWithId != null) && !existingEntryWithId.equals(entry)) {
        LOG.error(String.format("Conflicting schema entries with ID %d: %s vs %s",
            entry.getId(), entry, existingEntryWithId));
        isConsistent = false;
      }
      final SchemaEntry existingEntryWithHash = hashMap.put(entry.getHash(), entry);
      if ((existingEntryWithHash != null) && !existingEntryWithHash.equals(entry)) {
        if (existingEntryWithHash.getHash().equals(entry.getHash())
            && existingEntryWithHash.getSchema().equals(entry.getSchema())) {
          // Does not affect consistency:
          LOG.info(String.format("Schema with hash %s has multiple IDs: %d, %d: %s",
              entry.getHash(), entry.getId(), existingEntryWithHash.getId(), entry.getSchema()));
        } else {
          LOG.info(String.format("Conflicting schema entries with hash %s: %s vs %s",
              entry.getHash(), entry, existingEntryWithHash));
          isConsistent = false;
        }
      }
    }
    return isConsistent;
  }

  /** Primitive types pre-allocated in all schema tables. */
  private static final Schema.Type[] PRIMITIVE_TYPES = new Schema.Type[] {
      Schema.Type.STRING,  // schema ID = 0
      Schema.Type.BYTES,   // schema ID = 1
      Schema.Type.INT,     // schema ID = 2
      Schema.Type.LONG,    // schema ID = 3
      Schema.Type.FLOAT,   // schema ID = 4
      Schema.Type.DOUBLE,  // schema ID = 5
      Schema.Type.BOOLEAN, // schema ID = 6
      Schema.Type.NULL,    // schema ID = 7
  };

  /**
   * Pre-registers all the primitive data types.
   *
   * @throws IOException on I/O failure.
   */
  private synchronized void registerPrimitiveSchemas() throws IOException {
    int expectedSchemaId = 0;
    LOG.debug("Pre-registering primitive schema types.");
    for (Schema.Type type : PRIMITIVE_TYPES) {
      final Schema schema = Schema.create(type);
      Preconditions.checkState(getOrCreateSchemaId(schema) == expectedSchemaId);
      expectedSchemaId += 1;
    }
  }

  /**
   * Loads and check the consistency of the schema hash table.
   *
   * @param hashTable schema hash HTable.
   * @return the set of schema entries from the schema hash table.
   * @throws IOException on I/O error.
   */
  private Set<SchemaEntry> loadSchemaHashTable(HTableInterface hashTable) throws IOException {
    LOG.info("Loading entries from schema hash table.");
    final Set<SchemaEntry> entries = new HashSet<SchemaEntry>();
    int hashTableRowCounter = 0;
    final ResultScanner hashTableScanner = hashTable.getScanner(
        new Scan()
            .addColumn(SCHEMA_COLUMN_FAMILY_BYTES, SCHEMA_COLUMN_QUALIFIER_BYTES)
            .setMaxVersions());  // retrieve all versions
    for (Result result : hashTableScanner) {
      hashTableRowCounter += 1;
      if (result.getRow().length != Hasher.HASH_SIZE_BYTES) {
        LOG.error(String.format(
            "Invalid schema hash table row key size: %s, expecting %d bytes.",
            new BytesKey(result.getRow()), Hasher.HASH_SIZE_BYTES));
        continue;
      }
      final BytesKey rowKey = new BytesKey(result.getRow());
      for (KeyValue keyValue : result.getColumn(SCHEMA_COLUMN_FAMILY_BYTES,
                                                SCHEMA_COLUMN_QUALIFIER_BYTES)) {
        try {
          final SchemaEntry entry = fromAvroEntry(decodeSchemaEntry(keyValue.getValue()));
          entries.add(entry);
          if (!getSchemaHash(entry.getSchema()).equals(entry.getHash())) {
            LOG.error(String.format(
                "Invalid schema hash table entry: computed schema hash %s does not match entry %s",
                getSchemaHash(entry.getSchema()), entry));
          }
          if (!rowKey.equals(entry.getHash())) {
            LOG.error(String.format("Inconsistent schema hash table: "
                + "hash encoded in row key %s does not match schema entry: %s",
                rowKey, entry));
          }
        } catch (IOException ioe) {
          LOG.error(String.format(
              "Unable to decode schema hash table entry for row %s, timestamp %d: %s",
              rowKey, keyValue.getTimestamp(), ioe));
          continue;
        } catch (AvroRuntimeException are) {
          LOG.error(String.format(
              "Unable to decode schema hash table entry for row %s, timestamp %d: %s",
              rowKey, keyValue.getTimestamp(), are));
          continue;
        }
      }
    }
    LOG.info(String.format(
        "Schema hash table has %d rows and %d entries.", hashTableRowCounter, entries.size()));
    return entries;
  }

  /**
   * Loads and check the consistency of the schema ID table.
   *
   * @param idTable schema ID HTable.
   * @return the set of schema entries from the schema ID table.
   * @throws IOException on I/O error.
   */
  private Set<SchemaEntry> loadSchemaIdTable(HTableInterface idTable) throws IOException {
    LOG.info("Loading entries from schema ID table.");
    int idTableRowCounter = 0;
    final Set<SchemaEntry> entries = new HashSet<SchemaEntry>();
    final ResultScanner idTableScanner = idTable.getScanner(
        new Scan()
        .addColumn(SCHEMA_COLUMN_FAMILY_BYTES, SCHEMA_COLUMN_QUALIFIER_BYTES)
        .setMaxVersions());  // retrieve all versions
    for (Result result : idTableScanner) {
      idTableRowCounter += 1;
      final BytesKey rowKey = new BytesKey(result.getRow());

      long schemaId = -1;
      try {
        schemaId = new ByteStreamArray(result.getRow()).readVarInt64();
      } catch (EncodingException exn) {
        LOG.error(String.format("Unable to decode schema ID encoded in row key %s: %s",
            rowKey, exn));
      }

      for (KeyValue keyValue : result.getColumn(SCHEMA_COLUMN_FAMILY_BYTES,
                                                SCHEMA_COLUMN_QUALIFIER_BYTES)) {
        try {
          final SchemaEntry entry = fromAvroEntry(decodeSchemaEntry(keyValue.getValue()));
          entries.add(entry);
          if (!getSchemaHash(entry.getSchema()).equals(entry.getHash())) {
            LOG.error(String.format("Invalid schema hash table entry with row key %s: "
                + "computed schema hash %s does not match entry %s",
                rowKey, getSchemaHash(entry.getSchema()), entry));
          }
          if (schemaId != entry.getId()) {
            LOG.error(String.format("Inconsistent schema ID table: "
                + "ID encoded in row key %d does not match entry: %s", schemaId, entry));
          }
        } catch (IOException ioe) {
          LOG.error(String.format(
              "Unable to decode schema ID table entry for row %s, timestamp %d: %s",
              rowKey, keyValue.getTimestamp(), ioe));
          continue;
        } catch (AvroRuntimeException are) {
          LOG.error(String.format(
              "Unable to decode schema ID table entry for row %s, timestamp %d: %s",
              rowKey, keyValue.getTimestamp(), are));
          continue;
        }
      }
    }
    LOG.info(String.format(
        "Schema ID table has %d rows and %d entries.", idTableRowCounter, entries.size()));
    return entries;
  }

  /**
   * Distributed lock on top of ZooKeeper.
   */
  @ApiAudience.Private
  static class ZooKeeperLock implements Lock, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperLock.class);
    private static final byte[] EMPTY = new byte[0];

    private final ZooKeeper mZooKeeper;
    private final File mLockDir;
    private final File mLockPathPrefix;
    private File mCreatedPath = null;
    private WatchedEvent mPrecedingEvent = null;

    /**
     * Constructs a ZooKeeper lock object.
     *
     * @param zookeeper
     *          ZooKeeper client.
     * @param lockDir
     *          Path of the directory node to use for the lock.
     */
    ZooKeeperLock(ZooKeeper zookeeper, File lockDir) {
      this.mZooKeeper = zookeeper;
      this.mLockDir = lockDir;
      this.mLockPathPrefix = new File(lockDir, "lock-");
    }

    /** Watches the lock directory node. */
    private final class LockWatcher implements Watcher {
      @Override
      public void process(WatchedEvent event) {
        LOG.debug(String.format("%s: received event", this));
        synchronized (ZooKeeperLock.this) {
          mPrecedingEvent = event;
          ZooKeeperLock.this.notifyAll();
        }
      }
    }

    private final LockWatcher mLockWatcher = new LockWatcher();

    /** {@inheritDoc} */
    @Override
    public void lock() throws IOException {
      if (!lock(0.0)) {
        // This should never happen, instead lock(0.0) should raise a KeeperException!
        throw new RuntimeException("Unable to acquire ZooKeeper lock.");
      }
    }

    /** {@inheritDoc} */
    @Override
    public boolean lock(double timeout) throws IOException {
      while (true) {
        try {
          return lockInternal(timeout);
        } catch (InterruptedException ie) {
          LOG.info(ie.toString());
        } catch (KeeperException ke) {
          throw new IOException(ke);
        }
      }
    }

    /**
     * Creates a ZooKeeper node and all its parents, if necessary.
     *
     * @param zkClient ZooKeeper client.
     * @param path ZooKeeper node path.
     * @throws KeeperException on I/O error.
     */
    private void createZKNodeRecursively(ZooKeeper zkClient, File path)
        throws KeeperException {
      if (path.getPath().equals("/")) {
        return;
      }
      final File parent = path.getParentFile();
      if (parent != null) {
        createZKNodeRecursively(zkClient, parent);
      }
      while (true) {
        try {
          LOG.debug(String.format("Creating ZooKeeper node %s", path));
          final File created = new File(zkClient.create(
              path.toString(), EMPTY, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
          Preconditions.checkState(created.equals(path));
          return;
        } catch (NodeExistsException exn) {
          // Node already exists, ignore!
          return;
        } catch (InterruptedException ie) {
          LOG.error(ie.toString());
        }
      }
    }

    /**
     * Creates a ZooKeeper lock node.
     *
     * @param zkClient ZooKeeper client.
     * @param path ZooKeeper lock node path prefix.
     * @return the created ZooKeeper lock node path.
     * @throws KeeperException on error.
     */
    private File createZKLockNode(ZooKeeper zkClient, File path) throws KeeperException {
      boolean parentCreated = false;
      while (true) {
        try {
          return new File(zkClient.create(
              path.toString(), EMPTY, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL));
        } catch (NoNodeException nne) {
          LOG.debug(nne.toString());
          Preconditions.checkState(!parentCreated);  // creates the parent node at most once.
          createZKNodeRecursively(zkClient, path.getParentFile());
          parentCreated = true;
        } catch (InterruptedException ie) {
          LOG.debug(ie.toString());
          // Retry
        }
      }
    }

    /**
     * Acquires the lock.
     *
     * @param timeout
     *          Deadline, in seconds, to acquire the lock. 0 means no timeout.
     * @return whether the lock is acquired (ie. false means timeout).
     * @throws InterruptedException on interruption.
     * @throws KeeperException on error.
     */
    private boolean lockInternal(double timeout) throws InterruptedException, KeeperException {
      /** Absolute time deadline, in seconds since Epoch */
      final double absoluteDeadline = (timeout > 0.0)
          ? (System.currentTimeMillis() / 1000.0) + timeout
          : 0.0;

      File createdPath = null;
      synchronized (this) {
        Preconditions.checkState(null == mCreatedPath, mCreatedPath);
        // Queues for access to the lock:
        createdPath = createZKLockNode(mZooKeeper, mLockPathPrefix);
        mCreatedPath = createdPath;
      }
      LOG.debug(String.format("%s: queuing for lock with node %s", this, createdPath));

      while (true) {
        try {
          // Do we own the lock already, or do we have to wait?
          final Set<String> childrenSet =
              new TreeSet<String>(mZooKeeper.getChildren(mLockDir.toString(), false));
          final String[] children = childrenSet.toArray(new String[childrenSet.size()]);
          LOG.debug(String.format("%s: lock queue: %s", this, childrenSet));

          final int index = Arrays.binarySearch(children, createdPath.getName());
          if (index == 0) {
            // We own the lock:
            LOG.debug(String.format("%s: lock acquired", this));
            return true;
          } else { // index >= 1
            synchronized (this) {
              final File preceding = new File(mLockDir, children[index - 1]);
              LOG.debug(String.format("%s: waiting for preceding node %s to disappear",
                  this, preceding));
              if (mZooKeeper.exists(preceding.toString(), mLockWatcher) != null) {
                if (absoluteDeadline > 0.0) {
                  final double timeLeft = absoluteDeadline - (System.currentTimeMillis() / 1000.0);
                  if (timeLeft <= 0) {
                    LOG.debug(String.format("%s: out of time while acquiring lock, deleting %s",
                        this, mCreatedPath));
                    mZooKeeper.delete(mCreatedPath.toString(), -1); // -1 means any version
                    mCreatedPath = null;
                    return false;
                  }
                  this.wait((long)(timeLeft * 1000.0));
                } else {
                  this.wait();
                }
                if ((mPrecedingEvent != null)
                    && (mPrecedingEvent.getType() == EventType.NodeDeleted)
                    && (index == 1)) {
                  LOG.debug(String.format("%s: lock acquired after %s disappeared",
                      this, children[index - 1]));
                }
              }
            }
          }
        } catch (InterruptedException ie) {
          LOG.info(String.format("%s: interrupted: %s", this, ie));
        }
      }
    }

    /** {@inheritDoc} */
    @Override
    public void unlock() throws IOException {
      while (true) {
        try {
          unlockInternal();
          return;
        } catch (InterruptedException ie) {
          LOG.info(ie.toString());
        } catch (KeeperException ke) {
          throw new IOException(ke);
        }
      }
    }

    /**
       * Releases the lock.
       *
       * @throws InterruptedException if the thread is interrupted.
       * @throws KeeperException on error.
       */
    private void unlockInternal() throws InterruptedException, KeeperException {
      File pathToDelete = null;
      synchronized (this) {
        Preconditions.checkState(null != mCreatedPath, mCreatedPath);
        pathToDelete = mCreatedPath;
        LOG.debug(String.format("Releasing lock: deleting %s", mCreatedPath));
        mCreatedPath = null;
      }
      mZooKeeper.delete(pathToDelete.toString(), -1); // -1 means any version
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      while (true) {
        try {
          this.mZooKeeper.close();
          return;
        } catch (InterruptedException ie) {
          LOG.error(ie.toString());
        }
      }
    }
  }

  /** Factory for ZooKeeperLock instances. */
  @ApiAudience.Private
  private static final class ZooKeeperLockFactory implements LockFactory {

    /** ZooKeeper instance to use. */
    private final ZooKeeper mZooKeeper;

    /**
     * Creates a factory for ZooKeeperLock.
     *
     * @param zkClient ZooKeeper client
     */
    public ZooKeeperLockFactory(ZooKeeper zkClient) {
      mZooKeeper = Preconditions.checkNotNull(zkClient);
    }

    /** {@inheritDoc} */
    @Override
    public Lock create(String name) {
      return new ZooKeeperLock(mZooKeeper, new File(name));
    }
  }
}
