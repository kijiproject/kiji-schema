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

import static org.kiji.schema.util.ByteStreamArray.longToVarInt64;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
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
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.MD5Hash;
import org.kiji.schema.avro.SchemaTableBackup;
import org.kiji.schema.avro.SchemaTableEntry;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.impl.HTableInterfaceFactory;
import org.kiji.schema.platform.SchemaPlatformBridge;
import org.kiji.schema.util.ByteStreamArray;
import org.kiji.schema.util.ByteStreamArray.EncodingException;
import org.kiji.schema.util.BytesKey;
import org.kiji.schema.util.DebugResourceTracker;
import org.kiji.schema.util.Hasher;
import org.kiji.schema.util.Lock;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.schema.zookeeper.ZooKeeperLock;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

/**
 * <p>
 * Mapping between schema IDs, hashes and Avro schema objects.
 * This class is thread-safe.
 * </p>
 *
 * <p>
 * Schemas are stored in two tables with a single column family named "schema" and that contains
 * SchemaTableEntry records. One table is indexed by schema hashes (128-bit MD5 hashes of the
 * schema JSON representation). Other table is indexed by schema IDs (integers &gt;= 0).
 *
 * There may be multiple schema IDs for a single schema.
 * </p>
 */
@ApiAudience.Private
public final class HBaseSchemaTable implements KijiSchemaTable {
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

  /** Connection to ZooKeeper. */
  private final CuratorFramework mZKClient;

  /** Lock for the kiji instance schema table. */
  private final Lock mZKLock;

  /** Maps schema MD5 hashes to schema entries. */
  private final Map<BytesKey, SchemaEntry> mSchemaHashMap = new HashMap<BytesKey, SchemaEntry>();

  /** Maps schema IDs to schema entries. */
  private final Map<Long, SchemaEntry> mSchemaIdMap = new HashMap<Long, SchemaEntry>();

  /** Schema hash cache. */
  private final SchemaHashCache mHashCache = new KijiSchemaTable.SchemaHashCache();

  /** KijiURI of the Kiji instance this schema table belongs to. */
  private final KijiURI mURI;

  /** States of a SchemaTable instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this SchemaTable instance. */
  private AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /**
   * Creates an HTable handle to the schema hash table.
   *
   * @param kijiURI the KijiURI.
   * @param conf the Hadoop configuration.
   * @param factory HTableInterface factory.
   * @return a new interface for the table storing the mapping from schema hash to schema entry.
   * @throws IOException on I/O error.
   */
  public static HTableInterface newSchemaHashTable(
      KijiURI kijiURI,
      Configuration conf,
      HTableInterfaceFactory factory)
      throws IOException {
    return factory.create(conf,
        KijiManagedHBaseTableName.getSchemaHashTableName(kijiURI.getInstance()).toString());
  }

  /**
   * Creates an HTable handle to the schema ID table.
   *
   * @param kijiURI the KijiURI.
   * @param conf the Hadoop configuration.
   * @param factory HTableInterface factory.
   * @return a new interface for the table storing the mapping from schema ID to schema entry.
   * @throws IOException on I/O error.
   */
  public static HTableInterface newSchemaIdTable(
      KijiURI kijiURI,
      Configuration conf,
      HTableInterfaceFactory factory)
      throws IOException {
    return factory.create(
        conf,
        KijiManagedHBaseTableName.getSchemaIdTableName(kijiURI.getInstance()).toString());
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

  /** {@inheritDoc} */
  @Override
  public BytesKey getSchemaHash(Schema schema) {
    return mHashCache.getHash(schema);
  }

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
   * @param kijiURI the KijiURI
   * @param conf The Hadoop configuration.
   * @param tableFactory HTableInterface factory.
   * @throws IOException on I/O error.
   */
  public HBaseSchemaTable(
      KijiURI kijiURI,
      Configuration conf,
      HTableInterfaceFactory tableFactory
  ) throws IOException {
    mURI = kijiURI;
    mSchemaHashTable = newSchemaHashTable(mURI, conf, tableFactory);
    mSchemaIdTable = newSchemaIdTable(mURI, conf, tableFactory);
    mZKClient = ZooKeeperUtils.getZooKeeperClient(mURI);
    mZKLock = new ZooKeeperLock(mZKClient, ZooKeeperUtils.getSchemaTableLock(mURI));

    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open SchemaTable instance in state %s.", oldState);
    DebugResourceTracker.get().registerResource(this);
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
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get or create schema entry from SchemaTable instance in state %s.", state);

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

  /** {@inheritDoc} */
  @Override
  public synchronized SchemaEntry getSchemaEntry(long schemaId) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get schema entry from SchemaTable instance in state %s.", state);

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

  /** {@inheritDoc} */
  @Override
  public synchronized SchemaEntry getSchemaEntry(BytesKey schemaHash) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get schema entry from SchemaTable instance in state %s.", state);

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

  /** {@inheritDoc} */
  @Override
  public SchemaEntry getSchemaEntry(Schema schema) throws IOException {
    return getSchemaEntry(getSchemaHash(schema));
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
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot flush SchemaTable instance in state %s.", state);
    mSchemaIdTable.flushCommits();
    mSchemaHashTable.flushCommits();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void close() throws IOException {
    flush();
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close SchemaTable instance in state %s.", oldState);
    DebugResourceTracker.get().unregisterResource(this);
    ResourceUtils.closeOrLog(mSchemaHashTable);
    ResourceUtils.closeOrLog(mSchemaIdTable);
    ResourceUtils.closeOrLog(mZKLock);
    ResourceUtils.closeOrLog(mZKClient);
  }

  /**
   * Install the schema table into a Kiji instance.
   *
   * @param admin The HBase Admin interface for the HBase cluster to install into.
   * @param kijiURI the KijiURI.
   * @param conf The Hadoop configuration.
   * @param tableFactory HTableInterface factory.
   * @throws IOException on I/O error.
   */
  public static void install(
      HBaseAdmin admin,
      KijiURI kijiURI,
      Configuration conf,
      HTableInterfaceFactory tableFactory
  ) throws IOException {
    // Keep all versions of schema entries:
    //  - entries of the ID table should never be written more than once.
    //  - entries of the hash table could be written more than once:
    //      - with different schema IDs in some rare cases, for example when a client crashes
    //        while writing an entry.
    //      - with different schemas on MD5 hash collisions.
    final int maxVersions = Integer.MAX_VALUE;

    final HTableDescriptor hashTableDescriptor = new HTableDescriptor(
        KijiManagedHBaseTableName.getSchemaHashTableName(kijiURI.getInstance()).toString());
    final HColumnDescriptor hashColumnDescriptor = SchemaPlatformBridge.get()
        .createHColumnDescriptorBuilder(SCHEMA_COLUMN_FAMILY_BYTES)
        .setMaxVersions(maxVersions)
        .setCompressionType("none")
        .setInMemory(false)
        .setBlockCacheEnabled(true)
        .setTimeToLive(HConstants.FOREVER)
        .setBloomType("NONE")
        .build();
    hashTableDescriptor.addFamily(hashColumnDescriptor);
    admin.createTable(hashTableDescriptor);

    final HTableDescriptor idTableDescriptor = new HTableDescriptor(
        KijiManagedHBaseTableName.getSchemaIdTableName(kijiURI.getInstance()).toString());
    final HColumnDescriptor idColumnDescriptor = SchemaPlatformBridge.get()
        .createHColumnDescriptorBuilder(SCHEMA_COLUMN_FAMILY_BYTES)
        .setMaxVersions(maxVersions)
        .setCompressionType("none")
        .setInMemory(false)
        .setBlockCacheEnabled(true)
        .setTimeToLive(HConstants.FOREVER)
        .setBloomType("NONE")
        .build();
    idTableDescriptor.addFamily(idColumnDescriptor);
    admin.createTable(idTableDescriptor);

    final HBaseSchemaTable schemaTable = new HBaseSchemaTable(kijiURI, conf, tableFactory);
    try {
      schemaTable.setSchemaIdCounter(0L);
      schemaTable.registerPrimitiveSchemas();
    } finally {
      ResourceUtils.closeOrLog(schemaTable);
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
   * @param kijiURI The KijiURI for the instance to remove.
   * @throws IOException If there is an error.
   */
  public static void uninstall(HBaseAdmin admin, KijiURI kijiURI)
      throws IOException {
    final String hashTableName =
        KijiManagedHBaseTableName.getSchemaHashTableName(kijiURI.getInstance()).toString();
    deleteTable(admin, hashTableName);

    final String idTableName =
        KijiManagedHBaseTableName.getSchemaIdTableName(kijiURI.getInstance()).toString();
    deleteTable(admin, idTableName);
  }

  /** {@inheritDoc} */
  @Override
  public SchemaTableBackup toBackup() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot backup SchemaTable instance in state %s.", state);
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
    return SchemaTableBackup.newBuilder().setEntries(entries).build();
  }

  /** {@inheritDoc} */
  @Override
  public void fromBackup(final SchemaTableBackup backup) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot restore backup to SchemaTable instance in state %s.", state);
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

      final List<SchemaTableEntry> avroBackupEntries = backup.getEntries();
      final Set<SchemaEntry> schemaTableEntries =
          new HashSet<SchemaEntry>(avroBackupEntries.size());
      for (SchemaTableEntry avroEntry : avroBackupEntries) {
        schemaTableEntries.add(fromAvroEntry(avroEntry));
      }
      if (!checkConsistency(schemaTableEntries)) {
        LOG.error("Backup schema entries are inconsistent");
      }

      mergedEntries.addAll(schemaTableEntries);
      if (!checkConsistency(schemaTableEntries)) {
        LOG.error("Backup schema entries are inconsistent with already existing schema entries");
      }

      long maxSchemaId = -1L;
      for (SchemaEntry entry : mergedEntries) {
        maxSchemaId = Math.max(maxSchemaId, entry.getId());
      }
      final long nextSchemaId = maxSchemaId + 1;

      flush();
      SchemaPlatformBridge.get().setWriteBufferSize(mSchemaIdTable, schemaTableEntries.size() + 1);
      SchemaPlatformBridge.get().setWriteBufferSize(mSchemaHashTable, schemaTableEntries.size());

      // Restored schema entries share the same timestamp:
      final long timestamp = System.currentTimeMillis();
      for (SchemaEntry entry : schemaTableEntries) {
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
  enum PreRegisteredSchema {
    STRING(Schema.Type.STRING),   // ID 0
    BYTES(Schema.Type.BYTES),     // ID 1
    INT(Schema.Type.INT),         // ID 2
    LONG(Schema.Type.LONG),       // ID 3
    FLOAT(Schema.Type.FLOAT),     // ID 4
    DOUBLE(Schema.Type.DOUBLE),   // ID 5
    BOOLEAN(Schema.Type.BOOLEAN), // ID 6
    NULL(Schema.Type.NULL);       // ID 7

    /**
     * Initializes a pre-registered schema descriptor.
     *
     * @param type Avro schema type.
     */
    PreRegisteredSchema(Schema.Type type) {
      mType = type;
      mId = ordinal();
    }

    /** @return the Avro schema type. */
    public Schema.Type getType() {
      return mType;
    }

    /** @return the unique ID of the pre-allocated schema. */
    public int getSchemaId() {
      // By default, we use the enum ordinal
      return mId;
    }

    private final int mId;
    private final Schema.Type mType;
  }

  /** Number of pre-allocated schemas. */
  public static final int PRE_REGISTERED_SCHEMA_COUNT = PreRegisteredSchema.values().length;  // = 8

  /**
   * Pre-registers all the primitive data types.
   *
   * @throws IOException on I/O failure.
   */
  private synchronized void registerPrimitiveSchemas() throws IOException {
    int expectedSchemaId = 0;
    LOG.debug("Pre-registering primitive schema types.");
    for (PreRegisteredSchema desc : PreRegisteredSchema.values()) {
      final Schema schema = Schema.create(desc.getType());
      Preconditions.checkState(getOrCreateSchemaId(schema) == expectedSchemaId);
      Preconditions.checkState(desc.getSchemaId() == expectedSchemaId);
      expectedSchemaId += 1;
    }
    Preconditions.checkState(expectedSchemaId == PRE_REGISTERED_SCHEMA_COUNT);
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
      // Skip the schema ID counter row:
      if (Arrays.equals(result.getRow(), SCHEMA_COUNTER_ROW_NAME_BYTES)) {
        continue;
      }
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

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(HBaseSchemaTable.class)
        .add("uri", mURI)
        .add("state", mState.get())
        .toString();
  }
}
