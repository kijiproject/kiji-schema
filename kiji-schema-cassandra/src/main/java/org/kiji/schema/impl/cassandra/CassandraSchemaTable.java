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

package org.kiji.schema.impl.cassandra;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;
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
import org.apache.hadoop.hbase.HConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiNotInstalledException;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.MD5Hash;
import org.kiji.schema.avro.SchemaTableBackup;
import org.kiji.schema.avro.SchemaTableEntry;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.util.BytesKey;
import org.kiji.schema.util.DebugResourceTracker;
import org.kiji.schema.util.Lock;
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
 * There is a third table with a counter for the Schema IDs.
 *
 * There may be multiple schema IDs for a single schema.
 * </p>
 */
@ApiAudience.Private
public final class CassandraSchemaTable implements KijiSchemaTable {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraSchemaTable.class);

  /** The column name in C* for the keys in the schema hash table. */
  public static final String SCHEMA_COLUMN_HASH_KEY = "schema_hash";

  /** The column name in C* for the keys in the schema ID table. */
  public static final String SCHEMA_COLUMN_ID_KEY = "schema_id";

  /** The column name in C* for the values in the schema hash and ID tables (same for both). */
  public static final String SCHEMA_COLUMN_VALUE = "schema_blob";

  /** The column name for the timestamp value in the schema hash and ID tables (same for both). */
  public static final String SCHEMA_COLUMN_TIME = "time";

  /** We need some kind of PRIMARY KEY column for the counter table. */
  public static final String SCHEMA_COUNTER_COLUMN_KEY = "counter_key";

  /** We should have only one row ever in this table... */
  public static final String SCHEMA_COUNTER_ONLY_KEY_VALUE = "THE_ONLY_COUNTER";

  /**
   * The column name of the C* counter used to store schema IDs.  In C*, counters go into their
   * own tables.
   */
  public static final String SCHEMA_COUNTER_COLUMN_VALUE = "counter";

  /** Cassandra cluster connection. */
  private final CassandraAdmin mAdmin;

  /** C* table used to map schema hash to schema entries. */
  private final CassandraTableName mSchemaHashTable;

  /** C* table used to map schema IDs to schema entries. */
  private final CassandraTableName mSchemaIdTable;

  /** C* table used to increment schema IDs. */
  private final CassandraTableName mCounterTable;

  /** Connection to ZooKeeper. */
  private final CuratorFramework mZKClient;

  /** Lock for the kiji instance schema table. */
  private final Lock mZKLock;

  /** Maps schema MD5 hashes to schema entries. */
  private final Map<BytesKey, SchemaEntry> mSchemaHashMap = new HashMap<BytesKey, SchemaEntry>();

  /** Maps schema IDs to schema entries. */
  private final Map<Long, SchemaEntry> mSchemaIdMap = new HashMap<Long, SchemaEntry>();

  /** Schema hash cache. */
  private final SchemaHashCache mHashCache = new SchemaHashCache();

  /** KijiURI of the Kiji instance this schema table belongs to. */
  private final KijiURI mInstanceURI;

  /** States of a SchemaTable instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this SchemaTable instance. */
  private AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

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

  /** Prepared statement for reading from the hash table. */
  private PreparedStatement mPreparedStatementReadHashTable = null;

  /** Prepared statement for writing to the hash table. */
  private PreparedStatement mPreparedStatementWriteHashTable = null;

  /** Prepared statement for writing to the ID table. */
  private PreparedStatement mPreparedStatementWriteIdTable = null;

  /** {@inheritDoc} */
  @Override
  public BytesKey getSchemaHash(Schema schema) {
    return mHashCache.getHash(schema);
  }

  /**
   * Prepare the statement for writing to the hash table.
   *
   * @throws java.io.IOException if there is a problem preparing the statement.
   */
  private void prepareQueryWriteHashTable() throws IOException {
    String hashQueryText = String.format(
        "INSERT INTO %s (%s, %s, %s) VALUES(?, ?, ?);",
        mSchemaHashTable,
        SCHEMA_COLUMN_HASH_KEY,
        SCHEMA_COLUMN_TIME,
        SCHEMA_COLUMN_VALUE);

    mPreparedStatementWriteHashTable = mAdmin.getPreparedStatement(hashQueryText);
  }

  /**
   * Prepare the statement for writing to the ID table.
   *
   * @throws java.io.IOException if there is a problem preparing the statement.
   */
  private void prepareQueryWriteIdTable() throws IOException {
    String idQueryText = String.format(
        "INSERT INTO %s (%s, %s, %s) VALUES(?, ?, ?);",
        mSchemaIdTable,
        SCHEMA_COLUMN_ID_KEY,
        SCHEMA_COLUMN_TIME,
        SCHEMA_COLUMN_VALUE);

    mPreparedStatementWriteIdTable = mAdmin.getPreparedStatement(idQueryText);
  }

  /**
   * Prepare the statement for reading from the hash table.
   *
   * @throws java.io.IOException if there is a problem preparing the statement.
   */
  private void prepareQueryReadHashTable() throws IOException {
    String queryText = String.format(
        "SELECT %s FROM %s WHERE %s=? ORDER BY %s DESC LIMIT 1",
        SCHEMA_COLUMN_VALUE,
        mSchemaHashTable,
        SCHEMA_COLUMN_HASH_KEY,
        SCHEMA_COLUMN_TIME);
    mPreparedStatementReadHashTable = mAdmin.getPreparedStatement(queryText);
  }

  /**
   * Decodes a binary-encoded Avro schema entry.
   *
   * @param bytes Binary-encoded Avro schema entry.
   * @return Decoded Avro schema entry.
   * @throws java.io.IOException on I/O error.
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
   * @throws java.io.IOException on I/O error.
   */
  public static byte[] encodeSchemaEntry(final SchemaTableEntry avroEntry) throws IOException {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream(4096);
    final Encoder encoder = ENCODER_FACTORY.directBinaryEncoder(bytes, null);
    SCHEMA_ENTRY_WRITER.write(avroEntry, encoder);
    return bytes.toByteArray();
  }

  /**
   * Wrap an existing HBase table assumed to be where the schema data is stored.
   *
   * @param admin Cassandra connection.
   * @param instanceURI URI of the Kiji instance this schema table belongs to.
   * @throws java.io.IOException on I/O error.
   */
  public CassandraSchemaTable(CassandraAdmin admin, KijiURI instanceURI)
      throws IOException {
    mAdmin = Preconditions.checkNotNull(admin);
    mInstanceURI = instanceURI;
    mSchemaHashTable = CassandraTableName.getSchemaHashTableName(instanceURI);
    mSchemaIdTable = CassandraTableName.getSchemaIdTableName(instanceURI);
    mCounterTable = CassandraTableName.getSchemaCounterTableName(instanceURI);

    if (!mAdmin.tableExists(mSchemaHashTable)) {
      throw new KijiNotInstalledException("Schema hash table not installed.", instanceURI);
    }
    if (!mAdmin.tableExists(mSchemaIdTable)) {
      throw new KijiNotInstalledException("Schema ID table not installed.", instanceURI);
    }
    if (!mAdmin.tableExists(mCounterTable)) {
      throw new KijiNotInstalledException("Schema counter table not installed.", instanceURI);
    }
    mZKClient = ZooKeeperUtils.getZooKeeperClient(instanceURI.getZooKeeperEnsemble());
    mZKLock = new ZooKeeperLock(mZKClient, ZooKeeperUtils.getSchemaTableLock(mInstanceURI));

    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open SchemaTable instance in state %s.", oldState);
    DebugResourceTracker.get().registerResource(this);

    // Prepare queries that we'll use multiple times
    prepareQueryWriteHashTable();
    prepareQueryWriteIdTable();
    prepareQueryReadHashTable();
  }

  /**
   * Looks up a schema entry given an Avro schema object.
   *
   * Looks first in-memory. If the schema is not known in-memory, looks in the Cassandra tables.
   *
   * @param schema Avro schema to look up.
   * @return Either the pre-existing entry for the specified schema, or a newly created entry.
   * @throws java.io.IOException on I/O error.
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
   * @throws java.io.IOException on I/O error.
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
      incrementSchemaIdCounter(1);
      long schemaId = readSchemaIdCounter() - 1;

      final SchemaEntry entry = new SchemaEntry(schemaId, schemaHash, schema);
      storeInTable(toAvroEntry(entry));
      return entry;

    } finally {
      mZKLock.unlock();
    }
  }

  /**
   * Increment the schema ID counter.
   * @param incrementAmount Amount by which to increment the counter (can be negative).
   */
  private void incrementSchemaIdCounter(long incrementAmount) {
    CassandraTableName tableName = mCounterTable;
    String incrementSign = incrementAmount >= 0 ? "+" : "-";
    String queryText = String.format("UPDATE %s SET %s = %s %s %d WHERE %s='%s';",
        tableName,
        SCHEMA_COUNTER_COLUMN_VALUE,
        SCHEMA_COUNTER_COLUMN_VALUE,
        incrementSign,
        incrementAmount,
        SCHEMA_COUNTER_COLUMN_KEY,
        SCHEMA_COUNTER_ONLY_KEY_VALUE);
    mAdmin.execute(queryText);
  }

  /**
   * Read back the current value of the schema ID counter.
   * @return Value of the counter.
   */
  private long readSchemaIdCounter() {
    // Sanity check that counter value is 1!
    String queryText = String.format("SELECT * FROM %s;", mCounterTable);
    ResultSet resultSet = mAdmin.execute(queryText);
    List<Row> rows = resultSet.all();
    assert(rows.size() == 1);
    Row row = rows.get(0);
    return row.getLong(SCHEMA_COUNTER_COLUMN_VALUE);
  }

  /**
   * Used for resetting the schema ID counter.
   *
   * This is fairly hackish and relies upon the counter being locked with a ZooKeeper lock.
   * @param newCounterValue Value to which to set the counter.
   */
  private void setSchemaIdCounter(long newCounterValue) {
    // Get the current counter value
    long currentValue = readSchemaIdCounter();
    incrementSchemaIdCounter(newCounterValue - currentValue);
  }

  /**
   * Writes the given schema entry to the ID and hash tables.
   *
   * This is not protected from concurrent writes. Caller must ensure consistency.
   *
   * @param avroEntry Schema entry to write.
   * @throws java.io.IOException on I/O error.
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
   * @throws java.io.IOException on I/O error.
   */
  private void storeInTable(final SchemaTableEntry avroEntry, long timestamp, boolean flush)
      throws IOException {
    final byte[] entryBytes = encodeSchemaEntry(avroEntry);

    // TODO: Obviate this comment by doing all of this in batch.
    // Writes the ID mapping first: if the hash table write fails, we just lost one schema ID.
    // The hash table write must not happen before the ID table write has been persisted.
    // Otherwise, another client may see the hash entry, write cells with the schema ID that cannot
    // be decoded (since the ID mapping has not been written yet).
    final ResultSet resultSet = mAdmin.execute(
        mPreparedStatementWriteIdTable.bind(
            avroEntry.getId(),
            new Date(timestamp),
            CassandraByteUtil.bytesToByteBuffer(entryBytes))
    );
    Preconditions.checkNotNull(resultSet);

    // TODO: Anything here to flush the table or verify that this worked?
    //if (flush) { mSchemaIdTable.flushCommits(); }

    final ResultSet hashResultSet =
        mAdmin.execute(
            mPreparedStatementWriteHashTable.bind(
                CassandraByteUtil.bytesToByteBuffer(avroEntry.getHash().bytes()),
                new Date(timestamp),
                CassandraByteUtil.bytesToByteBuffer(entryBytes))
        );
    Preconditions.checkNotNull(hashResultSet);

    // TODO: Anything here to flush the table or verify that this worked?
    //if (flush) { mSchemaHashTable.flushCommits(); }
  }

  /**
   * Fetches a schema entry from the tables given a schema ID.
   *
   * @param schemaId schema ID
   * @return Avro schema entry, or null if the schema ID does not exist in the table
   * @throws java.io.IOException on I/O error.
   */
  private SchemaTableEntry loadFromIdTable(long schemaId) throws IOException {
    CassandraTableName tableName = mSchemaIdTable;

    // TODO: Prepare this statement once in constructor, not every load.
    final String queryText = String.format(
        "SELECT %s FROM %s WHERE %s=%d ORDER BY %s DESC LIMIT 1",
        SCHEMA_COLUMN_VALUE,
        tableName,
        SCHEMA_COLUMN_ID_KEY,
        schemaId,
        SCHEMA_COLUMN_TIME);
    final ResultSet resultSet = mAdmin.execute(queryText);
    final List<Row> rows = resultSet.all();

    if (0 == rows.size()) {
      return null;
    }

    assert(rows.size() == 1);
    final byte[] schemaAsBytes =
        CassandraByteUtil.byteBuffertoBytes(rows.get(0).getBytes(SCHEMA_COLUMN_VALUE));
    return decodeSchemaEntry(schemaAsBytes);
  }

  /**
   * Fetches a schema entry from the tables given a schema hash.
   *
   * @param schemaHash schema hash
   * @return Avro schema entry, or null if the schema hash does not exist in the table
   * @throws java.io.IOException on I/O error.
   */
  private SchemaTableEntry loadFromHashTable(BytesKey schemaHash) throws IOException {
    final ByteBuffer tableKey = CassandraByteUtil.bytesToByteBuffer(schemaHash.getBytes());
    final ResultSet resultSet = mAdmin.execute(mPreparedStatementReadHashTable.bind(tableKey));

    final List<Row> rows = resultSet.all();

    if (0 == rows.size()) {
      return null;
    }

    assert(rows.size() == 1);
    final byte[] schemaAsBytes =
        CassandraByteUtil.byteBuffertoBytes(rows.get(0).getBytes(SCHEMA_COLUMN_VALUE));
    return decodeSchemaEntry(schemaAsBytes);
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
    return SchemaTableEntry
        .newBuilder()
        .setId(entry.getId())
        .setHash(new MD5Hash(entry.getHash().getBytes()))
        .setAvroSchema(entry.getSchema().toString())
        .build();
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
    // TODO: Replace with actual C* code
    //mSchemaIdTable.flushCommits();
    //mSchemaHashTable.flushCommits();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void close() throws IOException {
    flush();
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close SchemaTable instance in state %s.", oldState);
    DebugResourceTracker.get().unregisterResource(this);
    mZKLock.close();
    mZKClient.close();
  }

  /**
   * Install the schema hash table.
   *
   * @param admin for the Kiji instance.
   * @param tableName name of the schema hash table to create.
   */
  private static void installHashTable(CassandraAdmin admin, CassandraTableName tableName) {
    // Let's try to make this somewhat readable...
    // TODO: Table should order by DESC for time
    final String tableDescription = String.format(
        "CREATE TABLE %s (%s blob, %s timestamp, %s blob, PRIMARY KEY (%s, %s));",
        tableName,
        SCHEMA_COLUMN_HASH_KEY,
        SCHEMA_COLUMN_TIME,
        SCHEMA_COLUMN_VALUE,
        SCHEMA_COLUMN_HASH_KEY,
        SCHEMA_COLUMN_TIME);
    admin.createTable(tableName, tableDescription);
  }

  /**
   * Install the schema ID table.
   *
   * @param admin for the Kiji instance.
   * @param tableName name of the schema ID table to create.
   */
  private static void installIdTable(CassandraAdmin admin, CassandraTableName tableName) {
    // TODO: Table should order by DESC for time
    final String tableDescription = String.format(
        "CREATE TABLE %s (%s bigint, %s timestamp, %s blob, PRIMARY KEY (%s, %s));",
        tableName,
        SCHEMA_COLUMN_ID_KEY,
        SCHEMA_COLUMN_TIME,
        SCHEMA_COLUMN_VALUE,
        SCHEMA_COLUMN_ID_KEY,
        SCHEMA_COLUMN_TIME);
    admin.createTable(tableName, tableDescription);
  }

  /**
   * Install the schema ID counter table.
   *
   * @param admin for the Kiji instance.
   * @param tableName name of the schema ID counter table to create.
   * @throws java.io.IOException if there is a problem creating the Cassandra table.
   */
  private static void installCounterTable(
      CassandraAdmin admin,
      CassandraTableName tableName) throws IOException {
    final String tableDescription = String.format(
        "CREATE TABLE %s (%s text PRIMARY KEY, %s counter);",
        tableName,
        SCHEMA_COUNTER_COLUMN_KEY,
        SCHEMA_COUNTER_COLUMN_VALUE);
    admin.createTable(tableName, tableDescription);

    // Now set the counter to zero
    final String updateQuery = String.format("UPDATE %s SET %s = %s + 0 WHERE %s='%s';",
        tableName,
        SCHEMA_COUNTER_COLUMN_VALUE,
        SCHEMA_COUNTER_COLUMN_VALUE,
        SCHEMA_COUNTER_COLUMN_KEY,
        SCHEMA_COUNTER_ONLY_KEY_VALUE);
    LOG.debug("Update query: {}.", updateQuery);
    admin.execute(updateQuery);

    // TODO: check if below is necessary, or leftover
    // Sanity check that counter value is 1!
    final String selectQuery = String.format("SELECT * FROM %s;", tableName);
    final ResultSet resultSet = admin.execute(selectQuery);
    final List<Row> rows = resultSet.all();
    assert(rows.size() == 1);
    final Row row = rows.get(0);
    final long counterValue = row.getLong(SCHEMA_COUNTER_COLUMN_VALUE);
    assert(0 == counterValue);
  }

  /**
   * Install the schema table into a Kiji instance.
   *
   * @param admin The C* Admin interface for the HBase cluster to install into.
   * @param kijiURI the KijiURI.
   * @throws java.io.IOException on I/O error.
   */
  public static void install(CassandraAdmin admin, KijiURI kijiURI) throws IOException {
    // Keep all versions of schema entries:
    //  - entries of the ID table should never be written more than once.
    //  - entries of the hash table could be written more than once:
    //      - with different schema IDs in some rare cases, for example when a client crashes
    //        while writing an entry.
    //      - with different schemas on MD5 hash collisions.

    installHashTable(admin, CassandraTableName.getSchemaHashTableName(kijiURI));
    installIdTable(admin, CassandraTableName.getSchemaIdTableName(kijiURI));
    installCounterTable(admin, CassandraTableName.getSchemaCounterTableName(kijiURI));

    final CassandraSchemaTable schemaTable = new CassandraSchemaTable(admin, kijiURI);
    try {
      schemaTable.registerPrimitiveSchemas();
    } finally {
      schemaTable.close();
    }
  }

  /**
   * Deletes a C* table.
   *
   * @param admin C* admin client.
   * @param tableName Name of the table to delete.
   */
  private static void deleteTable(CassandraAdmin admin, CassandraTableName tableName) {
    final String delete = CQLUtils.getDropTableStatement(tableName);
    admin.execute(delete);
  }

  /**
   * Disables and removes the schema table from HBase.
   *
   * @param admin The HBase Admin object.
   * @param kijiURI The KijiURI for the instance to remove.
   * @throws java.io.IOException If there is an error.
   */
  public static void uninstall(CassandraAdmin admin, KijiURI kijiURI)
      throws IOException {
    final CassandraTableName hashTableName = CassandraTableName.getSchemaHashTableName(kijiURI);
    deleteTable(admin, hashTableName);

    final CassandraTableName idTableName = CassandraTableName.getSchemaIdTableName(kijiURI);
    deleteTable(admin, idTableName);

    final CassandraTableName counterTableName =
        CassandraTableName.getSchemaCounterTableName(kijiURI);
    deleteTable(admin, counterTableName);
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
      final Set<SchemaEntry> hashTableEntries = loadSchemaHashTable();
      if (!checkConsistency(hashTableEntries)) {
        LOG.error("Schema hash table is inconsistent");
      }

      /** Entries from the schema ID table. */
      final Set<SchemaEntry> idTableEntries = loadSchemaIdTable();
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
      final Set<SchemaEntry> hashTableEntries = loadSchemaHashTable();

      /** Entries from the schema ID table. */
      final Set<SchemaEntry> idTableEntries = loadSchemaIdTable();

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
   * @throws java.io.IOException on I/O failure.
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
   * @return the set of schema entries from the schema hash table.
   * @throws java.io.IOException on I/O error.
   */
  private Set<SchemaEntry> loadSchemaHashTable() throws IOException {
    LOG.info("Loading entries from schema hash table.");
    final Set<SchemaEntry> entries = new HashSet<SchemaEntry>();
    int hashTableRowCounter = 0;

    // Fetch all of the schemas from the schema hash table (all versions)
    final String queryText = String.format("SELECT * FROM %s;", mSchemaHashTable);
    final ResultSet resultSet = mAdmin.execute(queryText);

    for (Row row : resultSet) {
      hashTableRowCounter += 1;

      // TODO: Not sure how to replicate this check in C*...
      /*
      if (result.getRow().length != Hasher.HASH_SIZE_BYTES) {
        LOG.error(String.format(
            "Invalid schema hash table row key size: %s, expecting %d bytes.",
            new BytesKey(result.getRow()), Hasher.HASH_SIZE_BYTES));
        continue;
      */

      // Get the row key, timestamp, and schema for this row
      final BytesKey rowKey =
          new BytesKey(CassandraByteUtil.byteBuffertoBytes(row.getBytes(SCHEMA_COLUMN_HASH_KEY)));
      final long timestamp = row.getDate(SCHEMA_COLUMN_TIME).getTime();
      final byte[] schemaAsBytes =
          CassandraByteUtil.byteBuffertoBytes(row.getBytes(SCHEMA_COLUMN_VALUE));

      try {
        final SchemaEntry entry = fromAvroEntry(decodeSchemaEntry(schemaAsBytes));
        entries.add(entry);
        if (!getSchemaHash(entry.getSchema()).equals(entry.getHash())) {
          LOG.error(
              "Invalid schema hash table entry: computed schema hash {} does not match entry {}.",
              getSchemaHash(entry.getSchema()), entry);
        }
        if (!rowKey.equals(entry.getHash())) {
          LOG.error(
              "Inconsistent schema hash table: hash encoded in row key {}"
                  + " does not match schema entry: {}.",
              rowKey, entry
          );
        }
      } catch (IOException ioe) {
        LOG.error("Unable to decode schema hash table entry for row {}, timestamp {}: {}.",
            rowKey, timestamp, ioe);
      } catch (AvroRuntimeException are) {
        LOG.error(
            "Unable to decode schema hash table entry for row {}, timestamp {}: {}.",
            rowKey, timestamp, are);
      }
    }
    LOG.info("Schema hash table has {} rows and {} entries.", hashTableRowCounter, entries.size());
    return entries;
  }

  /**
   * Loads and check the consistency of the schema ID table.
   *
   * @return the set of schema entries from the schema ID table.
   * @throws java.io.IOException on I/O error.
   */
  private Set<SchemaEntry> loadSchemaIdTable() throws IOException {
    LOG.info("Loading entries from schema ID table.");
    int idTableRowCounter = 0;
    final Set<SchemaEntry> entries = new HashSet<SchemaEntry>();

    // Fetch all of the schemas from the schema ID table (all versions)
    final String queryText = String.format("SELECT * FROM %s;", mSchemaIdTable);
    final ResultSet resultSet = mAdmin.execute(queryText);

    for (Row row : resultSet) {
      idTableRowCounter += 1;

      // Get the row key, timestamp, and schema for this row.  Use "Unsafe" version of method here
      // to get raw bytes no matter what format the field is in the C* table.
      final BytesKey rowKey = new BytesKey(
          CassandraByteUtil.byteBuffertoBytes(row.getBytesUnsafe(SCHEMA_COLUMN_ID_KEY)));

      long schemaId = -1;
      try {
        schemaId = row.getLong(SCHEMA_COLUMN_ID_KEY);
      } catch (InvalidTypeException exn) {
        LOG.error(String.format("Unable to decode schema ID encoded in row key %s: %s",
            rowKey, exn));
      }

      final long timestamp = row.getDate(SCHEMA_COLUMN_TIME).getTime();
      final byte[] schemaAsBytes =
          CassandraByteUtil.byteBuffertoBytes(row.getBytes(SCHEMA_COLUMN_VALUE));
      try {
        final SchemaEntry entry = fromAvroEntry(decodeSchemaEntry(schemaAsBytes));
        entries.add(entry);
        if (!getSchemaHash(entry.getSchema()).equals(entry.getHash())) {
          LOG.error(String.format("Invalid schema hash table entry with row key %s: "
              + "computed schema hash %s does not match entry %s",
              rowKey, getSchemaHash(entry.getSchema()), entry));
        }
        if (schemaId != entry.getId()) {
          LOG.error(
              "Inconsistent schema ID table: ID encoded in row key {} does not match entry: {}.",
              schemaId, entry);
        }
      } catch (IOException ioe) {
        LOG.error("Unable to decode schema ID table entry for row {}, timestamp {}: {}.",
            rowKey, timestamp, ioe);
      } catch (AvroRuntimeException are) {
        LOG.error("Unable to decode schema ID table entry for row {}, timestamp {}: {}.",
            rowKey, timestamp, are);
      }
    }
    LOG.info("Schema ID table has {} rows and {} entries.", idTableRowCounter, entries.size());
    return entries;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(CassandraSchemaTable.class)
        .add("uri", mInstanceURI)
        .add("state", mState.get())
        .toString();
  }
}
