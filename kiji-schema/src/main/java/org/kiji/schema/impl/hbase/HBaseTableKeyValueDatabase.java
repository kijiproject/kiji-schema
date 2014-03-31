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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiTableKeyValueDatabase;
import org.kiji.schema.avro.KeyValueBackup;
import org.kiji.schema.avro.KeyValueBackupEntry;
import org.kiji.schema.util.ResourceUtils;

 /**
 * Manages key-value pairs on a per table basis. Storage of these key-value pairs is provided by
 * a column family of an HTable.
 */
@ApiAudience.Private
public final class HBaseTableKeyValueDatabase
    implements KijiTableKeyValueDatabase<HBaseTableKeyValueDatabase> {

  public static final Logger LOG = LoggerFactory.getLogger(HBaseTableKeyValueDatabase.class);

  /** The name of the column family used to store the key-value database.*/
  private final String mFamily;

   /** The HBase column family, as bytes. */
  private final byte[] mFamilyBytes;

  /**  The HBase table that stores Kiji metadata. */
  private final HTableInterface mTable;

  /**
   * This class manages the storage and retrieval of key-value pairs on a per table basis. It is
   * backed by a column family in HBase specified by metaFamily, in the table specified by table.
   *
   * @param hTable The table to store the key-value information in.
   * @param metaFamily the name of the column family to use.
   */
  public HBaseTableKeyValueDatabase(HTableInterface hTable, String metaFamily) {
    mTable = Preconditions.checkNotNull(hTable);
    mFamily = Preconditions.checkNotNull(metaFamily);
    mFamilyBytes =  Bytes.toBytes(mFamily);
  }


  /** {@inheritDoc} */
  @Override
  public byte[] getValue(String table, String key) throws IOException {
    final List<byte[]> values = getValues(table, key, 1);
    return values.get(0);
  }

  /** {@inheritDoc} */
  @Override
  public List<byte[]> getValues(String table, String key, int numVersions) throws IOException {
    Preconditions.checkArgument(numVersions >= 1,  "numVersions must be positive");

    final byte[] bKey = Bytes.toBytes(key);
    final Get get = new Get(Bytes.toBytes(table))
        .addColumn(mFamilyBytes, bKey)
        .setMaxVersions(numVersions);
    final Result result = mTable.get(get);
    if (result.isEmpty()) {
      throw new IOException(String.format(
          "Could not find any values associated with table %s and key %s", table, key));
    }
    /** List of values, ordered from  */
    final List<byte[]> values = Lists.newArrayList();
    for (KeyValue column : result.getColumn(mFamilyBytes, bKey)) {
      values.add(column.getValue());
    }
    return values;
  }

  /** {@inheritDoc} */
  @Override
  public NavigableMap<Long, byte[]> getTimedValues(String table, String key, int numVersions)
      throws IOException {
    Preconditions.checkArgument(numVersions >= 1,  "numVersions must be positive");
    final byte[] bKey = Bytes.toBytes(key);
    Get get = new Get(Bytes.toBytes(table))
        .addColumn(mFamilyBytes, bKey);
    Result result = mTable.get(get);

    /** Map from timestamp to values. */
    final NavigableMap<Long, byte[]> timedValues = Maps.newTreeMap();

    // Pull out the full map: family -> qualifier -> timestamp -> TableLayoutDesc.
    // Family and qualifier are already specified : the 2 outer maps must be size 11.
    final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap =
        result.getMap();
    Preconditions.checkState(familyMap.size() == 1);
    final NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifierMap =
        familyMap.get(familyMap.firstKey());
    Preconditions.checkState(qualifierMap.size() == 1);
    final NavigableMap<Long, byte[]> timeSerieMap = qualifierMap.get(qualifierMap.firstKey());
    for (Map.Entry<Long, byte[]> timeSerieEntry : timeSerieMap.entrySet()) {
      final long timestamp = timeSerieEntry.getKey();
      final byte[] bytes = timeSerieEntry.getValue();
      Preconditions.checkState(timedValues.put(timestamp, bytes) == null);
    }
    return timedValues;
  }

  /** {@inheritDoc} */
  @Override
  public HBaseTableKeyValueDatabase putValue(String table, String key, byte[] value)
      throws IOException {
    Put put = new Put(Bytes.toBytes(table));
    put.add(mFamilyBytes, Bytes.toBytes(key), value);
    mTable.put(put);
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public void removeValues(String table, String key) throws IOException {
    Delete delete = new Delete(Bytes.toBytes(table));
    delete.deleteColumns(mFamilyBytes, Bytes.toBytes(key));
    mTable.delete(delete);
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> keySet(String table) throws IOException {
    Set<String> keys = new HashSet<String>();
    Get get = new Get(Bytes.toBytes(table));
    get.addFamily(mFamilyBytes);
    Result result = mTable.get(get);
    if (result.isEmpty()) {
      return keys;
    }
    for (byte[] qualifier : result.getFamilyMap(mFamilyBytes).navigableKeySet()) {

      LOG.debug("When constructing the keySet for the meta table we found key {}.",
          Bytes.toString(qualifier));
      keys.add(Bytes.toString(qualifier));
    }
    return keys;
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> tableSet() throws IOException {
    Scan scan = new Scan();
    scan.addFamily(mFamilyBytes).setFilter(new KeyOnlyFilter());
    ResultScanner resultScanner = mTable.getScanner(scan);
    if (null == resultScanner) {
      LOG.debug("No results were returned when you scanned for the {} family",
         mFamilyBytes);
      return Collections.emptySet();
    }
    Set<String> tableNames = new HashSet<String>();
    for (Result result : resultScanner) {
      String tableName = Bytes.toString(result.getRow());
      LOG.debug("When constructing the tableSet for the metatable we found table '{}'.", tableName);
      tableNames.add(tableName);
    }
    ResourceUtils.closeOrLog(resultScanner);
    return tableNames;

  }

  /** {@inheritDoc} */
  @Override
  public void removeAllValues(String table) throws IOException {
    Set<String> keysToRemove = keySet(table);
    for (String key : keysToRemove) {
      removeValues(table, key);
    }
  }

  /** {@inheritDoc} */
  @Override
  public KeyValueBackup keyValuesToBackup(String table) throws IOException {
    List<KeyValueBackupEntry> kvBackupEntries = Lists.newArrayList();
    final Set<String> keys = keySet(table);
    for (String key : keys) {
      NavigableMap<Long, byte[]> versionedValues = getTimedValues(table, key, Integer.MAX_VALUE);
      for (Long timestamp : versionedValues.descendingKeySet()) {
        kvBackupEntries.add(KeyValueBackupEntry.newBuilder()
            .setKey(key)
            .setValue(ByteBuffer.wrap(versionedValues.get(timestamp)))
            .setTimestamp(timestamp)
            .build());
      }
    }
    KeyValueBackup kvBackup = KeyValueBackup.newBuilder().setKeyValues(kvBackupEntries).build();
    return kvBackup;
  }

  /** {@inheritDoc} */
  @Override
  public void restoreKeyValuesFromBackup(final String tableName, KeyValueBackup keyValueBackup)
      throws IOException {
    LOG.debug(String.format("Restoring '%s' key-value(s) from backup for table '%s'.",
        keyValueBackup.getKeyValues().size(), tableName));
    for (KeyValueBackupEntry kvRecord : keyValueBackup.getKeyValues()) {
      final byte[] key = Bytes.toBytes(kvRecord.getKey());
      final ByteBuffer valueBuffer = kvRecord.getValue(); // Read in ByteBuffer of values
      byte[] value = new byte[valueBuffer.remaining()]; // Instantiate ByteArray
      valueBuffer.get(value); // Write buffer to array
      final long timestamp = kvRecord.getTimestamp();
      LOG.debug(String.format("For the table '%s' we are writing to family '%s', qualifier '%s'"
          + ", timestamp '%s', and value '%s' to the" + " meta table named '%s'.", tableName,
          Bytes.toString(mFamilyBytes), Bytes.toString(key), "" + timestamp, Bytes.toString(value),
          Bytes.toString(mTable.getTableName())));
      final Put put = new Put(Bytes.toBytes(tableName)).add(mFamilyBytes, key, timestamp, value);
      mTable.put(put);
    }
    LOG.debug("Flushing commits to restore key-values from backup.");
    mTable.flushCommits();
  }
}
