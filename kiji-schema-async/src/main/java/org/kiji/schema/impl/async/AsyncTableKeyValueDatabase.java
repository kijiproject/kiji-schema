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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;
import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyOnlyFilter;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiTableKeyValueDatabase;
import org.kiji.schema.avro.KeyValueBackup;
import org.kiji.schema.avro.KeyValueBackupEntry;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

/**
* Manages key-value pairs on a per table basis. Storage of these key-value pairs is provided by
* a column family of an HTable.
*/
@ApiAudience.Private
public final class AsyncTableKeyValueDatabase
   implements KijiTableKeyValueDatabase<AsyncTableKeyValueDatabase> {

 public static final Logger LOG = LoggerFactory.getLogger(AsyncTableKeyValueDatabase.class);

 /** The name of the column family used to store the key-value database.*/
 private final String mFamily;

  /** The HBase column family, as bytes. */
 private final byte[] mFamilyBytes;

  /** The HBaseClient of the Kiji instance this table key-value database table belongs to. */
  private final HBaseClient mHBClient;

  /** HBase table name */
  private final byte[] mTableName;

 /**
  * This class manages the storage and retrieval of key-value pairs on a per table basis. It is
  * backed by a column family in HBase specified by metaFamily, in the table specified by table.
  *
  * @param hbClient HBaseClient to use to store the key-value information in a table.
  * @param tableName the name of the table to store the key-value information in.
  * @param metaFamily the name of the column family to use.
  */
 public AsyncTableKeyValueDatabase(HBaseClient hbClient, byte[] tableName, String metaFamily) {
   mHBClient = Preconditions.checkNotNull(hbClient);
   mTableName = Preconditions.checkNotNull(tableName);
   mFamily = Preconditions.checkNotNull(metaFamily);
   mFamilyBytes = Bytes.UTF8(mFamily);
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

   final byte[] bKey = Bytes.UTF8(key);
   final GetRequest get = new GetRequest(
      mTableName,
      Bytes.UTF8(table),
      mFamilyBytes,
      bKey);
   get.maxVersions(numVersions);
   /** List of values */
   final List<byte[]> values = Lists.newArrayList();
   try {
     final ArrayList<KeyValue> results = mHBClient.get(get).join();
     if (results.isEmpty()) {
       throw new IOException(
           String.format(
               "Could not find any values associated with table %s and key %s", table, key));
     }

     for (KeyValue column : results) {
       values.add(column.value());
     }
   } catch (Exception e) {
     ZooKeeperUtils.wrapAndRethrow(e);
   }
   return values;
 }

 /** {@inheritDoc} */
 @Override
 public NavigableMap<Long, byte[]> getTimedValues(String table, String key, int numVersions)
     throws IOException {
   Preconditions.checkArgument(numVersions >= 1,  "numVersions must be positive");
   final byte[] bKey = Bytes.UTF8(key);
   GetRequest get = new GetRequest(
       mTableName,
       Bytes.UTF8(table),
       mFamilyBytes,
       bKey);

   /** Map from timestamp to values. */
   final NavigableMap<Long, byte[]> timedValues = Maps.newTreeMap();

   try {
     ArrayList<KeyValue> results = mHBClient.get(get).join();

     for (KeyValue kv : results) {
       final long timestamp = kv.timestamp();
       final byte[] bytes = kv.value();
       Preconditions.checkState(timedValues.put(timestamp, bytes) == null);
     }
   } catch (Exception e) {
     ZooKeeperUtils.wrapAndRethrow(e);
   }
   return timedValues;
 }

 /** {@inheritDoc} */
 @Override
 public AsyncTableKeyValueDatabase putValue(String table, String key, byte[] value)
     throws IOException {
   PutRequest put = new PutRequest(
       mTableName,
       Bytes.UTF8(table),
       mFamilyBytes,
       Bytes.UTF8(key),
       value);
   try {
     mHBClient.put(put).join();
   } catch (Exception e) {
     ZooKeeperUtils.wrapAndRethrow(e);
   }
   return this;
 }

 /** {@inheritDoc} */
 @Override
 public void removeValues(String table, String key) throws IOException {
   DeleteRequest delete = new DeleteRequest(
       mTableName,
       Bytes.UTF8(table),
       mFamilyBytes,
       Bytes.UTF8(key));
   try {
     mHBClient.delete(delete).join();
   } catch (Exception e) {
     ZooKeeperUtils.wrapAndRethrow(e);
   }

 }

 /** {@inheritDoc} */
 @Override
 public Set<String> keySet(String table) throws IOException {
   Set<String> keys = new HashSet<String>();
   GetRequest get = new GetRequest(mTableName, Bytes.UTF8(table), mFamilyBytes);
   try {
     ArrayList<KeyValue> results = mHBClient.get(get).join();
     if (results.isEmpty()) {
       return keys;
     }
     for (KeyValue kv : results) {
       LOG.debug("When constructing the keySet for the meta table we found key {}.",
           new String(kv.qualifier()));
       keys.add(new String(kv.qualifier()));
     }
   } catch (Exception e) {
     ZooKeeperUtils.wrapAndRethrow(e);
   }
   return keys;
 }

 /** {@inheritDoc} */
 @Override
 public Set<String> tableSet() throws IOException {
   final Scanner scanner = mHBClient.newScanner(mTableName);
   scanner.setFamily(mFamilyBytes);
   scanner.setFilter(new KeyOnlyFilter());

   Set<String> tableNames = new HashSet<String>();
   try {
     ArrayList<ArrayList<KeyValue>> results = scanner.nextRows().join();
     if (null == results) {
       LOG.debug(
           "No results were returned when you scanned for the {} family",
           mFamilyBytes);
       return Collections.emptySet();
     }
     while (null != results) {
       for (ArrayList<KeyValue> row : results) {
         if (!row.isEmpty()) {
           String tableName = new String(row.get(0).key());
           LOG.debug(
               "When constructing the tableSet for the metatable we found table '{}'.",
               tableName);
           tableNames.add(tableName);
         }
       }
       results = scanner.nextRows().join();
     }
   } catch (Exception e) {
     ZooKeeperUtils.wrapAndRethrow(e);
   }

   scanner.close();
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
   ArrayList<Deferred<Object>> workers = Lists.newArrayList();
   for (KeyValueBackupEntry kvRecord : keyValueBackup.getKeyValues()) {
     final byte[] key = Bytes.UTF8(kvRecord.getKey());
     final ByteBuffer valueBuffer = kvRecord.getValue(); // Read in ByteBuffer of values
     byte[] value = new byte[valueBuffer.remaining()]; // Instantiate ByteArray
     valueBuffer.get(value); // Write buffer to array
     final long timestamp = kvRecord.getTimestamp();
     LOG.debug(String.format("For the table '%s' we are writing to family '%s', qualifier '%s'"
         + ", timestamp '%s', and value '%s' to the" + " meta table named '%s'.", tableName,
         new String(mFamilyBytes), new String(key), "" + timestamp, new String(value),
         new String(mTableName)));
     final PutRequest put = new PutRequest(
        mTableName,
         Bytes.UTF8(tableName),
         mFamilyBytes,
         key,
         value,
         timestamp);
     workers.add(mHBClient.put(put));
   }
   LOG.debug("Flushing commits to restore key-values from backup.");
   try {
     mHBClient.flush();
     Deferred.group(workers).join();
   } catch (Exception e) {
     ZooKeeperUtils.wrapAndRethrow(e);
   }
 }
}
