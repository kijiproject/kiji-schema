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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiTableKeyValueDatabase;
import org.kiji.schema.KijiTableNotFoundException;


 /**
 * Manages key-value pairs on a per table basis. Storage of these key-value pairs is provided by
 * a column family of an HTable.
 */
public class HBaseTableKeyValueDatabase implements KijiTableKeyValueDatabase {
  public static final Logger LOG = LoggerFactory.getLogger(HBaseTableKeyValueDatabase.class);

  /** The name of the column family used to store the key-value database.*/
  private final String mFamily;

   /** The HBase column family, as bytes. */
  private final byte[] mFamilyBytes;

  /**  The HBase table that stores Kiji metadata. */
  private final HTable mTable;
  /**
   * This class manages the storage and retrieval of key-value pairs on a per table basis. It is
   * backed by a column family in HBase specified by metaFamily, in the table specified by table.
   *
   * @param hTable The table to store the key-value information in.
   * @param metaFamily the name of the column family to use.
   */
  public HBaseTableKeyValueDatabase(HTable hTable, String metaFamily) {
    mTable = Preconditions.checkNotNull(hTable);
    mFamily = Preconditions.checkNotNull(metaFamily);
    mFamilyBytes =  Bytes.toBytes(mFamily);
  }


/** {@inheritDoc} */
  @Override
  public byte[] getValue(String table, String key) throws IOException {
    Get get = new Get(Bytes.toBytes(table));
    byte[] bKey = Bytes.toBytes(key);
    get.addColumn(mFamilyBytes, bKey);
    Result result = mTable.get(get);
    if (!result.containsColumn(mFamilyBytes, bKey)) {
      throw new IOException(String.format("Unable to find value for table '%s' and key '%s'", table,
          key));
    }
    return result.getValue(mFamilyBytes, bKey);
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableKeyValueDatabase putValue(String table, String key, byte[] value)
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
      throw new KijiTableNotFoundException("Unable to find any keys for table" + table);
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
      LOG.debug("When constructing the tableSet for the meta table we founed table {}.", tableName);
      tableNames.add(tableName);
    }
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

}
