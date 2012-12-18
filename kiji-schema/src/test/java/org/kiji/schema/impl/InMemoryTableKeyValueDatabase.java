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
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiTableKeyValueDatabase;
import org.kiji.schema.KijiTableNotFoundException;

 /**
 * Manages key-value pairs on a per table basis. Storage of these key-value pairs is provided by
 * an in-memory map.
 */
public class InMemoryTableKeyValueDatabase implements KijiTableKeyValueDatabase {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryTableKeyValueDatabase.class);

  /** Map from table names to a Map of versioned Key-Value pairs used for storing metadata. */
  private final Map<String, NavigableMap<String, byte[]>> mMetaMap;

  /** Creates a new <code>InMemoryTableKeyValueDatabase</code> instance. */
  public InMemoryTableKeyValueDatabase() {
    this(new HashMap<String, NavigableMap<String, byte[]>>());
  }

  /**
   * Creates a new <code>InMemoryTableKeyValueDatabase</code> instance.
   *
   * @param metaDataMap A map to initialize your InMemoryTableKeyValuedatabase.
   */
  public InMemoryTableKeyValueDatabase(Map<String, NavigableMap<String, byte[]>> metaDataMap) {
    mMetaMap = metaDataMap;
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableKeyValueDatabase putValue(String table, String key, byte[] value)
      throws IOException {
    NavigableMap<String, byte[]> keyValueMap;
    if (mMetaMap.containsKey(table)) {
      keyValueMap = mMetaMap.get(table);
    } else {
        keyValueMap = new TreeMap<String, byte[]>();
    }
    keyValueMap.put(key, value);
    mMetaMap.put(table, keyValueMap);
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getValue(String table, String key) throws IOException {
    if (!mMetaMap.containsKey(table)) {
      LOG.debug("InMemoryTableKeyValueDatabase doesnt contain info for table {}", table);
      return null;
    }
    return mMetaMap.get(table).get(key);

  }


  /** {@inheritDoc} */
  @Override
  public void removeValues(String table, String key) throws IOException {
    mMetaMap.get(table).remove(key);
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> keySet(String table) throws KijiTableNotFoundException {
   if (mMetaMap.containsKey(table)) {
     Map<String, byte[]> tableMap = mMetaMap.get(table);
     return tableMap.keySet();
   } else {
     throw new KijiTableNotFoundException("The InMemoryTableKeyValueDatabase doesn't contain info"
       + "for table " + table);
   }
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
  public Set<String> tableSet() throws IOException {
    return mMetaMap.keySet();
  }

}
