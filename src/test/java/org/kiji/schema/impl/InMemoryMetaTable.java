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
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiTableKeyValueDatabase;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.avro.MetadataBackup;
import org.kiji.schema.avro.TableBackup;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.InMemoryTableLayoutDatabase;

/**
 * <p>A {@link org.kiji.schema.KijiMetaTable} implementation that is held
 * completely in memory.  This is mostly used for testing.</p>
 *
 * <p>This class doesn't have to do much because a meta table is just
 * a thin wrapper around a layout table and a key value store for whatever
 * other meta data users want to store on a per table basis. </p>
 */
public class InMemoryMetaTable extends KijiMetaTable {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryMetaTable.class);
  /**
   * An in-memory layout table.
   */
  private final InMemoryTableLayoutDatabase mTableLayoutDatabase;

    /** An in-memory Key-Value database, organized by table. */
  private final InMemoryTableKeyValueDatabase mTableKeyValueDatabase;

  /**
   * Construct a new empty meta table.
   */
  public InMemoryMetaTable() {
    this(new InMemoryTableLayoutDatabase(), new InMemoryTableKeyValueDatabase());
  }

  /**
   * Wrap an existing generic key-value table database and layout table database.
   *
   * @param tableKeyValueDatabase A map from table names to a map from string valued keys to byte[]
   *   valued values.
   * @param tableLayoutDatabase An in-memory Kiji table layout database.
   */
  public InMemoryMetaTable(InMemoryTableLayoutDatabase tableLayoutDatabase,
      InMemoryTableKeyValueDatabase tableKeyValueDatabase) {
    mTableLayoutDatabase = tableLayoutDatabase;
    mTableKeyValueDatabase = tableKeyValueDatabase;
  }


  /** {@inheritDoc} */
  @Override
  public void deleteTable(String table) throws IOException {
    mTableLayoutDatabase.removeAllTableLayoutVersions(table);
    try {
     mTableKeyValueDatabase.removeAllValues(table);
    } catch (KijiTableNotFoundException e) {
     LOG.info("No generic key-value data was found while deleting {}.", table);
    }
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableLayout updateTableLayout(String table, TableLayoutDesc layoutUpdate)
      throws IOException {
    return mTableLayoutDatabase.updateTableLayout(table, layoutUpdate);
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableLayout getTableLayout(String table) throws IOException {
    return mTableLayoutDatabase.getTableLayout(table);
  }

  /** {@inheritDoc} */
  @Override
  public List<KijiTableLayout> getTableLayoutVersions(String table, int numVersions)
      throws IOException {
    return mTableLayoutDatabase.getTableLayoutVersions(table, numVersions);
  }

  /** {@inheritDoc} */
  @Override
  public NavigableMap<Long, KijiTableLayout>
      getTimedTableLayoutVersions(String table, int numVersions) throws IOException {
    return mTableLayoutDatabase.getTimedTableLayoutVersions(table, numVersions);
  }

  /** {@inheritDoc} */
  @Override
  public void removeAllTableLayoutVersions(String table) throws IOException {
    mTableLayoutDatabase.removeAllTableLayoutVersions(table);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void removeRecentTableLayoutVersions(String table, int numVersions)
      throws IOException {
    mTableLayoutDatabase.removeRecentTableLayoutVersions(table, numVersions);
  }

  /** {@inheritDoc} */
  @Override
  public List<String> listTables() throws IOException {
    return mTableLayoutDatabase.listTables();
  }

  /** {@inheritDoc} */
  @Override
  public void writeToBackup(MetadataBackup.Builder backup) throws IOException {
    mTableLayoutDatabase.writeToBackup(backup);
  }

  /** {@inheritDoc} */
  @Override
  public void restoreFromBackup(MetadataBackup backup) throws IOException {
    mTableLayoutDatabase.restoreFromBackup(backup);
  }

  /** {@inheritDoc} */
  @Override
  public void restoreTableFromBackup(TableBackup tableBackup) throws IOException {
    mTableLayoutDatabase.restoreTableFromBackup(tableBackup);
  }

    /** {@inheritDoc} */
  @Override
  public KijiTableKeyValueDatabase putValue(String table, String key, byte[] value)
      throws IOException {
    return mTableKeyValueDatabase.putValue(table, key, value);
  }

    /** {@inheritDoc} */
  @Override
  public byte[] getValue(String table, String key) throws IOException {
    return mTableKeyValueDatabase.getValue(table, key);
  }

    /** {@inheritDoc} */
  @Override
  public void removeValues(String table, String key) throws IOException {
    mTableKeyValueDatabase.removeValues(table, key);
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> keySet(String table) throws IOException {
    return mTableKeyValueDatabase.keySet(table);
  }

  /** {@inheritDoc} */
  @Override
  public void removeAllValues(String table) throws IOException {
    mTableKeyValueDatabase.removeAllValues(table);
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> tableSet() throws IOException {
    return mTableKeyValueDatabase.tableSet();
  }

}
