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

import org.apache.avro.Schema;
import org.apache.commons.lang.NotImplementedException;

import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.avro.MetadataBackup;
import org.kiji.schema.util.BytesKey;


/**
 * A mock version of the schema table that only exists in memory.
 */
public class InMemorySchemaTable extends KijiSchemaTable {

  /** Map from schema ID to schema hash. */
  private Map<Long, SchemaEntry> mSchemaIdMap;

  /** Map schema hash to schema. */
  private Map<BytesKey, SchemaEntry> mSchemaHashMap;

  /**
   * Creates a new empty <code>InMemorySchemaTable</code> instance.
   */
  public InMemorySchemaTable() {
    mSchemaIdMap = new HashMap<Long, SchemaEntry>();
    mSchemaHashMap = new HashMap<BytesKey, SchemaEntry>();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized long getOrCreateSchemaId(Schema schema) {
    final BytesKey schemaHash = getSchemaHash(schema);
    SchemaEntry entry = mSchemaHashMap.get(schemaHash);
    if (entry == null) {
      long schemaId = mSchemaHashMap.size();
      entry = new SchemaEntry(schemaId, schemaHash, schema);
      mSchemaHashMap.put(schemaHash, entry);
      mSchemaIdMap.put(schemaId, entry);
    }
    return entry.getId();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized BytesKey getOrCreateSchemaHash(Schema schema) {
    final BytesKey schemaHash = getSchemaHash(schema);
    SchemaEntry entry = mSchemaHashMap.get(schemaHash);
    if (entry == null) {
      long schemaId = mSchemaHashMap.size();
      entry = new SchemaEntry(schemaId, schemaHash, schema);
      mSchemaHashMap.put(schemaHash, entry);
      mSchemaIdMap.put(schemaId, entry);
    }
    return entry.getHash();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized Schema getSchema(long schemaId) {
    return mSchemaIdMap.get(schemaId).getSchema();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized Schema getSchema(BytesKey schemaHash) {
    final SchemaEntry entry = mSchemaHashMap.get(schemaHash);
    return (entry != null) ? entry.getSchema() : null;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void close() throws IOException {
    // Nothing to do.
  }

  /** {@inheritDoc} */
  @Override
  public void writeToBackup(MetadataBackup backup) throws IOException {
    throw new NotImplementedException("Schema table backups are not implemented in this mock");
  }

  /** {@inheritDoc} */
  @Override
  public void restoreFromBackup(MetadataBackup backup) throws IOException {
    throw new NotImplementedException("Schema table backups are not implemented in this mock");
  }
}
