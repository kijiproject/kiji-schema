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

package org.kiji.schema;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import com.google.common.base.Objects;
import org.apache.avro.Schema;
import org.apache.avro.util.WeakIdentityHashMap;
import org.apache.commons.lang.builder.HashCodeBuilder;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.avro.SchemaTableBackup;
import org.kiji.schema.util.BytesKey;
import org.kiji.schema.util.Hasher;

/**
 * The Kiji schema table, which contains the lookup table between schema IDs, hashes, and full
 * schemas.
 *
 * @see KijiMetaTable
 * @see KijiSystemTable
 */
@ApiAudience.Framework
@ApiStability.Stable
@Inheritance.Sealed
public interface KijiSchemaTable extends Flushable, Closeable {
  /**
   * Looks up a schema ID given an Avro schema object.
   *
   * If the schema is unknown, allocates a new ID and stores the new schema mapping.
   *
   * @param schema The full schema to store in the table.
   * @return The schema ID.
   * @throws IOException on I/O error.
   */
  long getOrCreateSchemaId(Schema schema) throws IOException;

  /**
   * Looks up a schema hash given an Avro schema object.
   *
   * If the schema is unknown, allocates a new ID and stores the new schema mapping.
   *
   * @param schema Avro schema to look up.
   * @return The schema hash.
   * @throws IOException on I/O error.
   */
  BytesKey getOrCreateSchemaHash(Schema schema) throws IOException;

  /**
   * Computes a schema hash.
   *
   * @param schema The Avro schema to hash.
   * @return The hash of the schema
   */
  BytesKey getSchemaHash(Schema schema);

  /**
   * Looks up a schema given an ID.
   *
   * @param schemaId Schema ID to look up.
   * @return Avro schema, or null if the schema ID is unknown.
   * @throws IOException on I/O error.
   */
  Schema getSchema(long schemaId) throws IOException;

  /**
   * Looks up a schema given a hash.
   *
   * @param schemaHash Schema hash to look up.
   * @return Avro schema, or null if the schema hash is unknown.
   * @throws IOException on I/O error.
   */
  Schema getSchema(BytesKey schemaHash) throws IOException;

  /**
   * Looks up a schema entry from a given ID.
   *
   * @param schemaId Schema ID to look up.
   * @return SchemaEntry matching the given ID or null if the ID is unknown.
   * @throws IOException on I/O error.
   */
  SchemaEntry getSchemaEntry(long schemaId) throws IOException;

  /**
   * Looks up a schema entry from a given hash.
   *
   * @param schemaHash Schema hash to look up.
   * @return SchemaEntry matching the given hash or null if the hash is unknown.
   * @throws IOException on I/O error.
   */
  SchemaEntry getSchemaEntry(BytesKey schemaHash) throws IOException;

  /**
   * Looks up a schema entry from a given Schema definition.
   *
   * @param schema Schema definition to look up.
   * @return SchemaEntry matching the given Schema or null if the schema is unknown.
   * @throws IOException on I/O error.
   */
  SchemaEntry getSchemaEntry(Schema schema) throws IOException;

  /** Association between a schema and its ID. */
  public static class SchemaEntry {
    private final long mId;
    private final BytesKey mHash;
    private final Schema mSchema;

    /**
     * Creates a new schema entry.
     *
     * @param id the schema ID
     * @param hash the schema hash
     * @param schema the Avro schema object
     */
    @ApiAudience.Private
    public SchemaEntry(long id, BytesKey hash, Schema schema) {
      this.mId = id;
      this.mHash = hash;
      this.mSchema = schema;
    }

    /** @return the schema ID */
    public long getId() {
      return this.mId;
    }

    /** @return the schema hash */
    public BytesKey getHash() {
      return this.mHash;
    }

    /** @return the Avro schema object */
    public Schema getSchema() {
      return this.mSchema;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return Objects.toStringHelper(SchemaEntry.class)
                 .add("id", this.mId)
                 .add("hash", this.mHash)
                 .add("schema", this.mSchema)
                 .toString();
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object other) {
      if (!(other instanceof SchemaEntry)) {
        return false;
      }
      final SchemaEntry entry = (SchemaEntry) other;
      return (this.mId == entry.mId)
                 && (this.mHash.equals(entry.mHash))
                 && (this.mSchema.equals(entry.mSchema));
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return new HashCodeBuilder()
                 .append(mId)
                 .append(mHash)
                 .append(mSchema)
                 .toHashCode();
    }
  }

  /**
   * Cache providing an efficient mapping from Avro schema object to the schema hash.
   *
   * Computing the hash code of a schema is expensive as it serializes the Avro schema object
   * into JSON.
   */
  @ApiAudience.Private
  static final class SchemaHashCache {
    /**
     * Underlying cache is a weak identity hash map:
     * <li> We must use object IDs since Schema.hashCode() and Schema.equals() implement a
     *      comparison that ignores doc fields or default values.
     * <li> We must use a weak map to ensure the cache gets garbage collected properly.
     */
    private final Map<Schema, BytesKey> mCache =
        Collections.synchronizedMap(new WeakIdentityHashMap<Schema, BytesKey>());

    /**
     * Computes a hash of the specified Avro schema.
     *
     * Kiji currently uses MD5 sums (128 bits) of the schema JSON representation.
     *
     * @param schema Avro schema to compute a hash of.
     * @return Hash code as an array of bytes (16 bytes).
     */
    public static byte[] hashSchema(Schema schema) {
      return Hasher.hash(schema.toString());
    }

    /**
     * Hashes an Avro schema.
     *
     * @param schema Avro schema to hash.
     * @return the schema hash.
     */
    public BytesKey getHash(Schema schema) {
      final BytesKey hash = mCache.get(schema);
      if (null != hash) {
        return hash;
      }
      final BytesKey newHash = new BytesKey(hashSchema(schema));
      mCache.put(schema, newHash);
      return newHash;
    }
  }

  /**
   * Returns schema backup information in a form that can be directly written to a MetadataBackup
   * record. To read more about the avro type that has been specified to store this info, see
   * Layout.avdl
   *
   * @throws IOException on I/O error.
   * @return A list of schema table entries.
   */
  SchemaTableBackup toBackup() throws IOException;

  /**
   * Restores the schema entries from the specified backup record.
   *
   * @param backup The schema entries from a MetadataBackup record. This consist of the schema
   *     definition, schema id, and schema hash.
   * @throws IOException on I/O error.
   */
  void fromBackup(SchemaTableBackup backup) throws IOException;
}
