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

package org.kiji.schema.layout.impl;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.SchemaStorage;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.SchemaClassNotFoundException;
import org.kiji.schema.util.JavaIdentifiers;

/**
 * Specification of a Kiji cell.
 *
 * Contains everything needed to encode or decode a given column.
 * Wraps a CellSchema Avro record to avoid re-parsing JSON Avro schemas every time, and
 * associate it with a schema table to resolve schema IDs or hashes.
 */
@ApiAudience.Private
public final class CellSpec {
  /** Avro record specifying the cell encoding. */
  private CellSchema mCellSchema;

  /** For Avro encoded cells, decode the cell content in this reader schema. */
  private Schema mReaderSchema;

  /** Schema table to resolve schema hashes or UIDs. */
  private KijiSchemaTable mSchemaTable;

  /** @return a new CellSpec for a counter cell. */
  public static CellSpec newCounter() {
    return new CellSpec()
        .setType(SchemaType.COUNTER);
  }

  /**
   * @return a new CellSpec initialized from a CellSchema Avro descriptor.
   * @param cellSchema CellSchema record to initialize the CellSpec from.
   * @param schemaTable Schema table to resolve schema hashes or UIDs.
   * @throws InvalidLayoutException if the cell specification is invalid.
   */
  public static CellSpec fromCellSchema(CellSchema cellSchema, KijiSchemaTable schemaTable)
      throws InvalidLayoutException {
    return fromCellSchema(cellSchema)
        .setSchemaTable(schemaTable);
  }

  /**
   * Creates a CellSpec from a CellSchema record.
   *
   * @param cellSchema CellSchema record.
   * @return a new CellSpec initialized from the given CellSchema.
   * @throws InvalidLayoutException on layout error.
   */
  public static CellSpec fromCellSchema(CellSchema cellSchema)
      throws InvalidLayoutException {
    return new CellSpec()
        .setCellSchema(cellSchema);
  }

  /** Initializes a new, unspecified CellSpec. */
  public CellSpec() {
  }

  /**
   * Sets the cell schema.
   *
   * @param cellSchema CellSchema to initialize the CellSpec from.
   * @return this.
   * @throws InvalidLayoutException on layout error.
   */
  public CellSpec setCellSchema(CellSchema cellSchema) throws InvalidLayoutException {
    mCellSchema = cellSchema;
    if (isAvro()) {
      setReaderSchema(readAvroSchema(cellSchema));
    }
    return this;
  }

  /**
   * Sets the schema storage.
   *
   * Does not make sense for counter cells.
   *
   * @param schemaStorage Schema storage (hash, UID or final).
   * @return this.
   */
  public CellSpec setSchemaStorage(SchemaStorage schemaStorage) {
    getOrCreateCellSchema().setStorage(schemaStorage);
    Preconditions.checkState(!isCounter());
    return this;
  }

  /**
   * Sets the cell type.
   *
   * @param schemaType Cell type (counter, inline Avro or Avro class).
   * @return this.
   */
  public CellSpec setType(SchemaType schemaType) {
    getOrCreateCellSchema().setType(schemaType);
    return this;
  }

  /**
   * Sets the Avro reader schema.
   *
   * Does not make sense for counter cells.
   *
   * @param readerSchema Avro reader schema.
   * @return this.
   */
  public CellSpec setReaderSchema(Schema readerSchema) {
    mReaderSchema = readerSchema;
    Preconditions.checkState(!isCounter());
    return this;
  }

  /**
   * Sets the schema table.
   *
   * @param schemaTable Schema table.
   * @return this.
   */
  public CellSpec setSchemaTable(KijiSchemaTable schemaTable) {
    mSchemaTable = schemaTable;
    return this;
  }

  /** @return the schema table to resolve schema hashes or IDs. */
  public KijiSchemaTable getSchemaTable() {
    return mSchemaTable;
  }

  /** @return whether this cell is a counter. */
  public boolean isCounter() {
    Preconditions.checkNotNull(mCellSchema);
    return mCellSchema.getType() == SchemaType.COUNTER;
  }

  /** @return whether this cell is encoded with Avro. */
  public boolean isAvro() {
    Preconditions.checkNotNull(mCellSchema);
    return (mCellSchema.getType() == SchemaType.INLINE)
        || (mCellSchema.getType() == SchemaType.CLASS);
  }

  /** @return the underlying CellSchema Avro record. May be null. */
  public CellSchema getCellSchema() {
    return mCellSchema;
  }

  /** @return the underlying CellSchema Avro record, creating it if necessary. */
  private CellSchema getOrCreateCellSchema() {
    if (null == mCellSchema) {
      mCellSchema = new CellSchema();
    }
    return mCellSchema;
  }


  /** @return the cell Avro schema. Valid for Avro cells only. */
  public Schema getAvroSchema() {
    Preconditions.checkState(isAvro());
    return mReaderSchema;
  }

  /**
   * Reads the Avro schema from the table layout.
   *
   * @param cellSchema The portion of the table layout record to read from.
   * @return the Avro schema, or null for a counter.
   * @throws InvalidLayoutException if the specification or the schema is invalid.
   */
  public static Schema readAvroSchema(CellSchema cellSchema) throws InvalidLayoutException {
    switch (cellSchema.getType()) {
    case INLINE: {
      try {
        return new Schema.Parser().parse(cellSchema.getValue());
      } catch (RuntimeException e) {
        throw new InvalidLayoutException("Invalid schema: " + e.getMessage());
      }
    }
    case CLASS: {
      String className = cellSchema.getValue();
      if (!JavaIdentifiers.isValidClassName(className)) {
        throw new InvalidLayoutException(
            "Schema with type 'class' must be a valid Java identifier.");
      }
      try {
        Class<?> avroClass = Class.forName(className);
        try {
          return SpecificData.get().getSchema(avroClass);
        } catch (RuntimeException e) {
          throw new InvalidLayoutException(
              "Java class is not a valid Avro type: " + avroClass.getName());
        }
      } catch (ClassNotFoundException e) {
        throw new SchemaClassNotFoundException(
            "Java class " + cellSchema.getValue() + " was not found on the classpath.");
      }
    }
    case COUNTER: {
      // Counters are coded using a Bytes.toBytes(long) and Bytes.toLong(byte[]):
      return null;
    }
    default:
      throw new InvalidLayoutException("Invalid schema type: " + cellSchema.getType());
    }
  }

}
