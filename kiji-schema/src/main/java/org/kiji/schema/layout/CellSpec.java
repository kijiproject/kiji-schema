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

package org.kiji.schema.layout;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.GenericCellDecoderFactory;
import org.kiji.schema.KijiCellDecoderFactory;
import org.kiji.schema.KijiCellEncoderFactory;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.SpecificCellDecoderFactory;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.SchemaStorage;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.util.JavaIdentifiers;

/**
 * Specification of a column in a Kiji table.
 *
 * <p> Contains everything needed to encode or decode a given column.
 *   Wraps a CellSchema Avro record to avoid re-parsing JSON Avro schemas every time, and
 *   associates it with a schema table to resolve schema IDs or hashes.
 * </p>
 * <p> A CellSpec is constructed from a {@link KijiTableLayout}
 *    and is intended for a consumption by {@link org.kiji.schema.KijiCellEncoder}
 *    and {@link org.kiji.schema.KijiCellDecoder}.
 * </p>
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class CellSpec {
  /** Avro record specifying the cell encoding. */
  private CellSchema mCellSchema;

  /**
   * For Avro-encoded cells, decode the cell content with this reader schema.
   * Null means use the writer schema.
   * Also null for non Avro-encoded cells.
   */
  private Schema mReaderSchema;

  /** Schema table to resolve schema hashes or UIDs. */
  private KijiSchemaTable mSchemaTable;

  /** Factory for cell decoders (either specific or generic). */
  private KijiCellDecoderFactory mDecoderFactory = SpecificCellDecoderFactory.get();

  /** Factory for cell encoders. */
  private KijiCellEncoderFactory mEncoderFactory = DefaultKijiCellEncoderFactory.get();

  /**
   * Returns a new CellSpec for a counter cell.
   *
   * @return a new CellSpec for a counter cell.
   */
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

  /**
   * Returns a new unspecified CellSpec.
   *
   * <p> Use the setters to populate the necessary fields. </p>
   *
   * @return a new unspecified CellSpec.
   */
  public static CellSpec create() {
    return new CellSpec();
  }

  /**
   * Makes a copy of an existing CellSpec.
   *
   * @param spec Existing CellSpec to copy.
   * @return a copy of the specified CellSpec.
   */
  public static CellSpec copy(CellSpec spec) {
    final CellSpec copy = new CellSpec();
    copy.mCellSchema = CellSchema.newBuilder(spec.mCellSchema).build();
    copy.mReaderSchema = spec.mReaderSchema;
    copy.mSchemaTable = spec.mSchemaTable;
    copy.mDecoderFactory = spec.mDecoderFactory;
    return copy;
  }

  /** Initializes a new unspecified CellSpec. */
  private CellSpec() {
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
      try {
        setReaderSchema(readAvroSchema(cellSchema));
      } catch (SchemaClassNotFoundException scnfe) {
        setReaderSchema(null);  // use writer schema
      }
    }
    return this;
  }

  /**
   * Sets the schema storage.
   *
   * <p> Does not make sense for counter cells. </p>
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
   * Sets the cell type (counter, inline Avro or Avro class).
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
   * <p> Invalid for counter cells. </p>
   *
   * @param readerSchema Avro reader schema.
   * @return this CellSpec.
   */
  public CellSpec setReaderSchema(Schema readerSchema) {
    Preconditions.checkState(!isCounter());
    mReaderSchema = readerSchema;
    return this;
  }

  /**
   * Configures this column to decode cells using Avro writer schemas.
   *
   * <p> This forces the use of generic records. </p>
   * <p> Invalid for counter cells. </p>
   *
   * @return this CellSpec.
   */
  public CellSpec setUseWriterSchema() {
    Preconditions.checkState(!isCounter());
    mReaderSchema = null;
    return setDecoderFactory(GenericCellDecoderFactory.get());
  }

  /**
   * Configures this column to decode cells using Avro specific records.
   *
   * <p> Invalid for counter cells. </p>
   *
   * @param klass Avro generated class of the specific record to decode this column to.
   * @return this CellSpec.
   * @throws InvalidLayoutException of the specified class is not a valid Avro specific record.
   */
  public CellSpec setSpecificRecord(Class<? extends SpecificRecord> klass)
      throws InvalidLayoutException {
    try {
      final java.lang.reflect.Field field = klass.getField("SCHEMA$");
      final Schema schema = (Schema) field.get(klass);
      if (!klass.getName().equals(schema.getFullName())) {
        throw new InvalidLayoutException(String.format(
            "Specific record class name '%s' does not match schema name '%s'.",
            klass.getName(), schema.getFullName()));
      }
      return setReaderSchema(schema)
          .setDecoderFactory(SpecificCellDecoderFactory.get());

    } catch (IllegalAccessException iae) {
      throw new InvalidLayoutException(String.format(
          "Invalid specific record class '%s': %s", klass.getName(), iae.getMessage()));
    } catch (NoSuchFieldException nsfe) {
      throw new InvalidLayoutException(String.format(
          "Invalid specific record class '%s': %s", klass.getName(), nsfe.getMessage()));
    }
  }

  /**
   * Sets the table to resolve Avro schemas.
   *
   * @param schemaTable Table to resolve Avro schemas.
   * @return this CellSpec.
   */
  public CellSpec setSchemaTable(KijiSchemaTable schemaTable) {
    mSchemaTable = schemaTable;
    return this;
  }

  /**
   * Returns the schema table to resolve schema hashes or IDs.
   *
   * @return the schema table to resolve schema hashes or IDs.
   */
  public KijiSchemaTable getSchemaTable() {
    return mSchemaTable;
  }

  /**
   * Sets the factory for cell decoders (either specific or generic).
   *
   * @param decoderFactory Factory for cell decoders (either specific or generic).
   * @return this CellSpec.
   */
  public CellSpec setDecoderFactory(KijiCellDecoderFactory decoderFactory) {
    mDecoderFactory = decoderFactory;
    return this;
  }

  /**
   * Returns the factory for cell decoders to use for this column.
   *
   * @return the factory for cell decoders to use for this column.
   */
  public KijiCellDecoderFactory getDecoderFactory() {
    return mDecoderFactory;
  }

  /**
   * Sets the factory for cell encoders.
   *
   * @param encoderFactory Factory for cell encoders.
   * @return this CellSpec.
   */
  public CellSpec setEncoderFactory(KijiCellEncoderFactory encoderFactory) {
    mEncoderFactory = encoderFactory;
    return this;
  }

  /**
   * Returns the factory for cell encoders to use for this column.
   *
   * @return the factory for cell encoders to use for this column.
   */
  public KijiCellEncoderFactory getEncoderFactory() {
    return mEncoderFactory;
  }

  /**
   * Returns whether this cell is a counter.
   *
   * @return whether this cell is a counter.
   */
  public boolean isCounter() {
    Preconditions.checkNotNull(mCellSchema);
    return mCellSchema.getType() == SchemaType.COUNTER;
  }

  /**
   * Returns whether this cell is encoded with Avro.
   *
   * @return whether this cell is encoded with Avro.
   */
  public boolean isAvro() {
    Preconditions.checkNotNull(mCellSchema);
    return (mCellSchema.getType() == SchemaType.INLINE)
        || (mCellSchema.getType() == SchemaType.CLASS);
  }

  /**
   * Returns the underlying CellSchema Avro record. May be null.
   *
   * @return the underlying CellSchema Avro record. May be null.
   */
  public CellSchema getCellSchema() {
    return mCellSchema;
  }

  /**
   * Returns the underlying CellSchema Avro record, creating it if necessary.
   *
   * @return the underlying CellSchema Avro record, creating it if necessary.
   */
  private CellSchema getOrCreateCellSchema() {
    if (null == mCellSchema) {
      mCellSchema = new CellSchema();
    }
    return mCellSchema;
  }


  /**
   * Returns the cell Avro schema. Valid for Avro cells only.
   *
   * @return the cell Avro schema. Valid for Avro cells only.
   */
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
   * @throws SchemaClassNotFoundException if the class for a specific Avro record is not found.
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
