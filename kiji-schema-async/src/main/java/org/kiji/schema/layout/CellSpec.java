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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.GenericCellDecoderFactory;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiCellDecoderFactory;
import org.kiji.schema.KijiCellEncoderFactory;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiURI;
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
  private static final Logger LOG = LoggerFactory.getLogger(CellSpec.class);

  /** URI of the column this specification is for. Potentially null. */
  private KijiURI mColumnURI = null;

  /** Avro record specifying the cell encoding. */
  private CellSchema mCellSchema = null;

  /** Avro specific: enumerates the different reader schemas that may be used to decode a cell. */
  private static enum AvroReaderSchema {
    /** Use the default reader schema. */
    DEFAULT,

    /** Use the explicitly declared reader schema. */
    EXPLICIT,

    /** Use the writer schema. */
    WRITER;
  }

  /**
   * Avro specific: determines which Avro reader schema to apply when decoding a cell.
   * Null otherwise.
   */
  private AvroReaderSchema mAvroReaderSchema = null;

  /** Avro specific: decode the cell content with this Avro reader schema. Null otherwise. */
  private Schema mReaderSchema = null;

  /**
   * Avro specific: this is the default Avro reader schema, if any has been specified.
   * This field is lazily initialized as resolving the default reader schema requires
   * setting the schema table to use first.
   */
  private Schema mDefaultReaderSchema = null;

  /** Schema table to resolve schema hashes or UIDs. */
  private KijiSchemaTable mSchemaTable = null;

  /** Resolver for Avro schema descriptors. */
  private AvroSchemaResolver mAvroSchemaResolver = null;

  /** Factory for cell decoders (either specific or generic). */
  private KijiCellDecoderFactory mDecoderFactory = SpecificCellDecoderFactory.get();

  /** Factory for cell encoders. */
  private KijiCellEncoderFactory mEncoderFactory = DefaultKijiCellEncoderFactory.get();

  /**
   * Returns a new CellSpec for a counter cell.
   * @return a new CellSpec for a counter cell.
   */
  public static CellSpec newCounter() {
    return new CellSpec()
        .setType(SchemaType.COUNTER);
  }

  /**
   * Constructs a CellSpec instance from a CellSchema descriptor.
   *
   * @param cellSchema CellSchema record to initialize the CellSpec from.
   * @param schemaTable Schema table to resolve schema hashes or UIDs.
   * @return a new CellSpec initialized from a CellSchema descriptor.
   * @throws InvalidLayoutException if the cell specification is invalid.
   */
  public static CellSpec fromCellSchema(
      final CellSchema cellSchema,
      final KijiSchemaTable schemaTable)
      throws InvalidLayoutException {
    return fromCellSchema(cellSchema)
        .setSchemaTable(schemaTable);
  }

  /**
   * Constructs a CellSpec instance from a CellSchema descriptor.
   *
   * @param cellSchema CellSchema record to initialize the CellSpec from.
   * @return a new CellSpec initialized from the given CellSchema descriptor.
   * @throws InvalidLayoutException if the cell specification is invalid.
   */
  public static CellSpec fromCellSchema(final CellSchema cellSchema)
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
  public static CellSpec copy(final CellSpec spec) {
    final CellSpec copy = new CellSpec();
    copy.mColumnURI = spec.mColumnURI;
    copy.mCellSchema = CellSchema.newBuilder(spec.mCellSchema).build();  // deep copy
    copy.mAvroReaderSchema = spec.mAvroReaderSchema;
    copy.mReaderSchema = spec.mReaderSchema;
    copy.mSchemaTable = spec.mSchemaTable;
    copy.mAvroSchemaResolver = spec.mAvroSchemaResolver;
    copy.mDecoderFactory = spec.mDecoderFactory;
    copy.mEncoderFactory = spec.mEncoderFactory;
    return copy;
  }

  /** Initializes a new unspecified CellSpec. */
  private CellSpec() {
  }

  /**
   * Initializes this CellSpec instance from a given CellSchema specification.
   *
   * @param cellSchema CellSchema to initialize the CellSpec from.
   * @return this CellSpec.
   * @throws InvalidLayoutException on layout error.
   */
  public CellSpec setCellSchema(final CellSchema cellSchema) throws InvalidLayoutException {
    mCellSchema = CellSchema.newBuilder(cellSchema).build();  // deep copy
    if (isAvro()) {
      switch (mCellSchema.getType()) {
        case AVRO: {
          // By default, use the declared default reader schema, if any.
          mAvroReaderSchema = AvroReaderSchema.DEFAULT;
          break;
        }
        case INLINE: {
          mAvroReaderSchema = AvroReaderSchema.EXPLICIT;
          try {
            mReaderSchema = new Schema.Parser().parse(cellSchema.getValue());
          } catch (RuntimeException re) {
            throw new InvalidLayoutException(String.format(
                "Invalid Avro schema '%s' caused error: %s.",
                cellSchema.getValue(), re.getMessage()));
          }
          break;
        }
        case CLASS: {
          mAvroReaderSchema = AvroReaderSchema.EXPLICIT;
          try {
            mReaderSchema = getSchemaFromClass(cellSchema.getValue());
          } catch (SchemaClassNotFoundException scnfe) {
            // In KijiSchema 1.0, if the specific class isn't found on the classpath,
            // fall back on using the writer schema:
            setUseWriterSchema();
          }
          break;
        }
        default: throw new InternalKijiError("Unknown Avro cell type: " + mCellSchema);
      }
    }
    return this;
  }

  /**
   * Sets the Avro schema storage.
   *
   * <p> For Avro-encoded cells only. </p>
   *
   * @param schemaStorage Avro schema storage to use (hash, UID or final).
   * @return this CellSpec.
   */
  public CellSpec setSchemaStorage(final SchemaStorage schemaStorage) {
    getOrCreateCellSchema().setStorage(schemaStorage);
    Preconditions.checkState(isAvro());
    return this;
  }

  /**
   * Sets the type of the cells to encode or decode.
   *
   * @param schemaType Cell type (Avro (inline, class or validated), counter or raw bytes).
   * @return this CellSpec.
   */
  public CellSpec setType(final SchemaType schemaType) {
    getOrCreateCellSchema().setType(schemaType);
    return this;
  }

  /**
   * Sets the Avro schema to use when decoding cells.
   *
   * <p> For Avro-encoded cells only. </p>
   *
   * @param readerSchema Avro reader schema.
   * @return this CellSpec.
   */
  public CellSpec setReaderSchema(final Schema readerSchema) {
    Preconditions.checkState(isAvro());
    mAvroReaderSchema = AvroReaderSchema.EXPLICIT;
    mReaderSchema = Preconditions.checkNotNull(readerSchema);
    return this;
  }

  /**
   * Configures this column to decode cells using Avro writer schemas.
   *
   * <p> This forces the use of generic records. </p>
   * <p> For Avro-encoded cells only. </p>
   *
   * @return this CellSpec.
   */
  public CellSpec setUseWriterSchema() {
    Preconditions.checkState(isAvro());
    mReaderSchema = null;
    mAvroReaderSchema = AvroReaderSchema.WRITER;
    setDecoderFactory(GenericCellDecoderFactory.get());
    return this;
  }

  /**
   * Configures this column to decode cells using the default Avro reader schemas.
   *
   * <p> For Avro-encoded cells only. </p>
   *
   * @return this CellSpec.
   */
  public CellSpec setUseDefaultReaderSchema() {
    Preconditions.checkState(isAvro());
    mReaderSchema = null;
    mAvroReaderSchema = AvroReaderSchema.DEFAULT;
    return this;
  }

  /**
   * Configures this column to decode cells using Avro specific records.
   *
   * <p> For Avro-encoded cells only. </p>
   *
   * @param klass Avro generated class of the specific record to decode this column to.
   * @return this CellSpec.
   * @throws InvalidLayoutException if the specified class is not a valid Avro specific record.
   */
  public CellSpec setSpecificRecord(final Class<? extends SpecificRecord> klass)
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
   * Sets the Schema table used to resolve Avro schemas.
   *
   * <p> Also sets the Avro schema resolver. </p>
   *
   * @param schemaTable Schema table to resolve Avro schemas.
   * @return this CellSpec.
   */
  public CellSpec setSchemaTable(final KijiSchemaTable schemaTable) {
    mSchemaTable = schemaTable;
    mAvroSchemaResolver = new SchemaTableAvroResolver(mSchemaTable);
    return this;
  }

  /**
   * Returns the resolver function that resolves AvroSchema into Schema objects.
   * @return the resolver function that resolves AvroSchema into Schema objects.
   */
  public AvroSchemaResolver getAvroSchemaResolver() {
    return mAvroSchemaResolver;
  }

  /**
   * Returns the schema table to resolve schema hashes or IDs.
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
  public CellSpec setDecoderFactory(final KijiCellDecoderFactory decoderFactory) {
    mDecoderFactory = decoderFactory;
    return this;
  }

  /**
   * Returns the factory for cell decoders to use for this column.
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
  public CellSpec setEncoderFactory(final KijiCellEncoderFactory encoderFactory) {
    mEncoderFactory = encoderFactory;
    return this;
  }

  /**
   * Returns the factory for cell encoders to use for this column.
   * @return the factory for cell encoders to use for this column.
   */
  public KijiCellEncoderFactory getEncoderFactory() {
    return mEncoderFactory;
  }

  /**
   * Returns whether this cell is a counter.
   * @return whether this cell is a counter.
   */
  public boolean isCounter() {
    Preconditions.checkNotNull(mCellSchema);
    return mCellSchema.getType() == SchemaType.COUNTER;
  }

  /**
   * Returns whether this cell is encoded with Avro.
   * @return whether this cell is encoded with Avro.
   */
  public boolean isAvro() {
    Preconditions.checkNotNull(mCellSchema);
    return (mCellSchema.getType() == SchemaType.INLINE)
        || (mCellSchema.getType() == SchemaType.CLASS)
        || (mCellSchema.getType() == SchemaType.AVRO);
  }

  /**
   * Returns the underlying CellSchema Avro record. May be null.
   * @return the underlying CellSchema Avro record. May be null.
   */
  public CellSchema getCellSchema() {
    return mCellSchema;
  }

  /**
   * Returns the underlying CellSchema Avro record, creating it if necessary.
   * @return the underlying CellSchema Avro record, creating it if necessary.
   */
  private CellSchema getOrCreateCellSchema() {
    if (null == mCellSchema) {
      mCellSchema = new CellSchema();
    }
    return mCellSchema;
  }

  /**
   * Returns the Avro reader schema used to decode cells.
   *
   * <p> For Avro-encoded cells only. </p>
   *
   * @return the Avro reader schema used to decode cells.
   *     Null means cells are decoded using their writer schema.
   */
  public Schema getAvroSchema() {
    Preconditions.checkState(isAvro());
    switch (mAvroReaderSchema) {
      case DEFAULT: return getDefaultReaderSchema();
      case EXPLICIT: return mReaderSchema;
      case WRITER: return null;
      default: throw new InternalKijiError("Invalid Avro reader schema: " + mAvroReaderSchema);
    }
  }

  /**
   * Returns the default Avro schema to use to decode cells from this column, if any.
   *
   * <p> For Avro-encoded cells only. </p>
   *
   * @return the default Avro schema to use to decode cells from this column, if any.
   */
  public Schema getDefaultReaderSchema() {
    Preconditions.checkState(isAvro());
    if ((null == mDefaultReaderSchema) && (mCellSchema.getDefaultReader() != null)) {
      Preconditions.checkNotNull(mAvroSchemaResolver);
      mDefaultReaderSchema = mAvroSchemaResolver.apply(mCellSchema.getDefaultReader());
    }
    return mDefaultReaderSchema;
  }

  /**
   * Reads the Avro schema from the table layout.
   *
   * @param cellSchema The portion of the table layout record to read from.
   * @return the Avro schema if relevant, or null.
   * @throws InvalidLayoutException if the specification or the schema is invalid.
   * @throws SchemaClassNotFoundException if the class for a specific Avro record is not found.
   */
  public static Schema readAvroSchema(final CellSchema cellSchema) throws InvalidLayoutException {
    switch (cellSchema.getType()) {
    case INLINE: {
      try {
        return new Schema.Parser().parse(cellSchema.getValue());
      } catch (RuntimeException re) {
        throw new InvalidLayoutException(String.format(
            "Invalid Avro schema: '%s' caused error: %s.",
            cellSchema.getValue(), re.getMessage()));
      }
    }
    case CLASS: {
      final String className = cellSchema.getValue();
      return getSchemaFromClass(className);
    }
    case AVRO: {
      // Avro encoded cells have various associated schemas: readers, writers, default reader, etc.
      return null;
    }
    case PROTOBUF: {
      // Protocol-buffer have no Avro schema.
      return null;
    }
    case RAW_BYTES: {
      // No Avro schema for raw-byte cells:
      return null;
    }
    case COUNTER: {
      // Counters are coded using a Bytes.toBytes(long) and Bytes.toLong(byte[]):
      return null;
    }
    default:
      throw new InvalidLayoutException("Invalid schema type: " + cellSchema.getType());
    }
  }

  /**
   * Reports the Avro schema from a generated class.
   *
   * @param className Fully-qualified name of the generated class to report the schema of.
   * @return Avro schema of the specified generated class.
   * @throws InvalidLayoutException if the specified class does not correspond to a valid Avro type.
   *     In particular, throws {@link SchemaClassNotFoundException} if the class cannot be found.
   */
  public static Schema getSchemaFromClass(String className)
      throws InvalidLayoutException {

    if (!JavaIdentifiers.isValidClassName(className)) {
      throw new InvalidLayoutException(String.format(
          "Invalid cell specification with Avro class type has invalid class name: '%s'.",
          className));
    }

    try {
      final Class<? extends SpecificRecord> avroClass =
          Class.forName(className).asSubclass(SpecificRecord.class);
      return getSchemaFromClass(avroClass);
    } catch (ClassNotFoundException cnfe) {
      throw new SchemaClassNotFoundException(
          "Java class " + className + " was not found on the classpath.");
    }
  }

  /**
   * Reports the Avro schema from a generated class.
   *
   * @param klass Generated class to report the schema of.
   * @return Avro schema of the specified generated class.
   * @throws InvalidLayoutException if the specified class does not correspond to a valid Avro type.
   */
  public static Schema getSchemaFromClass(Class<? extends SpecificRecord> klass)
      throws InvalidLayoutException {
    try {
      return SpecificData.get().getSchema(klass);
    } catch (RuntimeException re) {
      LOG.debug("Error accessing schema from Avro class: {}",
          StringUtils.stringifyException(re));
      throw new InvalidLayoutException("Java class is not a valid Avro type: " + klass.getName());
    }
  }

  /**
   * Sets the URI of the column this specification is for.
   *
   * @param uri URI of the column this specification is for. May be null.
   * @return this CellSpec.
   */
  public CellSpec setColumnURI(KijiURI uri) {
    Preconditions.checkArgument((uri == null) || (uri.getColumns().size() == 1),
        "Invalid CellSpec URI '%s' : CellSpec requires a URI for a single column.", uri);
    mColumnURI = uri;
    return this;
  }

  /**
   * Returns the URI of the column this specification is for. Potentially null.
   * @return the URI of the column this specification is for. Potentially null.
   */
  public KijiURI getColumnURI() {
    return mColumnURI;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(CellSpec.class)
        .add("column_uri", mColumnURI)
        .add("cell_schema", mCellSchema)
        .add("reader_schema", mReaderSchema)
        .add("generic", mDecoderFactory instanceof GenericCellDecoderFactory)
        .toString();
  }

}
