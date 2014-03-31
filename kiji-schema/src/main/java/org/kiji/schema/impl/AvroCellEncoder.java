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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiEncodingException;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.AvroSchema;
import org.kiji.schema.avro.AvroValidationPolicy;
import org.kiji.schema.avro.SchemaStorage;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.impl.hbase.HBaseKiji;
import org.kiji.schema.impl.hbase.HBaseTableLayoutUpdater;
import org.kiji.schema.layout.AvroSchemaResolver;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.SchemaTableAvroResolver;
import org.kiji.schema.layout.TableLayoutBuilder;
import org.kiji.schema.util.ByteStreamArray;
import org.kiji.schema.util.BytesKey;

/**
 * Serializes Avro cells to bytes for persistence in HBase.
 *
 * <p>
 *   An Avro cell encoder is specific to one column in a KijiTable.
 *   Depending on the column specification, Avro cells embed the writer schema or not.
 *   When embedded, the Avro schema ID/hash is prepended to the encoded value.
 * <p>
 */
@ApiAudience.Private
public final class AvroCellEncoder implements KijiCellEncoder {
  /** Name of the system property to control schema validation. */
  public static final String SCHEMA_VALIDATION_POLICY =
      "org.kiji.schema.impl.AvroCellEncoder.SCHEMA_VALIDATION_POLICY";

  private static final Logger LOG = LoggerFactory.getLogger(AvroCellEncoder.class);

  /** Mapping from class names of Avro primitives to their corresponding Avro schemas. */
  public static final Map<String, Schema> PRIMITIVE_SCHEMAS;
  static {
    final Schema booleanSchema = Schema.create(Schema.Type.BOOLEAN);
    final Schema intSchema = Schema.create(Schema.Type.INT);
    final Schema longSchema = Schema.create(Schema.Type.LONG);
    final Schema floatSchema = Schema.create(Schema.Type.FLOAT);
    final Schema doubleSchema = Schema.create(Schema.Type.DOUBLE);
    final Schema stringSchema = Schema.create(Schema.Type.STRING);

    // Initialize primitive schema mapping.
    PRIMITIVE_SCHEMAS = ImmutableMap
        .<String, Schema>builder()
        .put(boolean.class.getCanonicalName(), booleanSchema)
        .put(Boolean.class.getCanonicalName(), booleanSchema)
        .put(int.class.getCanonicalName(), intSchema)
        .put(Integer.class.getCanonicalName(), intSchema)
        .put(long.class.getCanonicalName(), longSchema)
        .put(Long.class.getCanonicalName(), longSchema)
        .put(float.class.getCanonicalName(), floatSchema)
        .put(Float.class.getCanonicalName(), floatSchema)
        .put(double.class.getCanonicalName(), doubleSchema)
        .put(Double.class.getCanonicalName(), doubleSchema)
        .put(String.class.getCanonicalName(), stringSchema)
        .put(org.apache.avro.util.Utf8.class.getCanonicalName(), stringSchema)
        .put(java.nio.ByteBuffer.class.getCanonicalName(), Schema.create(Schema.Type.BYTES))
        .build();
  }

  private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);

  /**
   * Reports the Avro schema validation policy.
   *
   * @param cellSpec to get the Avro schema validation policy from.
   * @return the schema validation policy.
   */
  private static AvroValidationPolicy getAvroValidationPolicy(final CellSpec cellSpec) {
    final String validationPolicy = System.getProperty(SCHEMA_VALIDATION_POLICY);
    if (validationPolicy != null) {
      try {
        return AvroValidationPolicy.valueOf(validationPolicy);
      } catch (IllegalArgumentException iae) {
        throw new KijiEncodingException(
            String.format("Unrecognized validation policy: %s", validationPolicy), iae);
      }
    } else {
      return cellSpec.getCellSchema().getAvroValidationPolicy();
    }
  }

  /** Specification of the column to encode. */
  private final CellSpec mCellSpec;

  /** Schema encoder. */
  private final SchemaEncoder mSchemaEncoder;

  /**
   * Cache of Avro DatumWriter.
   *
   * <p>
   *   Avro datum writers aren't thread-safe, but if we ensure the schema of a datum writer is not
   *   modified, the datum writer becomes thread-safe.
   * </p>
   *
   * <p>
   *   This cache is not globally shared at present.
   *   To share this map globally (ie. static) requires using a WeakIdentityHashMap:
   *   a weak map is required to garbage collect unused schemas;
   *   an identity map is also required as Schema.hashCode/equals are imperfect.
   * </p>
   */
  private final Map<Schema, DatumWriter<Object>> mCachedDatumWriters = Maps.newHashMap();

  /**
   * A byte stream for when encoding to a byte array.
   *
   * Since we use the same instance for all encodings, this makes the encoder thread-unsafe.
   */
  private final ByteArrayOutputStream mByteArrayOutputStream = new ByteArrayOutputStream();

  /** An encoder that writes to the above byte stream. */
  private final Encoder mByteArrayEncoder =
      EncoderFactory.get().directBinaryEncoder(mByteArrayOutputStream, null);

  /**
   * Configured reader schema for the column to encode.
   *
   * This may currently be null if we only know the fully-qualified name of the record.
   * Eventually, this will always be populated so we can validate records being written against
   * the present reader schema.
   */
  private final Schema mReaderSchema;

  /**
   * Writer schemas registered for the column that this cell encoder will encode cells for.
   *
   * Note: This will be null if schema validation is disabled.
   */
  private final Set<Schema> mRegisteredWriters;

  /**
   * Avro validation policy to use when encoding cells. This may be overridden using the
   * "org.kiji.schema.impl.AvroCellEncoder.SCHEMA_VALIDATION_POLICY" system property.
   */
  private final AvroValidationPolicy mValidationPolicy;

  // -----------------------------------------------------------------------------------------------

  /**
   * Encodes the writer schema.
   */
  private interface SchemaEncoder {
    /**
     * Encodes the writer schema in the cell.
     *
     * @param writerSchema Avro schema of the data being encoded.
     * @throws IOException on I/O error.
     */
    void encode(Schema writerSchema) throws IOException;
  }

  // -----------------------------------------------------------------------------------------------

  /** Schema encoders that uses a hash of the schema. */
  private class SchemaHashEncoder implements SchemaEncoder {
    /** {@inheritDoc} */
    @Override
    public void encode(final Schema writerSchema) throws IOException {
      final BytesKey schemaHash = mCellSpec.getSchemaTable().getOrCreateSchemaHash(writerSchema);
      mByteArrayEncoder.writeFixed(schemaHash.getBytes());
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Schema encoders that uses the UID of the schema. */
  private class SchemaIdEncoder implements SchemaEncoder {
    /** {@inheritDoc} */
    @Override
    public void encode(final Schema writerSchema) throws IOException {
      final long schemaId = mCellSpec.getSchemaTable().getOrCreateSchemaId(writerSchema);
      mByteArrayEncoder.writeFixed(ByteStreamArray.longToVarInt64(schemaId));
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Schema encoders for final columns.
   *
   * <p>
   *   Schema is not encoded as part of the HBase cell.
   *   However, the Avro schema of the cell value must exactly match the column reader schema.
   *   In other words, the writer schema must be the reader schema at all times.
   * </p>
   */
  private static class FinalSchemaEncoder implements SchemaEncoder {
    /** Creates an encoder for a schema of a final column. */
    public FinalSchemaEncoder() {
    }

    /** {@inheritDoc} */
    @Override
    public void encode(final Schema writerSchema) throws IOException {
      // Nothing to encode, because the writer schema is already encoded in the column layout.
      // This means the writer schema must be exactly the declared reader schema.
    }
  }

  /**
   * Creates a schema encoder for the specified cell encoding.
   *
   * @param cellSpec Specification of the cell to encode.
   * @return a schema encoder for the specified cell encoding.
   * @throws IOException on I/O error.
   */
  private SchemaEncoder createSchemaEncoder(final CellSpec cellSpec) throws IOException {
    switch (cellSpec.getCellSchema().getStorage()) {
    case HASH: return new SchemaHashEncoder();
    case UID: return new SchemaIdEncoder();
    case FINAL: return new FinalSchemaEncoder();
    default:
      throw new RuntimeException(
          "Unexpected cell format: " + cellSpec.getCellSchema().getStorage());
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Creates a new <code>KijiCellEncoder</code> instance.
   *
   * @param cellSpec Specification of the cell to encode.
   * @throws IOException on I/O error.
   */
  public AvroCellEncoder(final CellSpec cellSpec) throws IOException {
    mCellSpec = Preconditions.checkNotNull(cellSpec);
    Preconditions.checkArgument(cellSpec.isAvro());
    mReaderSchema = mCellSpec.getAvroSchema();
    mSchemaEncoder = createSchemaEncoder(mCellSpec);
    mRegisteredWriters = flattenAvroUnions(getRegisteredWriters(mCellSpec));
    mValidationPolicy = getAvroValidationPolicy(mCellSpec);
  }

  /** {@inheritDoc} */
  @Override
  public byte[] encode(final DecodedCell<?> cell) throws IOException {
    return encode(cell.getData());
  }

  /** {@inheritDoc} */
  @Override
  public synchronized <T> byte[] encode(final T cellValue) throws IOException {
    mByteArrayOutputStream.reset();

    // Get the writer schema for this cell.
    final Schema writerSchema = getWriterSchema(cellValue);

    // Perform avro schema validation (if necessary).
    switch (mValidationPolicy) {
      case STRICT: {
        if (!mRegisteredWriters.contains(writerSchema)) {
          throw new KijiEncodingException(
              String.format("Error trying to use unregistered writer schema: %s",
                  writerSchema.toString(true)));
        }
        break;
      }
      case DEVELOPER: {
        if (!mRegisteredWriters.contains(writerSchema)) {
          LOG.info("Writer schema {} is currently not registered for column {}, registering now.",
              writerSchema, mCellSpec.getColumnURI());
          if (mCellSpec.getColumnURI() == null) {
            throw new InternalKijiError("CellSpec has no column URI: " + mCellSpec);
          }

          registerWriterSchema(mCellSpec.getColumnURI(), writerSchema);
        }
        break;
      }
      case NONE:
        // No-op. No validation required.
        break;
      case SCHEMA_1_0:
        // No-op. Validation happens for primitive types only during Avro serialization by setting
        // the writer schema (see getWriterSchema()).
        break;
      default: {
        throw new KijiEncodingException(
            String.format("Unrecognized schema validation policy: %s",
                mValidationPolicy.toString()));
      }
    }

    // Perform final column schema validation (if necessary).
    if (mCellSpec.getCellSchema().getStorage() == SchemaStorage.FINAL
        && !writerSchema.equals(mReaderSchema)) {
      throw new KijiEncodingException(
          String.format("Writer schema: %s does not match final column schema: %s",
              writerSchema.toString(true),
              mReaderSchema.toString(true)));
    }

    // Encode the Avro schema (if necessary):
    mSchemaEncoder.encode(writerSchema);

    // Encode the cell value:
    try {
      getDatumWriter(writerSchema).write(cellValue, mByteArrayEncoder);
    } catch (ClassCastException cce) {
      throw new KijiEncodingException(cce);
    } catch (AvroRuntimeException ure) {
      throw new KijiEncodingException(ure);
    }
    return mByteArrayOutputStream.toByteArray();
  }

  /**
   * Gets a datum writer for a schema and caches it.
   *
   * <p> Not thread-safe, calls to this method must be externally synchronized. </p>
   *
   * @param schema The writer schema.
   * @return A datum writer for the given schema.
   */
  private DatumWriter<Object> getDatumWriter(final Schema schema) {
    final DatumWriter<Object> existing = mCachedDatumWriters.get(schema);
    if (null != existing) {
      return existing;
    }
    final DatumWriter<Object> newWriter = new SpecificDatumWriter<Object>(schema);
    mCachedDatumWriters.put(schema, newWriter);
    return newWriter;
  }

  /**
   * Gets the writer schema of a specified value.
   *
   * @param <T> is the java type of the specified value.
   * @param cellValue to get the Avro schema of.
   * @return an Avro schema representing the type of data specified.
   * @throws KijiEncodingException if no Avro writer schema can be inferred from cellValue.
   */
  private <T> Schema getWriterSchema(final T cellValue) {
    if (cellValue == null) {
      return NULL_SCHEMA;
    } else if (cellValue instanceof GenericContainer) {
      return ((GenericContainer) cellValue).getSchema();
    } else if (mValidationPolicy == AvroValidationPolicy.SCHEMA_1_0) {
      // Compute the writer schema using old semantics. This will only validate primitive schemas.
      return mReaderSchema;
    } else {
      final String className = cellValue.getClass().getCanonicalName();
      final Schema schema = PRIMITIVE_SCHEMAS.get(className);
      if (schema == null) {
        throw new KijiEncodingException(String.format(
            "Unable to infer Avro writer schema for value: '%s'", cellValue));
      }
      return schema;
    }
  }

  /**
   * Gets the registered writer schemas associated with the provided cell specification.
   *
   * @param spec containing registered schemas.
   * @return the set of writer schemas registered for the provided cell.
   *     Null if validation is disabled.
   * @throws IOException if there is an error looking up schemas.
   */
  private static Set<Schema> getRegisteredWriters(final CellSpec spec) throws IOException {
    final List<AvroSchema> writerSchemas = spec.getCellSchema().getWriters();
    if (writerSchemas == null) {
      return null;
    }
    final AvroSchemaResolver resolver = new SchemaTableAvroResolver(spec.getSchemaTable());
    return Sets.newHashSet(Collections2.transform(writerSchemas, resolver));
  }

  /**
   * Flatten and expand the unions from a set of schemas.
   *
   * <p>
   *   For example, the set
   *     <code>{union(null, int), union(null, string)</code>
   *   will be expanded to
   *     <code>{null, int, string, union(null, int), union(null, string)}</code>.
   * </p>
   *
   * @param schemas Set of Avro schemas whose unions are to be expanded.
   * @return the expanded (flattened) set of Avro schemas.
   *     Null iff schemas is null.
   */
  private static Set<Schema> flattenAvroUnions(final Set<Schema> schemas) {
    if (schemas == null) {
      return null;
    }
    final Set<Schema> expanded = Sets.newHashSet();
    for (Schema schema : schemas) {
      expanded.add(schema);
      if (schema.getType() == Schema.Type.UNION) {
        for (Schema branch : schema.getTypes()) {
          Preconditions.checkArgument(branch.getType() != Schema.Type.UNION, branch);
          expanded.add(branch);
        }
      }
    }
    return expanded;
  }

  /**
   * Computes the layout ID directly following a given layout ID.
   *
   * <p>
   *   Increments the layout ID if it is an integer.
   *   Otherwise, forge a layout ID containing a timestamp.
   * </p>
   *
   * @param layoutId Layout ID to compute the next sequential ID from.
   * @return the next sequential layout ID.
   */
  private static String nextLayoutId(String layoutId) {
    try {
      final long lid = Long.parseLong(layoutId);
      return Long.toString(lid + 1);
    } catch (NumberFormatException nfe) {
      return String.format("layout-developer-%d", System.currentTimeMillis());
    }
  }

  /**
   * Registers a new writer schema in a given column.
   *
   * @param columnURI Full URI of the column for which to register a new writer schema.
   * @param writerSchema Writer schema to register.
   * @throws IOException on I/O error.
   */
  private static void registerWriterSchema(final KijiURI columnURI, final Schema writerSchema)
      throws IOException {
    Preconditions.checkArgument(columnURI.getColumns().size() == 1,
        "Expecting exactly one column in URI, got: %s", columnURI);
    final KijiColumnName column = columnURI.getColumns().get(0);

    // TODO(???) the layout updater interface is currently HBase specific.
    //     We should make a backend agnostic API for layout updates.
    final HBaseKiji kiji = (HBaseKiji) Kiji.Factory.open(columnURI);
    try {
      final Function<KijiTableLayout, TableLayoutDesc> update =
          new Function<KijiTableLayout, TableLayoutDesc>() {
            /** {@inheritDoc} */
            @Override
            public TableLayoutDesc apply(final KijiTableLayout refLayout) {
              Preconditions.checkNotNull(refLayout);
              try {
                final TableLayoutDesc refDesc = refLayout.getDesc();
                return new TableLayoutBuilder(refDesc, kiji)
                    .withLayoutId(nextLayoutId(refDesc.getLayoutId()))
                    .withWriter(column, writerSchema)
                    .withWritten(column, writerSchema)
                    .build();
              } catch (InvalidLayoutException ile) {
                LOG.error("Internal error while updating table layout in DEVELOPER mode: {}", ile);
                throw new InternalKijiError(ile);
              } catch (IOException ioe) {
                LOG.error("I/O error while updating table layout in DEVELOPER mode: {}", ioe);
                throw new KijiIOException(ioe);
              }
            }
          };

      try {
        final HBaseTableLayoutUpdater updater =
            new HBaseTableLayoutUpdater(kiji, columnURI, update);
        try {
          updater.update();
        } finally {
          updater.close();
        }
      } catch (KeeperException ke) {
        throw new IOException(ke);
      }
    } finally {
      kiji.release();
    }
  }
}
