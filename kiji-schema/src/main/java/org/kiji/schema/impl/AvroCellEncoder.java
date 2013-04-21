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
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiEncodingException;
import org.kiji.schema.layout.CellSpec;
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
    public void encode(Schema writerSchema) throws IOException {
      final BytesKey schemaHash = mCellSpec.getSchemaTable().getOrCreateSchemaHash(writerSchema);
      mByteArrayEncoder.writeFixed(schemaHash.getBytes());
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Schema encoders that uses the UID of the schema. */
  private class SchemaIdEncoder implements SchemaEncoder {
    /** {@inheritDoc} */
    @Override
    public void encode(Schema writerSchema) throws IOException {
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
    public void encode(Schema writerSchema) throws IOException {
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
  private SchemaEncoder createSchemaEncoder(CellSpec cellSpec) throws IOException {
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
  public AvroCellEncoder(CellSpec cellSpec) throws IOException {
    mCellSpec = Preconditions.checkNotNull(cellSpec);
    Preconditions.checkArgument(cellSpec.isAvro());
    mReaderSchema = mCellSpec.getAvroSchema();
    mSchemaEncoder = createSchemaEncoder(mCellSpec);
  }

  /** {@inheritDoc} */
  @Override
  public byte[] encode(DecodedCell<?> cell) throws IOException {
    return encode(cell.getData());
  }

  /**
   * Simplistic validation of Avro schema, until Avro provides a better one.
   *
   * <p>
   *   This is a terribly inaccurate method to validate reader/writer schemas.
   *   Until we have properly defined reader schemas and schema validation,
   *   this attempts to provide an overly restrictive safe default.
   *
   *   Concretely, this allows no evolution: writer and reader schemas must match exactly.
   * </p>
   *
   * @param readerSchema Reader schema configured for the column.
   * @param writerSchema Writer schema of the data being written to the column.
   * @throws KijiEncodingException if the validation fails.
   */
  private void validate(Schema readerSchema, Schema writerSchema) {
    if (readerSchema.getType() == writerSchema.getType()) {
      switch (readerSchema.getType()) {
      case RECORD: {
        // Overly restrictive for now, but safe, until we can provide safe evolution:
        if (!readerSchema.equals(writerSchema)) {
          throw new KijiEncodingException(String.format(
              "Incompatible reader/writer schema: reader=%s writer=%s",
              readerSchema, writerSchema));
        }
        break;
      }
      case ARRAY: {
        validate(readerSchema.getElementType(), writerSchema.getElementType());
        break;
      }
      case MAP: {
        validate(readerSchema.getValueType(), writerSchema.getValueType());
        break;
      }
      default: {
        // Overly restrictive for now, but safe, until we can provide safe evolution:
        if (!readerSchema.equals(writerSchema)) {
          throw new KijiEncodingException(String.format(
              "Incompatible reader/writer schema: reader=%s writer=%s",
              readerSchema, writerSchema));
        }
      }
      }
    } else {
      switch (readerSchema.getType()) {
      case RECORD:
        throw new KijiEncodingException(String.format(
          "Incompatible reader/writer schema: reader=%s writer=%s",
          readerSchema, writerSchema));
      default: {
        // Overly restrictive for now, but safe, until we can provide safe evolution:
        if (!readerSchema.equals(writerSchema)) {
          throw new KijiEncodingException(String.format(
              "Incompatible reader/writer schema: reader=%s writer=%s",
              readerSchema, writerSchema));
        }
      }
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public synchronized <T> byte[] encode(T cellValue) throws IOException {
    mByteArrayOutputStream.reset();
    final Schema writerSchema =
        (cellValue instanceof GenericContainer)
        ? ((GenericContainer) cellValue).getSchema()
        : mReaderSchema;
    // TODO(SCHEMA-326): reader schema should always be defined, independently of writer schemas:
    final Schema readerSchema = (mReaderSchema != null) ? mReaderSchema : writerSchema;
    // TODO(SCHEMA-326): Validate writer schema thoroughly against the actual reader schema.
    validate(readerSchema, writerSchema);

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
  private DatumWriter<Object> getDatumWriter(Schema schema) {
    final DatumWriter<Object> existing = mCachedDatumWriters.get(schema);
    if (null != existing) {
      return existing;
    }
    final DatumWriter<Object> newWriter = new SpecificDatumWriter<Object>(schema);
    mCachedDatumWriters.put(schema, newWriter);
    return newWriter;
  }
}
