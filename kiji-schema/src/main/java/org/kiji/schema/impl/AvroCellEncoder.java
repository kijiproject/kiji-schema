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
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.layout.impl.CellSpec;
import org.kiji.schema.util.ByteStreamArray;

/**
 * Serializes a KijiCell.
 *
 * A Kiji cell is encoded as an Extension record, although this class implements the encoder
 * in a specific way for efficiency.
 */
@ApiAudience.Private
public final class AvroCellEncoder implements KijiCellEncoder {

  /** Specification of the cells to encode. */
  private final CellSpec mCellSpec;

  /** Schema encoder. */
  private final SchemaEncoder mSchemaEncoder;

  // TODO: Should this be static?
  /** A cache of avro datum writers. */
  private final Map<Schema, DatumWriter<Object>> mCachedDatumWriters = Maps.newHashMap();

  /** A byte stream for when encoding to a byte array. */
  private final ByteArrayOutputStream mByteArrayOutputStream = new ByteArrayOutputStream();

  /** An encoder that writes to the above byte stream. */
  private final Encoder mByteArrayEncoder =
      EncoderFactory.get().directBinaryEncoder(mByteArrayOutputStream, null);

  /** A reusable encoder for when encoding to an arbitrary output stream. */
  // private BinaryEncoder mReusableEncoder = null;

  private final Schema mSchema;

  // -----------------------------------------------------------------------------------------------

  /**
   * Interface for schema encoders.
   *
   * An encoder is configured to encode one specific schema in one specific way, and then writes
   * the encoded schema into any given Avro encoder.
   */
  private interface SchemaEncoder {
    /**
     * Encodes the configured schema.
     *
     * @param encoder Encode the configured schema using this Avro encoder.
     * @throws IOException on I/O error.
     */
    void encode(Encoder encoder) throws IOException;
  }

  /** Schema encoders that uses a hash of the schema. */
  private static class SchemaHashEncoder implements SchemaEncoder {
    private final byte[] mSchemaHash;

    /**
     * Creates an encoder for a schema hash.
     *
     * @param schemaTable Schema table to resolve the schema hash.
     * @param schema Schema to encode.
     * @throws IOException on I/O error.
     */
    public SchemaHashEncoder(KijiSchemaTable schemaTable, Schema schema) throws IOException {
      mSchemaHash = schemaTable.getOrCreateSchemaHash(schema).getBytes();
    }

    /** {@inheritDoc} */
    @Override
    public void encode(Encoder encoder) throws IOException {
      encoder.writeFixed(mSchemaHash);
    }
  }

  /** Schema encoders that uses the UID of the schema. */
  private static class SchemaIdEncoder implements SchemaEncoder {
    private final long mSchemaId;

    /**
     * Creates an encoder for schema IDs.
     *
     * @param schemaTable Schema table to resolve schema IDs.
     * @param schema Schema to encode.
     * @throws IOException on I/O error.
     */
    public SchemaIdEncoder(KijiSchemaTable schemaTable, Schema schema) throws IOException {
      mSchemaId = schemaTable.getOrCreateSchemaId(schema);
    }

    /** {@inheritDoc} */
    @Override
    public void encode(Encoder encoder) throws IOException {
      encoder.writeFixed(ByteStreamArray.longToVarInt64(mSchemaId));
    }
  }

  /** Schema encoders for final columns (schema is not encoded as part of the cell). */
  private static class FinalSchemaEncoder implements SchemaEncoder {
    /** {@inheritDoc} */
    @Override
    public void encode(Encoder encoder) throws IOException {
      // Nothing to encode
    }
  }

  /** Singleton for the final schema encoder (schema is not encoded as part of the cell). */
  private static final FinalSchemaEncoder FINAL_SCHEMA_ENCODER = new FinalSchemaEncoder();

  /**
   * Creates a schema encoder for the specified cell encoding.
   *
   * @param cellSpec Specification of the cell to encode.
   * @return a schema encoder for the specified cell encoding.
   * @throws IOException on I/O error.
   */
  private static SchemaEncoder createSchemaEncoder(CellSpec cellSpec) throws IOException {
    switch (cellSpec.getCellSchema().getStorage()) {
    case HASH: return new SchemaHashEncoder(cellSpec.getSchemaTable(), cellSpec.getAvroSchema());
    case UID: return new SchemaIdEncoder(cellSpec.getSchemaTable(), cellSpec.getAvroSchema());
    case FINAL: return FINAL_SCHEMA_ENCODER;
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
    mSchemaEncoder = createSchemaEncoder(mCellSpec);
    mSchema = mCellSpec.getAvroSchema();
  }

  /** {@inheritDoc} */
  @Override
  public byte[] encode(KijiCell<?> cell) throws IOException {
    return encode(cell.getData());
  }

  /** {@inheritDoc} */
  @Override
  public <T> byte[] encode(T cellValue) throws IOException {
    mByteArrayOutputStream.reset();
    mSchemaEncoder.encode(mByteArrayEncoder);
    getDatumWriter(mSchema).write(cellValue, mByteArrayEncoder);
    return mByteArrayOutputStream.toByteArray();
  }

  /**
   * Gets a datum writer for a schema and caches it.
   *
   * @param schema The writer schema.
   * @return A datum writer for the given schema.
   */
  private DatumWriter<Object> getDatumWriter(Schema schema) {
    // TODO: synchronized?
    final DatumWriter<Object> existing = mCachedDatumWriters.get(schema);
    if (null != existing) {
      return existing;
    }
    final DatumWriter<Object> newWriter = new SpecificDatumWriter<Object>(schema);
    mCachedDatumWriters.put(schema, newWriter);
    return newWriter;
  }
}
