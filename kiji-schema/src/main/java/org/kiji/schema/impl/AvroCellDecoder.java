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
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.layout.impl.CellSpec;
import org.kiji.schema.util.ByteStreamArray;
import org.kiji.schema.util.BytesKey;
import org.kiji.schema.util.Hasher;

/**
 * Base class for decoders that read Kiji cells encoded using Avro.
 *
 * Kiji cells are encoded as Extension records, although for efficiency reasons, this class
 * provides a custom decoder that avoid copying bytes unnecessarily.
 *
 * @param <T> The type of the decoded cell data.
 */
@ApiAudience.Private
public abstract class AvroCellDecoder<T> implements KijiCellDecoder<T> {

  /** Specification of the cell encoding. */
  private final CellSpec mCellSpec;

  /** Schema decoder. */
  private final SchemaDecoder mSchemaDecoder;

  /** Reader Avro schema. */
  private final Schema mReaderSchema;

  // -----------------------------------------------------------------------------------------------

  /** Interface for schema decoders. */
  private interface SchemaDecoder {
    /**
     * Decodes a schema from a given byte stream encoded using a SchemaEncoder.
     *
     * @param bsa Byte stream to decode from.
     * @return the decoded schema.
     * @throws IOException on I/O error.
     */
    Schema decode(ByteStreamArray bsa) throws IOException;
  }

  /** Schema decoder for schema hashes. */
  private static class SchemaHashDecoder implements SchemaDecoder {
    private final KijiSchemaTable mSchemaTable;

    /**
     * Creates a decoder for schemas encoded as hashes.
     *
     * @param schemaTable SchemaTable to resolve schema hashes.
     */
    public SchemaHashDecoder(KijiSchemaTable schemaTable) {
      mSchemaTable = Preconditions.checkNotNull(schemaTable);
    }

    /** {@inheritDoc} */
    @Override
    public Schema decode(ByteStreamArray bstream) throws IOException {
      final BytesKey schemaHash = new BytesKey(bstream.readBytes(Hasher.HASH_SIZE_BYTES));
      final Schema schema = mSchemaTable.getSchema(schemaHash);
      if (null == schema) {
        throw new IOException(
            String.format("Schema with hash %s not found in schema table.", schemaHash));
      }
      return schema;
    }
  }

  /** Schema decoder for schema UIDs. */
  private static class SchemaIdDecoder implements SchemaDecoder {
    private final KijiSchemaTable mSchemaTable;

    /**
     * Creates a decoder for schemas encoded as UIDs.
     *
     * @param schemaTable SchemaTable to resolve UIDs.
     */
    public SchemaIdDecoder(KijiSchemaTable schemaTable) {
      mSchemaTable = Preconditions.checkNotNull(schemaTable);
    }

    /** {@inheritDoc} */
    @Override
    public Schema decode(ByteStreamArray bstream) throws IOException {
      final long schemaId = bstream.readVarInt64();
      final Schema schema = mSchemaTable.getSchema(schemaId);
      if (null == schema) {
        throw new IOException(
            String.format("Schema with ID %d not found in schema table.", schemaId));
      }
      return schema;
    }
  }

  /** Schema decoder for cells from final columns (schema is not encoded as part of the cell). */
  private static class FinalSchemaDecoder implements SchemaDecoder {
    private final Schema mSchema;

    /**
     * Creates a schema decoder for final columns.
     *
     * @param schema Schema to decode.
     */
    public FinalSchemaDecoder(Schema schema) {
      mSchema = Preconditions.checkNotNull(schema);
    }

    /** {@inheritDoc} */
    @Override
    public Schema decode(ByteStreamArray bsa) throws IOException {
      return mSchema;
    }
  }

  /**
   * Creates a schema decoder.
   *
   * @param cellSpec Specification of the cell encoding.
   * @return a new schema decoder.
   * @throws IOException on I/O error.
   */
  private static SchemaDecoder createSchemaDecoder(CellSpec cellSpec) throws IOException {
    switch (cellSpec.getCellSchema().getStorage()) {
    case HASH: return new SchemaHashDecoder(cellSpec.getSchemaTable());
    case UID: return new SchemaIdDecoder(cellSpec.getSchemaTable());
    case FINAL: return new FinalSchemaDecoder(cellSpec.getAvroSchema());
    default:
      throw new RuntimeException(
          "Unexpected cell schema: " + cellSpec.getCellSchema().getStorage());
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Initializes an abstract KijiAvroCellDecoder.
   *
   * @param cellSpec Specification of the cell encoding.
   * @throws IOException on I/O error.
   */
  protected AvroCellDecoder(CellSpec cellSpec) throws IOException {
    mCellSpec = Preconditions.checkNotNull(cellSpec);
    Preconditions.checkArgument(mCellSpec.isAvro());
    mSchemaDecoder = createSchemaDecoder(cellSpec);
    mReaderSchema = Preconditions.checkNotNull(mCellSpec.getAvroSchema());
  }

  /**
   * Factory for DatumReader instances.
   *
   * Sub-classes must create DatumReader implementations for specific or generic records.
   *
   * @param writer Writer schema.
   * @param reader Reader schema.
   * @return a new DatumReader instance for the specified writer/reader schema combination.
   */
  protected abstract DatumReader<T> createDatumReader(Schema writer, Schema reader);

  /** {@inheritDoc} */
  @Override
  public KijiCell<T> decodeCell(byte[] encodedBytes) throws IOException {
    return decode(encodedBytes, null);
  }

  /** {@inheritDoc} */
  @Override
  public T decodeValue(byte[] bytes) throws IOException {
    return decodeCell(bytes).getData();
  }

  /**
   * Decodes the serialized bytes into a KijiCell. If reuse is non-null, the implementation may fill
   * it and return it as the KijiCell data payload.
   *
   * @param bytes The bytes from an HBase table cell.
   * @param reuse If non-null, may be filled with the decoded data and used as the data payload in
   *          the return value.
   * @return The decoded KijiCell.
   * @throws IOException If there is an error.
   */
  private KijiCell<T> decode(byte[] bytes, T reuse) throws IOException {
    final ByteStreamArray byteStream = new ByteStreamArray(bytes);
    final Schema writerSchema = mSchemaDecoder.decode(byteStream);
    final ByteBuffer binaryData =
        ByteBuffer.wrap(bytes, byteStream.getOffset(), bytes.length - byteStream.getOffset());
    final T data = decodeAvro(binaryData, writerSchema, mReaderSchema, reuse);
    return new KijiCell<T>(writerSchema, data);
  }

  /**
   * Gets the portion of the encoded byte array from an HBase table cell that has the avro-encoded
   * data payload.
   *
   * @param bytes The bytes from an HBase table cell.
   * @return the portion of the encoded byte array that contains the binary-encoded avro message.
   * @throws IOException on I/O error (eg. decoding error).
   */
  public ByteBuffer getPayload(byte[] bytes) throws IOException {
    final ByteStreamArray byteStream = new ByteStreamArray(bytes);
    // Decode the writer schema and throw it away:
    mSchemaDecoder.decode(byteStream);
    return ByteBuffer.wrap(bytes, byteStream.getOffset(), bytes.length - byteStream.getOffset());
  }

  /**
   * Decodes the data payload given the reader and writer schema. If reuse is non-null, the
   * implementation may fill it and return that object.
   *
   * @param encodedData The avro-encoded bytes of the data payload.
   * @param writerSchema The schema that was used to encode the data.
   * @param readerSchema The schema that is expected by the reader.
   * @param reuse An optional object to be filled and returned to save on object construction
   *     (may be null).
   * @return The decoded avro object.
   * @throws IOException If there is an error.
   */
  protected T decodeAvro(
      ByteBuffer encodedData,
      Schema writerSchema,
      Schema readerSchema,
      T reuse)
      throws IOException {
    final DatumReader<T> reader = createDatumReader(writerSchema, readerSchema);
    return reader.read(reuse,
        DecoderFactory.get().binaryDecoder(
            encodedData.array(),
            encodedData.position(),
            encodedData.limit() - encodedData.position(),
            null));
  }
}
