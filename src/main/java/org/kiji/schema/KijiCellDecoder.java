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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.util.ByteStreamArray;
import org.kiji.schema.util.BytesKey;
import org.kiji.schema.util.Hasher;

/**
 * Base class for decoders that read data from kiji cells.
 *
 * Kiji cells are encoded as Extension records, although for efficiency reasons, this class
 * provides a custom decoder that avoid copying bytes unnecessarily.
 *
 * @param <T> The type of the decoded cell data.
 */
@ApiAudience.Framework
public abstract class KijiCellDecoder<T> {
  /** The Kiji schema table. */
  private final KijiSchemaTable mSchemaTable;

  /** The expected Avro schema when reading the cells. */
  private final Schema mReaderSchema;

  /** Cell coding format. */
  private final KijiCellFormat mFormat;

  /**
   * Use a {@link org.kiji.schema.KijiCellDecoderFactory} instead to create instances.
   *
   * @param schemaTable The Kiji schema table.
   * @param readerSchema The expected schema when decoding the data from the cells.
   * @param format Kiji cell encoding format.
   */
  protected KijiCellDecoder(KijiSchemaTable schemaTable,
      Schema readerSchema,
      KijiCellFormat format) {
    mSchemaTable = schemaTable;
    mReaderSchema = readerSchema;
    mFormat = format;
    Preconditions.checkArgument(
        !((mFormat == KijiCellFormat.NONE) && (null == mReaderSchema)),
        "Reader schema required when serializing cells without schema hash or ID.");
  }

  /**
   * Decodes the serialized bytes into a KijiCell.
   *
   * @param encodedBytes The bytes from an HBase table cell.
   * @return The decoded KijiCell.
   * @throws IOException If there is an error.
   */
  public KijiCell<T> decode(byte[] encodedBytes) throws IOException {
    return decode(encodedBytes, null);
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
  public KijiCell<T> decode(byte[] bytes, T reuse) throws IOException {
    if (0 == bytes.length) {
      // Probably a trimmed row. Throw an exception.
      throw new IOException("Attempting to decode an empty, filtered table cell.");
    }

    final ByteStreamArray byteStream = new ByteStreamArray(bytes);

    Schema writerSchema = null;
    switch (this.mFormat) {
    case HASH:
      final BytesKey schemaHash = new BytesKey(byteStream.readBytes(Hasher.HASH_SIZE_BYTES));
      writerSchema = mSchemaTable.getSchema(schemaHash);
      if (null == writerSchema) {
        throw new IOException(String.format("Unable to find schema hash %s in schema table.",
            schemaHash));
      }
      break;
    case UID:
      final long schemaId = byteStream.readVarInt64();
      writerSchema = mSchemaTable.getSchema(schemaId);
      if (null == writerSchema) {
        throw new IOException(String.format("Unable to find schema ID %d in schema table.",
            schemaId));
      }
      break;
    case NONE:
      // Schema is not encoded in the byte stream, but is known externally:
      Preconditions.checkState(null != mReaderSchema, "Reader schema unspecified");
      writerSchema = mReaderSchema;
      break;
    default:
      throw new RuntimeException("Unhandled case: " + this.mFormat);
    }

    final ByteBuffer binaryData =
        ByteBuffer.wrap(bytes, byteStream.getOffset(), bytes.length - byteStream.getOffset());

    // Assume that the reader and writer schema are the same if not specified.
    final Schema schema = (mReaderSchema == null) ? writerSchema : mReaderSchema;
    final T data = decodeData(binaryData, writerSchema, schema, reuse);
    return new KijiCell<T>(writerSchema, data);
  }

  /**
   * Gets the portion of the encoded byte array from an HBase table cell that has the avro-encoded
   * data payload according to the configured cell format.
   *
   * @param bytes The bytes from an HBase table cell.
   * @return the portion of the encoded byte array that contains the binary-encoded avro message.
   * @throws IOException on I/O error (eg. decoding error).
   */
  public ByteBuffer getPayload(byte[] bytes) throws IOException {
    return getPayload(bytes, mFormat);
  }

  /**
   * Gets the portion of the encoded byte array from an HBase table cell that has the avro-encoded
   * data payload.
   *
   * @param bytes The bytes from an HBase table cell.
   * @param format Kiji cell format.
   * @return the portion of the encoded byte array that contains the binary-encoded avro message.
   * @throws IOException on I/O error (eg. decoding error).
   */
  public ByteBuffer getPayload(byte[] bytes, KijiCellFormat format) throws IOException {
    final ByteStreamArray byteStream = new ByteStreamArray(bytes);

    switch (format) {
    case HASH:
      /* final byte[Hasher.HASH_SIZE_BYTES] unusedSchemaHash = */
      byteStream.skip(Hasher.HASH_SIZE_BYTES);
      break;
    case UID:
      /* final long unusedSchemaId = */
      byteStream.readVarInt64();
      break;
    case NONE:
      // Nothing to read
      break;
    default: throw new RuntimeException("Unhandled case: " + this.mFormat);
    }

    return ByteBuffer.wrap(bytes, byteStream.getOffset(), bytes.length - byteStream.getOffset());
  }

  /** @return the cell coding format used to decode from bytes. */
  public KijiCellFormat getFormat() {
    return this.mFormat;
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
  protected abstract T decodeData(
      ByteBuffer encodedData,
      Schema writerSchema,
      Schema readerSchema,
      T reuse) throws IOException;
}
