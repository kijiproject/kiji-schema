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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import org.kiji.schema.util.ByteStreamArray;
import org.kiji.schema.util.BytesKey;


/**
 * Serializes a KijiCell.
 *
 * A Kiji cell is encoded as an Extension record, although this class implements the encoder
 * in a specific way for efficiency.
 */
public class KijiCellEncoder {
  /** The kiji schema table. */
  private final KijiSchemaTable mSchemaTable;

  /** A cache of avro datum writers. */
  private final Map<Schema, DatumWriter<Object>> mCachedDatumWriters;

  /** A byte stream for when encoding to a byte array. */
  private final ByteArrayOutputStream mByteArrayOutputStream;

  /** An encoder that writes to the above byte stream. */
  private final Encoder mByteArrayEncoder;

  /** A reusable encoder for when encoding to an arbitrary output stream. */
  private BinaryEncoder mReusableEncoder;

  /**
   * Creates a new <code>KijiCellEncoder</code> instance.
   *
   * @param schemaTable The kiji schema table.
   */
  public KijiCellEncoder(KijiSchemaTable schemaTable) {
    mSchemaTable = schemaTable;
    mCachedDatumWriters = new HashMap<Schema, DatumWriter<Object>>();
    mByteArrayOutputStream = new ByteArrayOutputStream();
    mByteArrayEncoder = EncoderFactory.get().directBinaryEncoder(mByteArrayOutputStream, null);
    mReusableEncoder = null;
  }

  /**
   * Encodes a kiji cell to an output stream that can be later decoded with a {@link
   * org.kiji.schema.KijiCellDecoder}.
   *
   * @param cell The Kiji cell data to encode.
   * @param out The output stream to write the encoded data to.
   * @param format Cell encoding format.
   * @throws IOException If there is an error.
   */
  public void encode(KijiCell<?> cell, OutputStream out, KijiCellFormat format) throws IOException {
    mReusableEncoder = EncoderFactory.get().directBinaryEncoder(out, mReusableEncoder);
    encode(cell, out, mReusableEncoder, format);
  }

  /**
   * Encodes a kiji cell to bytes that can be later decoded with a {@link
   * org.kiji.schema.KijiCellDecoder}.
   *
   * @param cell The Kiji cell data to encode.
   * @param format Cell encoding format.
   * @return The encoded bytes, ready to be stored in an HBase table cell.
   * @throws IOException If there is an error.
   */
  public byte[] encode(KijiCell<?> cell, KijiCellFormat format) throws IOException {
    mByteArrayOutputStream.reset();
    encode(cell, mByteArrayOutputStream, mByteArrayEncoder, format);
    return mByteArrayOutputStream.toByteArray();
  }

  /**
   * Encodes a kiji cell to bytes that can be later decoded with a {@link
   * org.kiji.schema.KijiCellDecoder}.
   *
   * @param cell The Kiji cell to encode.
   * @param out The output stream.
   * @param encoder The avro encoder.
   * @param format Cell encoding format.
   * @throws IOException If there is an error.
   */
  private void encode(
      KijiCell<?> cell,
      OutputStream out,
      Encoder encoder,
      KijiCellFormat format) throws IOException {
    // Here, we manually encode the equivalent of an Extension record declared as:
    //   Extension.newBuilder()
    //       .setSchemaId(schemaId)
    //       .setPayload(cell.getData())
    //       .build();
    // Using the Extension record would be fairly inefficient as we would need to serialize the
    // payload into bytes in the Extension record before serializing the Extension record itself.
    switch (format) {
    case HASH:
      final BytesKey schemaHash = mSchemaTable.getOrCreateSchemaHash(cell.getWriterSchema());
      out.write(schemaHash.getBytes());
      break;
    case UID:
      final long schemaId = mSchemaTable.getOrCreateSchemaId(cell.getWriterSchema());
      encoder.writeFixed(ByteStreamArray.longToVarInt64(schemaId));
      break;
    case NONE:
      // Nothing to encode in this case
      break;
    default:
      throw new RuntimeException("dead code");
    }
    getDatumWriter(cell.getWriterSchema()).write(cell.getData(), encoder);
  }

  /**
   * Gets a datum writer for a schema and caches it.
   *
   * @param schema The writer schema.
   * @return A datum writer for the given schema.
   */
  private DatumWriter<Object> getDatumWriter(Schema schema) {
    DatumWriter<Object> writer = mCachedDatumWriters.get(schema);
    if (null != writer) {
      return writer;
    }
    writer = new SpecificDatumWriter<Object>(schema);
    mCachedDatumWriters.put(schema, writer);
    return writer;
  }
}
