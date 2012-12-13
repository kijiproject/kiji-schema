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

import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.KijiCellFormat;
import org.kiji.schema.KijiSchemaTable;

/**
 * Decodes specific avro data from a Kiji cell.
 *
 * @param <T> The type of the decoded data.
 */
@ApiAudience.Private
public final class SpecificCellDecoder<T> extends KijiCellDecoder<T> {
  /**
   * Use a {@link org.kiji.schema.KijiCellDecoderFactory} to get an instance.
   *
   * @param schemaTable The kiji schema table.
   * @param readerSchema The expected avro schema when reading data.
   * @param format Cell coding format.
   */
  public SpecificCellDecoder(
      KijiSchemaTable schemaTable,
      Schema readerSchema,
      KijiCellFormat format) {
    super(schemaTable, readerSchema, format);
  }

  @Override
  protected T decodeData(ByteBuffer encodedData, Schema writerSchema, Schema readerSchema, T reuse)
      throws IOException {
    SpecificDatumReader<T> reader = new SpecificDatumReader<T>(writerSchema, readerSchema);
    return reader.read(reuse,
        DecoderFactory.get().binaryDecoder(
            encodedData.array(),
            encodedData.position(),
            encodedData.limit() - encodedData.position(),
            null));
  }
}
