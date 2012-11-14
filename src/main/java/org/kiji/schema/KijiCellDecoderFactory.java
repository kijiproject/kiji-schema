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

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

/**
 * A factory for creating KijiCellDecoders.
 */
public abstract class KijiCellDecoderFactory {
  /** The kiji schema table for looking up full schemas. */
  protected final KijiSchemaTable mSchemaTable;

  /**
   * Creates a new <code>KijiCellDecoderFactory</code> instance.
   *
   * @param schemaTable The kiji schema table.
   */
  public KijiCellDecoderFactory(KijiSchemaTable schemaTable) {
    mSchemaTable = schemaTable;
  }

  /**
   * Gets the schema table that this decoder reads from.
   *
   * @return The schema table.
   */
  public KijiSchemaTable getSchemaTable() {
    return mSchemaTable;
  }

  /**
   * Creates a new KijiCellDecoder using a reader schema equal to the writer schema.
   *
   * @param <T> The type of data the decoder decodes.
   * @param format Cell encoding format.
   * @return A new KijiCellDecoder.
   */
  public <T> KijiCellDecoder<T> create(KijiCellFormat format) {
    return create((Schema) null, format);
  }

  /**
   * Creates a new KijiCellDecoder using an expected reader schema.
   *
   * @param <T> The type of data the decoder decodes.
   * @param readerSchema The avro schema the reader expects.
   * @param format Cell encoding format.
   * @return A new KijiCellDecoder.
   */
  public abstract <T> KijiCellDecoder<T> create(Schema readerSchema, KijiCellFormat format);

  /**
   * Creates a new KijiCellDecoder using an expected specific record class.  Only implemented for
   * SpecificRecords
   *
   * @param <T> The type of the specific record the decoder decodes.
   * @param specificRecordClass The class of the decoded data.
   * @param format Cell encoding format.
   * @return A new KijiCellDecoder.
   */
  public abstract <T extends SpecificRecord> KijiCellDecoder<T> create(
      Class<T> specificRecordClass,
      KijiCellFormat format);
}
