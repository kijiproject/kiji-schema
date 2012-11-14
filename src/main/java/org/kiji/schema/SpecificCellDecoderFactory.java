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
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

import org.kiji.schema.impl.SpecificCellDecoder;

/**
 * A factory for creating KijiCellDecoders which uses a SpecificCellDecoder
 * to handle record-based schemas.
 */
public class SpecificCellDecoderFactory extends KijiCellDecoderFactory {
  /**
   * Default c'tor.
   *
   * @param schemaTable the kiji schema table.
   */
  public SpecificCellDecoderFactory(KijiSchemaTable schemaTable) {
    super(schemaTable);
  }

  /** {@inheritDoc} */
  @Override
  public <T> KijiCellDecoder<T> create(Schema readerSchema, KijiCellFormat format) {
    return new SpecificCellDecoder<T>(getSchemaTable(), readerSchema, format);
  }

  /** {@inheritDoc} */
  @Override
  public <T extends SpecificRecord> KijiCellDecoder<T> create(Class<T> specificRecordClass,
      KijiCellFormat format) {
    Schema readerSchema = SpecificData.get().getSchema(specificRecordClass);
    return new SpecificCellDecoder<T>(mSchemaTable, readerSchema, format);
  }
}
