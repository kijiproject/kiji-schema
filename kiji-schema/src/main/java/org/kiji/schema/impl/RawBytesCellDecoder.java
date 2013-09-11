/**
 * (c) Copyright 2013 WibiData, Inc.
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

import com.google.common.base.Preconditions;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.layout.CellSpec;

/**
 * Deserializes an HBase cell as a raw byte array.
 *
 * @param <T> The type of the decoded cell data. Should be byte[].
 */
@ApiAudience.Private
public class RawBytesCellDecoder<T> implements KijiCellDecoder<T> {

  /** Specification of the cell encoding. */
  private final CellSpec mCellSpec;

  /**
   * Initializes a new RawBytesCellDecoder.
   *
   * @param cellSpec Specification of the cell encoding.
   * @throws IOException on I/O error.
   */
  public RawBytesCellDecoder(CellSpec cellSpec) throws IOException {
    mCellSpec = Preconditions.checkNotNull(cellSpec);
    Preconditions.checkArgument(
        mCellSpec.getCellSchema().getType() == SchemaType.RAW_BYTES);
  }

  /** {@inheritDoc} */
  @Override
  public DecodedCell<T> decodeCell(byte[] encodedBytes) throws IOException {
    return new DecodedCell(null, encodedBytes);
  }

  /** {@inheritDoc} */
  @Override
  public T decodeValue(byte[] bytes) throws IOException {
    return (T) bytes;
  }
}
