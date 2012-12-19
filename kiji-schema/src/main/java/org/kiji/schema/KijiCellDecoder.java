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

import org.kiji.annotations.ApiAudience;

/**
 * Interface for Kiji cell decoders.
 *
 * A cell decoder is specific to one column and decodes only one type of value.
 * Cell decoders are instantiated via {@link KijiCellDecoderFactory}.
 *
 * @param <T> Type of the values being decoded.
 */
@ApiAudience.Framework
public interface KijiCellDecoder<T> {
  /**
   * Decodes a Kiji cell from its binary-encoded form.
   *
   * @param bytes Binary encoded Kiji cell.
   * @return the decoded KijiCell.
   * @throws IOException on I/O error.
   */
  KijiCell<T> decodeCell(byte[] bytes) throws IOException;

  /**
   * Decodes a Kiji cell from its binary-encoded form.
   *
   * @param bytes Binary encoded Kiji cell value.
   * @return the decoded cell value.
   * @throws IOException on I/O error.
   */
  T decodeValue(byte[] bytes) throws IOException;
}
