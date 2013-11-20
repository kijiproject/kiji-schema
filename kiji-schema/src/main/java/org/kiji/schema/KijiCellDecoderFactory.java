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
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.impl.BoundColumnReaderSpec;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.KijiTableLayout;

/** Interface for factories of KijiCellDecoder instances. */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
public interface KijiCellDecoderFactory {
  /**
   * Creates a new Kiji cell decoder.
   *
   * @param cellSpec Specification of the cell encoding.
   * @return a new Kiji cell decoder.
   * @throws IOException on I/O error.
   *
   * @param <T> Type of the value to decode.
   */
  <T> KijiCellDecoder<T> create(CellSpec cellSpec) throws IOException;

  /**
   * Creates a new Kiji cell decoder.
   *
   * @param layout KijiTableLayout from which to retrieve storage information.
   * @param spec Specification of the cell encoding.
   * @return a new Kiji cell decoder.
   * @throws IOException on I/O error.
   *
   * @param <T> Type of the value to decode.
   */
  <T> KijiCellDecoder<T> create(KijiTableLayout layout, BoundColumnReaderSpec spec)
      throws IOException;
}
