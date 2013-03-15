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

import com.google.common.base.Preconditions;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.impl.CounterCellDecoder;
import org.kiji.schema.impl.GenericCellDecoder;
import org.kiji.schema.layout.CellSpec;

/**
 * Factory for Kiji cell decoders using GenericCellDecoder to handle record-based schemas.
 */
@ApiAudience.Framework
public final class GenericCellDecoderFactory implements KijiCellDecoderFactory {
  /** Singleton instance. */
  private static final GenericCellDecoderFactory SINGLETON = new GenericCellDecoderFactory();

  /** @return an instance of the GenericCellDecoderFactory. */
  public static KijiCellDecoderFactory get() {
    return SINGLETON;
  }

  /** Singleton constructor. */
  private GenericCellDecoderFactory() {
  }

  /** {@inheritDoc} */
  @Override
  public <T> KijiCellDecoder<T> create(CellSpec cellSpec) throws IOException {
    Preconditions.checkNotNull(cellSpec);
    switch (cellSpec.getCellSchema().getType()) {
    case CLASS:
    case INLINE:
      return new GenericCellDecoder(cellSpec);
    case COUNTER:
      // purposefully forget the type (long) param of cell decoders for counters.
      return (KijiCellDecoder<T>) CounterCellDecoder.get();
    default:
      throw new RuntimeException("Unhandled cell encoding: " + cellSpec.getCellSchema());
    }
  }

}
