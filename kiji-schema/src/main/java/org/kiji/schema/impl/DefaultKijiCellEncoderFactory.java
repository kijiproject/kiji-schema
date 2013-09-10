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

import com.google.common.base.Preconditions;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiCellEncoderFactory;
import org.kiji.schema.layout.CellSpec;

/** Factory for cell encoders. */
@ApiAudience.Private
public final class DefaultKijiCellEncoderFactory implements KijiCellEncoderFactory {
  /** Singleton instance. */
  private static final DefaultKijiCellEncoderFactory SINGLETON =
      new DefaultKijiCellEncoderFactory();

  /** @return the default factory for cell encoders. */
  public static DefaultKijiCellEncoderFactory get() {
    return SINGLETON;
  }

  /** Singleton constructor. */
  private DefaultKijiCellEncoderFactory() {
  }

  /** {@inheritDoc} */
  @Override
  public KijiCellEncoder create(final CellSpec cellSpec) throws IOException {
    Preconditions.checkNotNull(cellSpec);
    switch (cellSpec.getCellSchema().getType()) {
    case INLINE:
    case AVRO:
    case CLASS:
      return new AvroCellEncoder(cellSpec);
    case COUNTER:
      return CounterCellEncoder.get();
    case PROTOBUF:
      return new ProtobufCellEncoder(cellSpec);
    case RAW_BYTES:
      return new RawBytesCellEncoder(cellSpec);
    default:
      throw new RuntimeException("Unhandled cell encoding: " + cellSpec.getCellSchema().getType());
    }
  }
}
