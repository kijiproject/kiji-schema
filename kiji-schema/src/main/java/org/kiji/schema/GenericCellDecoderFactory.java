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
import org.kiji.annotations.ApiStability;
import org.kiji.schema.impl.BoundColumnReaderSpec;
import org.kiji.schema.impl.CounterCellDecoder;
import org.kiji.schema.impl.GenericCellDecoder;
import org.kiji.schema.impl.ProtobufCellDecoder;
import org.kiji.schema.impl.RawBytesCellDecoder;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Factory for Kiji cell decoders using GenericCellDecoder to handle record-based schemas.
 */
@ApiAudience.Framework
@ApiStability.Evolving
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
      case AVRO:
      case CLASS:
      case INLINE:
        return new GenericCellDecoder<T>(cellSpec);
      case RAW_BYTES:
        return new RawBytesCellDecoder(cellSpec);
      case PROTOBUF:
        return new ProtobufCellDecoder<T>(cellSpec);
      case COUNTER:
        // purposefully forget the type (long) param of cell decoders for counters.
        @SuppressWarnings("unchecked")
        final KijiCellDecoder<T> counterCellDecoder = (KijiCellDecoder<T>) CounterCellDecoder.get();
        return counterCellDecoder;
      default:
        throw new InternalKijiError("Unhandled cell encoding: " + cellSpec.getCellSchema());
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T> KijiCellDecoder<T> create(KijiTableLayout layout, BoundColumnReaderSpec spec)
      throws IOException {
    Preconditions.checkNotNull(layout);
    Preconditions.checkNotNull(spec);
    switch (spec.getColumnReaderSpec().getEncoding()) {
      case RAW_BYTES:
        return new RawBytesCellDecoder<T>(spec);
      case AVRO:
        return new GenericCellDecoder<T>(layout, spec);
      case PROTOBUF:
        return new ProtobufCellDecoder<T>(layout, spec);
      case COUNTER:
        // purposefully forget the type (long) param of cell decoders for counters.
        @SuppressWarnings("unchecked")
        final KijiCellDecoder<T> counterCellDecoder = (KijiCellDecoder<T>) CounterCellDecoder.get();
        return counterCellDecoder;
      default:
        throw new InternalKijiError(
            "Unhandled cell encoding in reader spec: " + spec.getColumnReaderSpec());
    }
  }
}
