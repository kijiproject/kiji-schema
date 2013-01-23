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

import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.KijiCellEncoder;

/** Encoder for Kiji counters. */
@ApiAudience.Private
public final class CounterCellEncoder implements KijiCellEncoder {
  /** Singleton instance. */
  private static final CounterCellEncoder SINGLETON = new CounterCellEncoder();

  /** @return the singleton encoder for counters. */
  public static CounterCellEncoder get() {
    return SINGLETON;
  }

  /** Singleton constructor. */
  private CounterCellEncoder() {
  }

  /** {@inheritDoc} */
  @Override
  public byte[] encode(DecodedCell<?> cell) throws IOException {
    return encode(cell.getData());
  }

  /** {@inheritDoc} */
  @Override
  public <T> byte[] encode(T cellValue) throws IOException {
    return Bytes.toBytes(((Number) cellValue).longValue());
  }
}
