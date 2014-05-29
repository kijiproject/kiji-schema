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

package org.kiji.schema.impl.hbase;

import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.AtomicKijiPutter;
import org.kiji.schema.KijiBufferedWriter;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiWriterFactory;

/** Factory for Table Writers. */
@ApiAudience.Private
public final class HBaseKijiWriterFactory implements KijiWriterFactory {

  /** HBaseKijiTable for this writer factory. */
  private final HBaseKijiTable mTable;

  /**
   * Constructor for this writer factory.
   *
   * @param table The HBaseKijiTable to which this writer factory's writers write.
   */
  public HBaseKijiWriterFactory(HBaseKijiTable table) {
    mTable = table;
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableWriter openTableWriter() throws IOException {
    return new HBaseKijiTableWriter(mTable);
  }

  /** {@inheritDoc} */
  @Override
  public AtomicKijiPutter openAtomicPutter() throws IOException {
    return new HBaseAtomicKijiPutter(mTable);
  }

  /** {@inheritDoc} */
  @Override
  public KijiBufferedWriter openBufferedWriter() throws IOException {
    return new HBaseKijiBufferedWriter(mTable);
  }
}
