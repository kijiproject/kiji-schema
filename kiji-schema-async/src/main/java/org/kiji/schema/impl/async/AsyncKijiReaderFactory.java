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
import java.util.Map;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiReaderFactory;
import org.kiji.schema.layout.CellSpec;

/** Factory for Table Writers. */
@ApiAudience.Private
public final class HBaseKijiReaderFactory implements KijiReaderFactory {

  /** HBaseKijiTable for this writer factory. */
  private final HBaseKijiTable mTable;

  /**
   * Initializes a factory for HBaseKijiTable readers.
   *
   * @param table HBaseKijiTable for which to construct readers.
   */
  public HBaseKijiReaderFactory(HBaseKijiTable table) {
    mTable = table;
  }

  /** {@inheritDoc} */
  @Override
  public HBaseKijiTable getTable() {
    return mTable;
  }

  /** {@inheritDoc} */
  @Override
  public HBaseKijiTableReader openTableReader() throws IOException {
    return HBaseKijiTableReader.create(mTable);
  }

  /** {@inheritDoc} */
  @Override
  public HBaseKijiTableReader openTableReader(Map<KijiColumnName, CellSpec> overrides)
      throws IOException {
    return HBaseKijiTableReader.createWithCellSpecOverrides(mTable, overrides);
  }

  /** {@inheritDoc} */
  @Override
  public HBaseKijiTableReaderBuilder readerBuilder() {
    return HBaseKijiTableReaderBuilder.create(mTable);
  }

}
