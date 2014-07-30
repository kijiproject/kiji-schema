/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.schema.impl.async;

import java.io.IOException;
import java.util.Map;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.AsyncKijiTableReader;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiReaderFactory;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.impl.hbase.HBaseKijiTableReaderBuilder;
import org.kiji.schema.layout.CellSpec;

/** Factory for Table Writers. */
@ApiAudience.Private
public final class AsyncHBaseKijiReaderFactory implements KijiReaderFactory {

  /** HBaseKijiTable for this writer factory. */
  private final AsyncHBaseKijiTable mTable;

  /**
   * Initializes a factory for HBaseKijiTable readers.
   *
   * @param table HBaseKijiTable for which to construct readers.
   */
  public AsyncHBaseKijiReaderFactory(AsyncHBaseKijiTable table) {
    mTable = table;
  }

  /** {@inheritDoc} */
  @Override
  public AsyncHBaseKijiTable getTable() {
    return mTable;
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableReader openTableReader() throws IOException {
    return AsyncHBaseKijiTableReader.create(mTable);
  }

  /** {@inheritDoc} */
  @Override
  public AsyncKijiTableReader openAsyncTableReader() throws IOException {
    // TODO: Implement this using AsyncHBase
    throw new UnsupportedOperationException("Not yet implemented.");
    //return AsyncHBaseAsyncKijiTableReader.create(mTable);
  }

  /** {@inheritDoc} */
  @Override
  public AsyncHBaseKijiTableReader openTableReader(Map<KijiColumnName, CellSpec> overrides)
      throws IOException {
    return AsyncHBaseKijiTableReader.createWithCellSpecOverrides(mTable, overrides);
  }

  /** {@inheritDoc} */
  @Override
  public AsyncHBaseKijiTableReaderBuilder readerBuilder() {
    return AsyncHBaseKijiTableReaderBuilder.create(mTable);
  }

}
