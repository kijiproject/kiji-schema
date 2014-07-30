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
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiReaderFactory;
import org.kiji.schema.impl.hbase.HBaseKijiTableReaderBuilder;
import org.kiji.schema.layout.CellSpec;

/** Factory for Table Writers. */
@ApiAudience.Private
public final class AsyncKijiReaderFactory implements KijiReaderFactory {

  /** HBaseKijiTable for this writer factory. */
  private final AsyncKijiTable mTable;

  /**
   * Initializes a factory for HBaseKijiTable readers.
   *
   * @param table HBaseKijiTable for which to construct readers.
   */
  public AsyncKijiReaderFactory(AsyncKijiTable table) {
    mTable = table;
  }

  /** {@inheritDoc} */
  @Override
  public AsyncKijiTable getTable() {
    // TODO(gabe): Replace this with asynchbase
    throw new UnsupportedOperationException("Not yet implemented to work with AsyncHBase");

    /*
    return mTable;
    */
  }

  /** {@inheritDoc} */
  @Override
  public AsyncKijiTableReader openTableReader() throws IOException {
    // TODO(gabe): Replace this with asynchbase
    throw new UnsupportedOperationException("Not yet implemented to work with AsyncHBase");

    /*
    return AsyncKijiTableReader.create(mTable);
    */
  }

  /** {@inheritDoc} */
  @Override
  public AsyncKijiTableReader openTableReader(Map<KijiColumnName, CellSpec> overrides)
      throws IOException {
    // TODO(gabe): Replace this with asynchbase
    throw new UnsupportedOperationException("Not yet implemented to work with AsyncHBase");

    /*
    return AsyncKijiTableReader.createWithCellSpecOverrides(mTable, overrides);
    */
  }

  /** {@inheritDoc} */
  @Override
  public org.kiji.schema.AsyncKijiTableReader openAsyncTableReader() throws IOException {
    // TODO(gabe): Replace this with asynchbase
    throw new UnsupportedOperationException("Not yet implemented to work with AsyncHBase");
  }

  /** {@inheritDoc} */
  @Override
  public HBaseKijiTableReaderBuilder readerBuilder() {
    // TODO(gabe): Replace this with asynchbase
    throw new UnsupportedOperationException("Not yet implemented to work with AsyncHBase");

    /*
    return AsyncKijiTableReaderBuilder.create(mTable);
    */
  }

}
