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

package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.Map;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiReaderFactory;
import org.kiji.schema.layout.CellSpec;

/** Factory for Table Writers. */
@ApiAudience.Private
public final class CassandraKijiReaderFactory implements KijiReaderFactory {

  /** CassandraKijiTable for this writer factory. */
  private final CassandraKijiTable mTable;

  /**
   * Initializes a factory for CassandraKijiTable readers.
   *
   * @param table CassandraKijiTable for which to construct readers.
   */
  public CassandraKijiReaderFactory(CassandraKijiTable table) {
    mTable = table;
  }

  /** {@inheritDoc} */
  @Override
  public CassandraKijiTable getTable() {
    return mTable;
  }

  /** {@inheritDoc} */
  @Override
  public CassandraKijiTableReader openTableReader() throws IOException {
    return CassandraKijiTableReader.create(mTable);
  }

  /** {@inheritDoc} */
  @Override
  public CassandraKijiTableReader openTableReader(Map<KijiColumnName, CellSpec> overrides)
      throws IOException {
    return CassandraKijiTableReader.createWithCellSpecOverrides(mTable, overrides);
  }

  /** {@inheritDoc} */
  @Override
  public CassandraKijiTableReaderBuilder readerBuilder() {
    return CassandraKijiTableReaderBuilder.create(mTable);
  }

}
