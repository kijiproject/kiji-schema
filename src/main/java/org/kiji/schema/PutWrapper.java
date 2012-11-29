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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;

import org.kiji.schema.layout.ColumnNameTranslator;

/**
 * Used by implementations of {@link WrappedDataWriter}s to wrap data in {@link Put}s. Internally
 * wrapped data elements are stored in {@link KeyValue}s and then aggregated into {@link Put}s when
 * {@link PutWrapper#getWrapped()} is called.
 *
 * @see KijiDataWrapper
 */
public class PutWrapper
    extends KeyValueBackedWrapper<Put> {
  /**
   * Creates an instance.
   *
   * @param cellEncoder The cell encoder to use when wrapping data.
   * @param columnNameTranslator The translator used to convert from Kiji column names to HBase
   * column names.
   */
  public PutWrapper(KijiCellEncoder cellEncoder, ColumnNameTranslator columnNameTranslator) {
    super(cellEncoder, columnNameTranslator);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put wrap(EntityId id, String family, String qualifier, Schema schema, long timestamp,
      Object value) throws IOException {
    KeyValue wrapped = wrapInKeyValue(id, family, qualifier, schema, timestamp, value);
    return new Put(wrapped.getRow()).add(wrapped);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<Put> getWrapped() throws IOException {
    List<Put> returnMe = new ArrayList<Put>();
    Collection<List<KeyValue>> elements = getBuffer().getBuffers();

    for (List<KeyValue> keyValues : elements) {
      Put put = new Put(keyValues.get(0).getRow());
      for (KeyValue keyValue : keyValues) {
        put.add(keyValue);
      }
      returnMe.add(put);
    }

    return returnMe;
  }
}
