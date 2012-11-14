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

import org.apache.avro.Schema;
import org.apache.hadoop.hbase.KeyValue;

import org.kiji.schema.impl.KijiDataBuffer;
import org.kiji.schema.layout.ColumnNameTranslator;

/**
 * Base class of wrappers that use {@link KeyValue}s as the internal representation of wrapped data.
 *
 * @param <T> The output data type of wrapped data.
 * @see KijiDataWrapper
 */
public abstract class KeyValueBackedWrapper<T> extends KijiDataWrapper<T> {
  /**
   * A Buffer used to store {@link KeyValue}s before writing.
   */
  private KijiDataBuffer<KeyValue> mBuffer;

  /**
   * Constructs an instance.
   *
   * @param cellEncoder The cell encoder to use while wrapping data.
   * @param columnNameTranslator The translator to use to convert Kiji column names to HBase column
   * names.
   */
  public KeyValueBackedWrapper(KijiCellEncoder cellEncoder,
      ColumnNameTranslator columnNameTranslator) {
    super(cellEncoder, columnNameTranslator);
    mBuffer = new KijiDataBuffer<KeyValue>();
  }

  /** {@inheritDoc} */
  @Override
  public void add(EntityId id, String family, String qualifier, Schema schema, long timestamp,
      Object value) throws IOException {
    getBuffer().add(id, wrapInKeyValue(id, family, qualifier, schema, timestamp, value));
  }

  /** {@inheritDoc} */
  @Override
  public int size() {
    return getBuffer().size();
  }

  /** {@inheritDoc} */
  @Override
  public void clear() {
    getBuffer().clear();
  }

  /**
   * @return The buffer used to store {@link KeyValue}s before writing.
   */
  protected KijiDataBuffer<KeyValue> getBuffer() {
    return mBuffer;
  }

  /**
   * Used to wrap a data element in a {@link KeyValue}.
   *
   * @param entityId The entity id of the row to write to.
   * @param family The family to write the data to.
   * @param qualifier The qualifier of the column to write the data to.
   * @param schema The writer schema for the value.
   * @param timestamp The timestamp at which the data will be written.
   * @param value The data payload.
   * @return The wrapped data element.
   * @throws IOException If there is a problem writing.
   */
  protected KeyValue wrapInKeyValue(EntityId entityId, String family, String qualifier,
      Schema schema, long timestamp, Object value) throws IOException {
    final KijiColumnName column = new KijiColumnName(family, qualifier);
    final KijiCellFormat format = getColumnNameTranslator().getTableLayout().getCellFormat(column);
    final HBaseColumnName hBaseColumnName = getColumnNameTranslator().toHBaseColumnName(column);

    return new KeyValue(
        entityId.getHBaseRowKey(),
        hBaseColumnName.getFamily(),
        hBaseColumnName.getQualifier(),
        timestamp,
        getCellEncoder().encode(new KijiCell<Object>(schema, value), format));
  }
}
