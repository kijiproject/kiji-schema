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
import java.util.List;

import org.apache.avro.Schema;

import org.kiji.schema.layout.ColumnNameTranslator;

/**
 * Base class for wrappers of data used in {@link KijiDataWriter}s.
 *
 * <p>Data wrappers should be used in conjunction with instances of {@link WrappedDataWriter}s. Each
 * wrapper should provide functionality to wrap an individual data element through the
 * {@link KijiDataWrapper#wrap(EntityId, String, String, Schema, long, Object)} method and wrap
 * multiple elements using the
 * {@link KijiDataWrapper#add(EntityId, String, String, Schema, long, Object)} and
 * {@link KijiDataWrapper#getWrapped()} methods.</p>
 *
 * @param <T> The output data type of wrapped data.
 */
public abstract class KijiDataWrapper<T> {
  /** A Kiji cell encoder to use when writing cells. */
  private KijiCellEncoder mCellEncoder;

  /** A column name translator used for writing. */
  private ColumnNameTranslator mColumnNameTranslator;

  /**
   * Constructs an instance.
   *
   * @param cellEncoder The cell encoder to use while wrapping data.
   * @param columnNameTranslator The translator to use to convert Kiji column names to HBase column
   * names.
   */
  public KijiDataWrapper(KijiCellEncoder cellEncoder, ColumnNameTranslator columnNameTranslator) {
    mCellEncoder = cellEncoder;
    mColumnNameTranslator = columnNameTranslator;
  }

  /**
   * Wraps {@code value} in type specified by {@code T} and adds it to buffer.
   *
   * @param id The entity id of the row to write to.
   * @param family The family to write the data to.
   * @param qualifier The qualifier of the column to write the data to.
   * @param schema The writer schema for the value.
   * @param timestamp The timestamp at which the data will be written.
   * @param value The data payload.
   * @throws IOException If there is a problem writing.
   */
  public abstract void add(EntityId id, String family, String qualifier, Schema schema,
      long timestamp, Object value) throws IOException;

  /**
   * Wraps {@code value} in type specified by {@code T} and returns it.
   *
   * <p> This method should be only used if buffering is not desired. </p>
   *
   * @param id The entity id of the row to write to.
   * @param family The family to write the data to.
   * @param qualifier The qualifier of the column to write the data to.
   * @param schema The writer schema for the value.
   * @param timestamp The timestamp at which the data will be written.
   * @param value The data payload.
   * @return The wrapped datum.
   * @throws IOException If there is a problem writing.
   */
  public abstract T wrap(EntityId id, String family, String qualifier, Schema schema,
      long timestamp, Object value) throws IOException;

  /**
   * @return The list of wrapped datum to write.
   * @throws IOException If there is a problem writing.
   */
  public abstract List<T> getWrapped() throws IOException;

  /**
   * @return The number of elements in the buffer.
   */
  public abstract int size();

  /**
   * Clears any data stored in the buffer.
   */
  public abstract void clear();

  /**
   * A Kiji cell encoder to use when writing cells.
   *
   * @return the CellEncoder
   */
  protected KijiCellEncoder getCellEncoder() {
    return mCellEncoder;
  }

  /**
   * A column name translator used for writing.
   *
   * @return the ColumnNameTranslator
   */
  protected ColumnNameTranslator getColumnNameTranslator() {
    return mColumnNameTranslator;
  }
}
