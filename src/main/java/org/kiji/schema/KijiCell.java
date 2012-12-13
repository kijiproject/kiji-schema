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

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;

import org.kiji.annotations.ApiAudience;

/**
 * <p>The data held in one cell of a kiji table.  A KijiCell is a single
 * unit of data in Kiji, addressed by a tuple of (table, row, family,
 * qualifier, timestamp).</p>
 *
 * <p>Use a {@link org.kiji.schema.KijiCellDecoder} or a
 * {@link org.kiji.schema.KijiCellEncoder} to serialize/deserialize this cell in and out of a
 * Kiji table.</p>
 *
 * <p>This class has a Java type parameter <code>T</code>, which should be the Java type
 * determined by the Avro Schema.  The mapping between Avro Schemas and Java types are
 * documented in the Avro Java API documentation:
 *
 *   <ul>
 *     <li><a
 * href="http://avro.apache.org/docs/current/api/java/org/apache/avro/specific/package-summary.html"
 * >Specific types</a></li>
 *     <li><a
 * href="http://avro.apache.org/docs/current/api/java/org/apache/avro/generic/package-summary.html"
 * >Generic types</a></li>
 *   </ul>
 * </p>
 *
 * @param <T> The type of the data in the cell.
 */
@ApiAudience.Public
public final class KijiCell<T> {
  /** The schema used to write the cell data. */
  private Schema mWriterSchema;
  /** The data in the Kiji cell. */
  private T mData;

  /**
   * Constructs a single cell of kiji data.
   *
   * @param writerSchema The schema used to the encode the data in the cell.
   * @param data The data in the cell (the payload).
   */
  public KijiCell(Schema writerSchema, T data) {
    mWriterSchema = writerSchema;
    mData = data;
  }

  /**
   * Gets the schema used to write data in the cell.
   *
   * @return The avro schema used by the writer to encode the data payload.
   */
  public Schema getWriterSchema() {
    return mWriterSchema;
  }

  /**
   * Gets the data in the cell.
   *
   * @return The decoded original data payload.
   */
  public T getData() {
    return mData;
  }

  /**
   * Determines whether this KijiCell is equivalent to another.  They are equivalent if
   * they are both KijiCell objects, they have the same writer schema, and they have the
   * same data.
   *
   * @param obj The other object to compare to.
   * @return Whether they are equivalent.
   */
  @Override
  public boolean equals(Object obj) {
    // Check if they are both KijiCells.
    if (!(obj instanceof KijiCell<?>)) {
      return false;
    }
    KijiCell<?> other = (KijiCell<?>) obj;

    // Check that the schemas are the same.
    if (!getWriterSchema().equals(other.getWriterSchema())) {
      return false;
    }

    // Special case check for strings to work around the weirdness of avro.
    if (getData() instanceof Utf8 || other.getData() instanceof Utf8) {
      return getData().toString().equals(other.getData().toString());
    }
    return getData().equals(other.getData());
  }

  /**
   * This operation is not supported.
   *
   * @return Nothing, since this operation is not supported.
   */
  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }
}
