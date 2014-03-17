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

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Content of a Kiji cell.
 *
 * DecodedCell is obtained through a KijiCellDecoder.
 *
 * @param <T> Type of the data in the cell.
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class DecodedCell<T> {
  /**
   * Constant representing a Decoded cell without a Schema. This can be used for reader or writer
   * Schemas.
   */
  public static final Schema NO_SCHEMA = null;

  /** Schema used to read the cell data, or null for a non-Avro values. */
  private final Schema mReaderSchema;

  /** Schema used to write the cell data, or null for a non-Avro values. */
  private Schema mWriterSchema;

  /** Decoded cell content. */
  private final T mData;

  /**
   * Initializes a DecodedCell instance.
   *
   * @param writerSchema Avro schema used to encode the cell, or null for non-Avro values.
   * @param readerSchema Avro schema used to decode the cell, or null for non-Avro values.
   * @param data Cell content.
   */
  public DecodedCell(Schema writerSchema, Schema readerSchema, T data) {
    mWriterSchema = writerSchema;
    mReaderSchema = readerSchema;
    mData = data;
  }

  /**
   * Initializes a DecodedCell instance.
   *
   * @param writerAndReaderSchema Avro schema used to encode and decode the cell, or null for
   *     non-Avro values.
   * @param data Cell content.
   */
  public DecodedCell(Schema writerAndReaderSchema, T data) {
    mWriterSchema = writerAndReaderSchema;
    mReaderSchema = writerAndReaderSchema;
    mData = data;
  }

  /** @return the Avro schema used to decode the cell content, or null for non-Avro values. */
  public Schema getReaderSchema() {
    return mReaderSchema;
  }

  /** @return the Avro schema used to encode the cell content, or null for non-Avro values. */
  public Schema getWriterSchema() {
    return mWriterSchema;
  }

  /** @return the decoded cell content. */
  public T getData() {
    return mData;
  }

  /**
   * Determines whether the data contained in this DecodedCell is equivalent to another.  The
   * data is equivalent if they have the same schema and the same data, regardless of location.
   *
   * @param obj The object to compare.
   * @return Whether this contains the same data as the other DecodedCell.
   */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof DecodedCell<?>)) {
      return false;
    }
    final DecodedCell<?> other = (DecodedCell<?>) obj;
    if (!Objects.equal(this.getWriterSchema(), other.getWriterSchema())) {
      return false;
    }
    if (!Objects.equal(this.getReaderSchema(), other.getReaderSchema())) {
      return false;
    }

    Object data = getData();
    Object otherData = other.getData();
    // UTF8 strings don't compare well with other CharSequences:
    if ((data instanceof Utf8) ^ (otherData instanceof Utf8)) {
      data = data.toString();
      otherData = otherData.toString();
    }
    return data.equals(otherData);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(getWriterSchema(), getData());
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    final ToStringHelper helper = Objects.toStringHelper(DecodedCell.class);
    if (mWriterSchema == null) {
      helper.add("counter", mData);
    } else {
      helper
          .add("avro", mData)
          .add("writerSchema", mWriterSchema);
    }
    return helper.toString();
  }
}
