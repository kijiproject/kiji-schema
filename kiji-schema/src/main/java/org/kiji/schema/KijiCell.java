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
import com.google.common.base.Preconditions;
import org.apache.avro.Schema;

import org.kiji.annotations.ApiAudience;

/**
 * KijiCell represents a cell in a Kiji table.
 *
 * It contains the family, qualifier, timestamp
 * that uniquely locates the cell within a table as well as the data in the cell.
 *
 * <p>This class has a Java type parameter <code>T</code>, which should be the Java type
 * determined by the Avro Schema.  The mapping between Avro Schemas and Java types are
 * documented in the Avro Java API documentation:
 *
 * @param <T> Type of data stored in the cell.
 */
@ApiAudience.Framework
public final class KijiCell<T> {
  /** Decoded cell content. */
  private final DecodedCell<T> mDecodedCell;

  /** Kiji column family name. */
  private final String mFamily;

  /** Kiji column qualifier name. */
  private final String mQualifier;

  /** Timestamp of the cell, in ms since the Epoch. */
  private final long mTimestamp;

  /** Type of a Kiji cell. */
  public enum CellType {
    /** Cell encoded with Avro. */
    AVRO,

    /** Cell encoded as a counter. */
    COUNTER
  }

  /**
   * Initializes a KijiCell.
   *
   * @param family Kiji column family name of the cell.
   * @param qualifier Kiji column qualifier name of the cell.
   * @param timestamp Timestamp the cell was written at, in ms since the Epoch.
   * @param decodedCell Decoded cell content.
   */
  public KijiCell(String family, String qualifier, long timestamp, DecodedCell<T> decodedCell) {
    mDecodedCell = Preconditions.checkNotNull(decodedCell);
    mFamily = Preconditions.checkNotNull(family);
    mQualifier = Preconditions.checkNotNull(qualifier);
    mTimestamp = timestamp;
  }

  /** @return the Kiji column family name of this cell. */
  public String getFamily() {
    return mFamily;
  }

  /** @return the Kiji column qualifier name of this cell. */
  public String getQualifier() {
    return mQualifier;
  }

  /** @return the timestamp this cell was written with, in ms since the Epoch. */
  public long getTimestamp() {
    return mTimestamp;
  }

  /** @return the cell content. */
  public T getData() {
    return mDecodedCell.getData();
  }

  /** @return the Avro schema used to encode this cell, or null. */
  public Schema getWriterSchema() {
    return mDecodedCell.getWriterSchema();
  }

  /** @return this cell's encoding type. */
  public CellType getType() {
    if (mDecodedCell.getWriterSchema() == null) {
      return CellType.COUNTER;
    } else {
      return CellType.AVRO;
    }
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(KijiCell.class)
        .add("family", mFamily)
        .add("qualifier", mQualifier)
        .add("timestamp", mTimestamp)
        .add("encoding", getType())
        .add("content", getData())
        .toString();
  }
}
