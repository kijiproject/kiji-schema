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

package org.kiji.schema.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NavigableMap;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCounter;
import org.kiji.schema.KijiRowData;


/**
 * An abstract base class that implements the convenience methods of the KijiRowData interface.
 */
@ApiAudience.Private
public abstract class AbstractKijiRowData implements KijiRowData {
  /** {@inheritDoc} */
  @Override
  public long getCounterValue(String family, String qualifier) throws IOException {
    KijiCounter counter = getCounter(family, qualifier);
    if (null == counter) {
      return 0L;
    }
    return counter.getValue();
  }

  /** {@inheritDoc} */
  @Override
  public long getCounterValue(String family, String qualifier, long timestamp) throws IOException {
    KijiCounter counter = getCounter(family, qualifier, timestamp);
    if (null == counter) {
      return 0L;
    }
    return counter.getValue();
  }

  /** {@inheritDoc} */
  @Override
  public <T> T getValue(String family, String qualifier, Schema readerSchema) throws IOException {
    KijiCell<T> kijiCell = getCell(family, qualifier, readerSchema);
    return null == kijiCell ? null : kijiCell.getData();
  }

  /** {@inheritDoc} */
  @Override
  public <T> T getValue(String family, String qualifier, long timestamp, Schema readerSchema)
      throws IOException {
    KijiCell<T> kijiCell = getCell(family, qualifier, timestamp, readerSchema);
    return null == kijiCell ? null : kijiCell.getData();
  }

  /** {@inheritDoc} */
  @Override
  public <T extends SpecificRecord> T getValue(String family, String qualifier, Class<T> type)
      throws IOException {
    KijiCell<T> kijiCell = getCell(family, qualifier, type);
    return null == kijiCell ? null : kijiCell.getData();
  }

  /** {@inheritDoc} */
  @Override
  public <T extends SpecificRecord> T getValue(
      String family, String qualifier, long timestamp, Class<T> type) throws IOException {
    KijiCell<T> kijiCell = getCell(family, qualifier, timestamp, type);
    return null == kijiCell ? null : kijiCell.getData();
  }

  /** {@inheritDoc} */
  @Override
  public CharSequence getStringValue(String family, String qualifier) throws IOException {
    return getValue(family, qualifier, Schema.create(Schema.Type.STRING));
  }

  /** {@inheritDoc} */
  @Override
  public CharSequence getStringValue(String family, String qualifier, long timestamp)
      throws IOException {
    return getValue(family, qualifier, timestamp, Schema.create(Schema.Type.STRING));
  }

  /** {@inheritDoc} */
  @Override
  public Integer getIntValue(String family, String qualifier) throws IOException {
    return getValue(family, qualifier, Schema.create(Schema.Type.INT));
  }

  /** {@inheritDoc} */
  @Override
  public Integer getIntValue(String family, String qualifier, long timestamp) throws IOException {
    return getValue(family, qualifier, timestamp, Schema.create(Schema.Type.INT));
  }

  /** {@inheritDoc} */
  @Override
  public Long getLongValue(String family, String qualifier) throws IOException {
    return getValue(family, qualifier, Schema.create(Schema.Type.LONG));
  }

  /** {@inheritDoc} */
  @Override
  public Long getLongValue(String family, String qualifier, long timestamp) throws IOException {
    return getValue(family, qualifier, timestamp, Schema.create(Schema.Type.LONG));
  }

  /** {@inheritDoc} */
  @Override
  public Float getFloatValue(String family, String qualifier) throws IOException {
    return getValue(family, qualifier, Schema.create(Schema.Type.FLOAT));
  }

  /** {@inheritDoc} */
  @Override
  public Float getFloatValue(String family, String qualifier, long timestamp) throws IOException {
    return getValue(family, qualifier, timestamp, Schema.create(Schema.Type.FLOAT));
  }

  /** {@inheritDoc} */
  @Override
  public Double getDoubleValue(String family, String qualifier) throws IOException {
    return getValue(family, qualifier, Schema.create(Schema.Type.DOUBLE));
  }

  /** {@inheritDoc} */
  @Override
  public Double getDoubleValue(String family, String qualifier, long timestamp) throws IOException {
    return getValue(family, qualifier, timestamp, Schema.create(Schema.Type.DOUBLE));
  }

  /** {@inheritDoc} */
  @Override
  public ByteBuffer getByteArrayValue(String family, String qualifier) throws IOException {
    return getValue(family, qualifier, Schema.create(Schema.Type.BYTES));
  }

  /** {@inheritDoc} */
  @Override
  public ByteBuffer getByteArrayValue(String family, String qualifier, long timestamp)
      throws IOException {
    return getValue(family, qualifier, timestamp, Schema.create(Schema.Type.BYTES));
  }

  /** {@inheritDoc} */
  @Override
  public Boolean getBooleanValue(String family, String qualifier) throws IOException {
    return getValue(family, qualifier, Schema.create(Schema.Type.BOOLEAN));
  }

  /** {@inheritDoc} */
  @Override
  public Boolean getBooleanValue(String family, String qualifier, long timestamp)
      throws IOException {
    return getValue(family, qualifier, timestamp, Schema.create(Schema.Type.BOOLEAN));
  }

  /** {@inheritDoc} */
  @Override
  public NavigableMap<Long, CharSequence> getStringValues(String family, String qualifier)
      throws IOException {
    return getValues(family, qualifier, Schema.create(Schema.Type.STRING));
  }

  /** {@inheritDoc} */
  @Override
  public NavigableMap<Long, Integer> getIntValues(String family, String qualifier)
      throws IOException {
    return getValues(family, qualifier, Schema.create(Schema.Type.INT));
  }

  /** {@inheritDoc} */
  @Override
  public NavigableMap<Long, Long> getLongValues(String family, String qualifier)
      throws IOException {
    return getValues(family, qualifier, Schema.create(Schema.Type.LONG));
  }

  /** {@inheritDoc} */
  @Override
  public NavigableMap<Long, Float> getFloatValues(String family, String qualifier)
      throws IOException {
    return getValues(family, qualifier, Schema.create(Schema.Type.FLOAT));
  }

  /** {@inheritDoc} */
  @Override
  public NavigableMap<Long, Double> getDoubleValues(String family, String qualifier)
      throws IOException {
    return getValues(family, qualifier, Schema.create(Schema.Type.DOUBLE));
  }

  /** {@inheritDoc} */
  @Override
  public NavigableMap<Long, Boolean> getBooleanValues(String family, String qualifier)
      throws IOException {
    return getValues(family, qualifier, Schema.create(Schema.Type.BOOLEAN));
  }

  /** {@inheritDoc} */
  @Override
  public NavigableMap<Long, ByteBuffer> getByteArrayValues(String family, String qualifier)
      throws IOException {
    return getValues(family, qualifier, Schema.create(Schema.Type.BYTES));
  }
}
