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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.hadoop.hbase.HConstants;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCounter;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.mapreduce.KijiDelete;
import org.kiji.schema.mapreduce.KijiDelete.KijiDeleteScope;
import org.kiji.schema.mapreduce.KijiIncrement;
import org.kiji.schema.mapreduce.KijiPut;

/**
 * Abstract base class for {@link org.kiji.schema.KijiTableWriter}s. This class provides
 * implementations for the various convenience methods provided by KijiTableWriters that
 * delegate to methods processing {@link org.kiji.schema.mapreduce.KijiMutation}s.
 *
 * To implement your own KijiTableWriter using BaseKijiTableWriter:
 * <code>
 *   <pre>
 *   public class MyKijiTableWriter extends BaseKijiTableWriter {
 *     public void put(KijiPut put) {
 *       // Your implementation here...
 *     }
 *
 *     public void increment(KijiIncrement increment) {
 *       // Your implementation here...
 *     }
 *
 *     public void delete(KijiDelete delete) {
 *       // Your implementation here...
 *     }
 *
 *     // ...
 *   }
 *   </pre>
 * </code>
 */
public abstract class BaseKijiTableWriter implements KijiTableWriter {

  // Convenience methods for put:

  /** {@inheritDoc} */
  @Override
  public void put(EntityId entityId, String family, String qualifier, CharSequence value)
      throws IOException {
    put(entityId, family, qualifier, Long.MAX_VALUE, value);
  }

  /** {@inheritDoc} */
  @Override
  public void put(EntityId entityId, String family, String qualifier, GenericContainer value)
      throws IOException {
    put(entityId, family, qualifier, Long.MAX_VALUE, value);
  }

  /** {@inheritDoc} */
  @Override
  public void put(EntityId entityId, String family, String qualifier, KijiCell<?> cell)
      throws IOException {
    put(entityId, family, qualifier, Long.MAX_VALUE, cell);
  }

  /** {@inheritDoc} */
  @Override
  public void put(EntityId entityId, String family, String qualifier, boolean value)
      throws IOException {
    put(entityId, family, qualifier, Long.MAX_VALUE, value);
  }

  /** {@inheritDoc} */
  @Override
  public void put(EntityId entityId, String family, String qualifier, byte[] value)
      throws IOException {
    put(entityId, family, qualifier, Long.MAX_VALUE, value);
  }

  /** {@inheritDoc} */
  @Override
  public void put(EntityId entityId, String family, String qualifier, double value)
      throws IOException {
    put(entityId, family, qualifier, Long.MAX_VALUE, value);
  }

  /** {@inheritDoc} */
  @Override
  public void put(EntityId entityId, String family, String qualifier, float value)
      throws IOException {
    put(entityId, family, qualifier, Long.MAX_VALUE, value);
  }

  /** {@inheritDoc} */
  @Override
  public void put(EntityId entityId, String family, String qualifier, int value)
      throws IOException {
    put(entityId, family, qualifier, Long.MAX_VALUE, value);
  }

  /** {@inheritDoc} */
  @Override
  public void put(EntityId entityId, String family, String qualifier, long value)
      throws IOException {
    put(entityId, family, qualifier, Long.MAX_VALUE, value);
  }

  /** {@inheritDoc} */
  @Override
  public void put(EntityId entityId, String family, String qualifier, long timestamp,
      CharSequence value) throws IOException {
    put(entityId, family, qualifier, timestamp,
        new KijiCell<CharSequence>(Schema.create(Schema.Type.STRING), value));
  }

  /** {@inheritDoc} */
  @Override
  public void put(EntityId entityId, String family, String qualifier, long timestamp,
      GenericContainer value) throws IOException {
    put(entityId, family, qualifier, timestamp,
        new KijiCell<GenericContainer>(value.getSchema(), value));
  }

  /** {@inheritDoc} */
  @Override
  public void put(EntityId entityId, String family, String qualifier, long timestamp, boolean value)
      throws IOException {
    put(entityId, family, qualifier, timestamp,
        new KijiCell<Boolean>(Schema.create(Schema.Type.BOOLEAN), value));
  }

  /** {@inheritDoc} */
  @Override
  public void put(EntityId entityId, String family, String qualifier, long timestamp, byte[] value)
      throws IOException {
    put(entityId, family, qualifier, timestamp,
        new KijiCell<ByteBuffer>(Schema.create(Schema.Type.BYTES), ByteBuffer.wrap(value)));
  }

  /** {@inheritDoc} */
  @Override
  public void put(EntityId entityId, String family, String qualifier, long timestamp, double value)
      throws IOException {
    put(entityId, family, qualifier, timestamp,
        new KijiCell<Double>(Schema.create(Schema.Type.DOUBLE), value));
  }

  /** {@inheritDoc} */
  @Override
  public void put(EntityId entityId, String family, String qualifier, long timestamp, float value)
      throws IOException {
    put(entityId, family, qualifier, timestamp,
        new KijiCell<Float>(Schema.create(Schema.Type.FLOAT), value));
  }

  /** {@inheritDoc} */
  @Override
  public void put(EntityId entityId, String family, String qualifier, long timestamp, int value)
      throws IOException {
    put(entityId, family, qualifier, timestamp,
        new KijiCell<Integer>(Schema.create(Schema.Type.INT), value));
  }

  /** {@inheritDoc} */
  @Override
  public void put(EntityId entityId, String family, String qualifier, long timestamp, long value)
      throws IOException {
    put(entityId, family, qualifier, timestamp,
        new KijiCell<Long>(Schema.create(Schema.Type.LONG), value));
  }

  /** {@inheritDoc} */
  @Override
  public void put(EntityId entityId, String family, String qualifier, long timestamp,
      KijiCell<?> cell) throws IOException {
    put(new KijiPut(entityId, family, qualifier, timestamp, cell));
  }

  /** {@inheritDoc} */
  @Override
  public abstract void put(KijiPut put)
      throws IOException;


  // Convenience methods for counters:

  /** {@inheritDoc} */
  @Override
  public void setCounter(EntityId entityId, String family, String qualifier, long value)
      throws IOException {
    put(entityId, family, qualifier, value);
  }

  /** {@inheritDoc} */
  @Override
  public KijiCounter increment(EntityId entityId, String family, String qualifier, long amount)
      throws IOException {
    return increment(new KijiIncrement(entityId, family, qualifier, amount));
  }

  /** {@inheritDoc} */
  @Override
  public abstract KijiCounter increment(KijiIncrement increment)
      throws IOException;


  // Convenience methods for deletes:

  /** {@inheritDoc} */
  @Override
  public void deleteRow(EntityId entityId)
      throws IOException {
    deleteRow(entityId, HConstants.LATEST_TIMESTAMP);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteRow(EntityId entityId, long upToTimestamp)
      throws IOException {
    delete(new KijiDelete(entityId, upToTimestamp));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(EntityId entityId, String family)
      throws IOException {
    deleteFamily(entityId, family, HConstants.LATEST_TIMESTAMP);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(EntityId entityId, String family, long upToTimestamp)
      throws IOException {
    delete(new KijiDelete(entityId, family, upToTimestamp, KijiDeleteScope.MULTIPLE_VERSIONS));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(EntityId entityId, String family, String qualifier)
      throws IOException {
    deleteColumn(entityId, family, qualifier, HConstants.LATEST_TIMESTAMP);
  }

  @Override
  public void deleteColumn(EntityId entityId, String family, String qualifier, long upToTimestamp)
      throws IOException {
    delete(new KijiDelete(entityId, family, qualifier, upToTimestamp,
        KijiDeleteScope.MULTIPLE_VERSIONS));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(EntityId entityId, String family, String qualifier)
      throws IOException {
    deleteCell(entityId, family, qualifier, HConstants.LATEST_TIMESTAMP);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(EntityId entityId, String family, String qualifier, long timestamp)
      throws IOException {
    delete(new KijiDelete(entityId, family, qualifier, timestamp, KijiDeleteScope.SINGLE_VERSION));
  }

  /** {@inheritDoc} */
  @Override
  public abstract void delete(KijiDelete delete)
      throws IOException;


  // Cleanup methods:

  /** {@inheritDoc} */
  @Override
  public void flush() throws IOException {
    // No-op by default.
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    // No-op by default.
  }
}
