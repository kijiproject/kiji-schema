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

package org.kiji.schema.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCounter;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.mapreduce.KijiDelete.KijiDeleteScope;

/**
 * ContextKijiTableWriter is a {@link KijiTableWriter} used to perform mutations to a
 * Kiji table from within a Hadoop mapper or reducer.
 *
 * This table writer expects the job to be configured to use the {@link KijiTableOutputFormat}.
 * To use this table writer, instantiate it with the provided mapper/reducer context:
 * <code>
 *   <pre>
 *   public void map(K key, V value, Context context) {
 *     final ContextKijiTableWriter writer = new ContextKijiTableWriter
 *
 *     writer.put(entityId, family, qualifier, data);
 *
 *     //...
 *   }
 *   </pre>
 * </code>
 *
 * ContextKijiTableWriter also expects the job to be configured to have an output key class
 * of NullWritable:
 * <code>
 *   <pre>
 *   job.setOutputKeyClass(NullWritable.class);
 *   </pre>
 * </code>
 */
@ApiAudience.Public
public class ContextKijiTableWriter extends KijiTableWriter {
  private final TaskInputOutputContext<?, ?, NullWritable, KijiOutput> mContext;

  /**
   * Builds a new ContextKijiTableWriter.
   *
   * @param context The map-reduce context to write to.
   */
  public ContextKijiTableWriter(TaskInputOutputContext<?, ?, NullWritable, KijiOutput> context) {
    mContext = context;
  }

  /** {@inheritDoc} */
  @Override
  public void put(EntityId entityId, String family, String qualifier, long timestamp,
      KijiCell<?> cell) throws IOException, InterruptedException {
    final KijiPut put = new KijiPut(entityId, family, qualifier, timestamp, cell);
    mContext.write(NullWritable.get(), new KijiOutput(put));
  }

  /**
   * Increments a counter in a kiji table. This call will always return null (KIJI-147).
   *
   * <p>This method will throw an exception if called on a column that isn't a counter.</p>
   *
   * @param entityId The entity (row) that contains the counter.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param amount The amount to increment the counter (may be negative).
   * @return The new counter value, post increment.
   * @throws IOException If there is an IO error.
   */
  @Override
  public KijiCounter increment(EntityId entityId, String family, String qualifier, long amount)
      throws IOException {
    final KijiIncrement increment = new KijiIncrement(entityId, family, qualifier, amount);
    try {
      mContext.write(NullWritable.get(), new KijiOutput(increment));
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }

    return null;
  }

  /** {@inheritDoc} */
  @Override
  public void setCounter(EntityId entityId, String family, String qualifier, long value)
      throws IOException {
    try {
      put(entityId, family, qualifier, value);
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteRow(EntityId entityId) throws IOException {
    final KijiDelete delete = new KijiDelete(entityId);

    try {
      mContext.write(NullWritable.get(), new KijiOutput(delete));
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteRow(EntityId entityId, long upToTimestamp) throws IOException {
    final KijiDelete delete = new KijiDelete(entityId, upToTimestamp);

    try {
      mContext.write(NullWritable.get(), new KijiOutput(delete));
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(EntityId entityId, String family, long upToTimestamp)
      throws IOException {
    final KijiDelete delete = new KijiDelete(entityId, family, upToTimestamp,
        KijiDeleteScope.MULTIPLE_VERSIONS);

    try {
      mContext.write(NullWritable.get(), new KijiOutput(delete));
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(EntityId entityId, String family, String qualifier, long upToTimestamp)
      throws IOException {
    final KijiDelete delete = new KijiDelete(entityId, family, qualifier, upToTimestamp,
        KijiDeleteScope.MULTIPLE_VERSIONS);

    try {
      mContext.write(NullWritable.get(), new KijiOutput(delete));
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(EntityId entityId, String family, String qualifier, long timestamp)
      throws IOException {
    final KijiDelete delete = new KijiDelete(entityId, family, qualifier, timestamp,
        KijiDeleteScope.SINGLE_VERSION);

    try {
      mContext.write(NullWritable.get(), new KijiOutput(delete));
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
  }
}
