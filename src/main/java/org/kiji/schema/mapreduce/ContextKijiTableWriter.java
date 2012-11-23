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

import org.kiji.schema.KijiCounter;
import org.kiji.schema.impl.BaseKijiTableWriter;

/**
 * ContextKijiTableWriter is a {@link org.kiji.schema.KijiTableWriter} used to perform mutations
 * to a Kiji table from within a Hadoop mapper or reducer.
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
public class ContextKijiTableWriter extends BaseKijiTableWriter {
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
  public void put(KijiPut put)
      throws IOException {
    try {
      mContext.write(NullWritable.get(), new KijiOutput(put));
    } catch (InterruptedException ex) {
      // It's unclear whether or not InterruptedExceptions get thrown at all ever by hadoop.
      throw new IOException(ex);
    }
  }

  /** {@inheritDoc} */
  @Override
  public KijiCounter increment(KijiIncrement increment)
      throws IOException {
    try {
      mContext.write(NullWritable.get(), new KijiOutput(increment));
    } catch (InterruptedException ex) {
      // It's unclear whether or not InterruptedExceptions get thrown at all ever by hadoop.
      throw new IOException(ex);
    }

    return null;
  }

  /** {@inheritDoc} */
  @Override
  public void delete(KijiDelete delete)
      throws IOException {
    try {
      mContext.write(NullWritable.get(), new KijiOutput(delete));
    } catch (InterruptedException ex) {
      // It's unclear whether or not InterruptedExceptions get thrown at all ever by hadoop.
      throw new IOException(ex);
    }
  }
}
