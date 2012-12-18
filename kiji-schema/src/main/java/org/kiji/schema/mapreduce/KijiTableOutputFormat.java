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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiManagedHBaseTableName;
import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.layout.ColumnNameTranslator;

/**
 * Used to write {@link KijiMutation}s to Kiji tables. Use the
 * {@link KijiTableOutputFormat#setOptions(Job, String, String)}
 * method to configure this output format for use in a mapreduce job.
 *
 * To setup a job to use KijiTableOutputFormat:
 * <code>
 *   <pre>
 *   final Job job = new Job(conf);
 *
 *   KijiTableOutputFormat.setOptions(job.getConfiguration(), instance, table);
 *
 *   // This is optional, the key class can be set to anything.
 *   job.setOutputKeyClass(NullWritable.class);
 *   // The output class must KijiOutput for KijiTableOutputFormat to function properly.
 *   job.setOutputValueClass(KijiOutput.class);
 *   job.setOutputFormatClass(KijiTableOutputFormat.class);
 *   </pre>
 * </code>
 *
 * @param <K> The key is ignored in this OutputFormat.
 */
@ApiAudience.Public
public final class KijiTableOutputFormat<K> extends OutputFormat<K, KijiOutput>
    implements Configurable {
  /** Configuration variable for the input kiji instance. */
  public static final String INSTANCE_CONF_NAME = "kiji.output.instance";

  /** Configuration variable for the input kiji table. */
  public static final String TABLE_CONF_NAME = "kiji.output.table";

  private final HTableOutputFormat<K> mDelegate = new HTableOutputFormat<K>();

  /**
   * A {@link org.apache.hadoop.mapreduce.RecordWriter} for writing KijiMutations to a KijiTable.
   */
  protected static class KijiTableRecordWriter<K> extends RecordWriter<K, KijiOutput> {
    private final RecordWriter<K, Writable> mDelegate;
    private final ColumnNameTranslator mTranslator;
    private final KijiCellEncoder mEncoder;
    private final Kiji mKiji;
    private final HBaseKijiTable mTable;

    /**
     * Creates a new RecordWriter for this output format. This RecordWriter will perform the actual
     * writes to Kiji.
     *
     * @param delegate The underlying TableRecordWriter to delegate to.
     * @param conf The configuration object to use.
     * @throws IOException If there is an error opening a connection to Kiji.
     */
    public KijiTableRecordWriter(RecordWriter<K, Writable> delegate, Configuration conf)
        throws IOException {
      final String instance = checkNotNull(conf.get(INSTANCE_CONF_NAME),
          "Missing output Kiji instance in job configuration.");
      final String table = checkNotNull(conf.get(TABLE_CONF_NAME),
          "Missing output Kiji table in job configuration.");
      mKiji = Kiji.open(new KijiConfiguration(conf, instance));
      mTable = HBaseKijiTable.downcast(mKiji.openTable(table));

      mDelegate = delegate;
      mTranslator = new ColumnNameTranslator(HBaseKijiTable.downcast(mTable).getLayout());
      mEncoder = new KijiCellEncoder(mKiji.getSchemaTable());
    }

    /** {@inheritDoc} */
    @Override
    public void write(K key, KijiOutput value) throws IOException, InterruptedException {
      final KijiMutation op = value.getOperation();

      if (op instanceof KijiIncrement) {
        // Write increments directly using an HTable.
        final KijiIncrement increment = (KijiIncrement) op;
        mTable.getHTable().increment(increment.toIncrement(mTranslator));
      } else if (op instanceof KijiPut) {
        // Pass puts off to the underlying TableRecordWriter.
        final KijiPut put = (KijiPut) op;
        mDelegate.write(key, put.toPut(mTranslator, mEncoder));
      } else if (op instanceof KijiDelete) {
        // Pass deletes off to the underlying TableRecordWriter.
        final KijiDelete del = (KijiDelete) op;
        mDelegate.write(key, del.toDelete(mTranslator));
      } else {
        throw new IOException("Found an unrecognized KijiOperation: " + op.toString());
      }
    }

    /** {@inheritDoc} */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      IOUtils.closeQuietly(mKiji);
      IOUtils.closeQuietly(mTable);
      mDelegate.close(context);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    return mDelegate.getConf();
  }

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    mDelegate.setConf(conf);
  }

  /**
   * Gets the KijiTableRecordWriter for writing output records from a task to the table.
   *
   * @param context The task context.
   * @return The record writer.
   * @throws IOException If there is an error.
   * @throws InterruptedException If the thread is interrupted.
   */
  @Override
  public RecordWriter<K, KijiOutput> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    final RecordWriter<K, Writable> delegate = mDelegate.getRecordWriter(context);

    return new KijiTableRecordWriter<K>(delegate, context.getConfiguration());
  }

  /** {@inheritDoc} */
  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    mDelegate.checkOutputSpecs(context);
  }

  /** {@inheritDoc} */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return mDelegate.getOutputCommitter(context);
  }

  /**
   * Serializes the desired Kiji table to the specified job's configuration. This method will
   * assume you are using the default Kiji instance.
   *
   * @param job Configuration object to configure.
   * @param table Name of the Kiji table to write to.
   */
  public static void setOptions(Job job, String table) {
    setOptions(job, KijiConfiguration.DEFAULT_INSTANCE_NAME, table);
  }

  /**
   * Serializes the desired Kiji instance and table to the specified job's configuration.
   *
   * @param job Configuration object to configure.
   * @param instance Name of the Kiji instance to write to.
   * @param table Name of the Kiji table to write to.
   */
  public static void setOptions(Job job, String instance, String table) {
    final Configuration conf = job.getConfiguration();
    final String htable = KijiManagedHBaseTableName
        .getKijiTableName(instance, table)
        .toString();

    conf.set(TableOutputFormat.OUTPUT_TABLE, htable);
    conf.set(INSTANCE_CONF_NAME, instance);
    conf.set(TABLE_CONF_NAME, table);
  }
}
