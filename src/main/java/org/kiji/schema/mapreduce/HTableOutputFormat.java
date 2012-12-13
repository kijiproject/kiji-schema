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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;

/**
 * An output format that writes to the HBase table underlying a Kiji table.
 *
 * @param <K> The type of the MapReduce key, which is ignored in this class.
 */
@ApiAudience.Private
public final class HTableOutputFormat<K> extends TableOutputFormat<K> {
  private static final Logger LOG = LoggerFactory.getLogger(HTableOutputFormat.class);

  /** The job configuration. */
  private Configuration mConf;

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    return mConf;
  }

  /**
   * Sets the configuration.
   *
   * <p>This method behaves just like the HBase TableOutputFormat, but it is overridden
   * here so we can create the HTable instance (since the super class's HTable instance it
   * would have created would be private).</p>
   *
   * @param conf The configuration.
   */
  @Override
  public void setConf(Configuration conf) {
    // This method is basically copied from org.apache.hadoop.hbase.mapreduce.TableOutputFormat.
    String address = conf.get(QUORUM_ADDRESS);
    String serverClass = conf.get(REGION_SERVER_CLASS);
    String serverImpl = conf.get(REGION_SERVER_IMPL);
    try {
      if (address != null) {
        ZKUtil.applyClusterKeyToConf(conf, address);
      }
      if (serverClass != null) {
        conf.set(HConstants.REGION_SERVER_CLASS, serverClass);
        conf.set(HConstants.REGION_SERVER_IMPL, serverImpl);
      }
    } catch (IOException e) {
      LOG.error("Error configuring output format.", e);
    }
    mConf = conf;
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
  public RecordWriter<K, Writable> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    final Configuration conf = getConf();
    final String tableName = checkNotNull(conf.get(OUTPUT_TABLE),
        "Missing output HBase table in job configuration");
    // TODO: Inject an HTableInfactory instead of hardcoding an HTable constructor:
    final HTableInterface htable = new HTable(conf, tableName);
    htable.setAutoFlush(false);
    // TODO: TableRecordWriter requires the concrete class HTable even though it does not need it:
    return new TableRecordWriter<K>((HTable) htable);
  }
}
