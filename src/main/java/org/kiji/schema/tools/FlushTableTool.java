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

package org.kiji.schema.tools;

import java.io.IOException;
import java.util.List;

import com.odiago.common.flags.Flag;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiManagedHBaseTableName;

/**
 * Command-line tool for flushing kiji meta and user tables in hbase.
 */
public class FlushTableTool extends VersionValidatedTool {
  private static final Logger LOG = LoggerFactory.getLogger(FlushTableTool.class.getName());

  @Flag(name="table", usage="The name of a kiji user table to flush.")
  private String mTableName = null;

  @Flag(name="meta", usage="If true, flushes all kiji meta tables.")
  private boolean mFlushMeta = false;

  private HBaseAdmin mHBaseAdmin;

  @Override
  protected void validateFlags() throws Exception {
    super.validateFlags();
    if (mTableName == null && !mFlushMeta) {
      throw new Exception("Requires at least one of --table or --meta");
    }

    if (mTableName != null && mTableName.isEmpty()) {
      throw new Exception("Invalid table name (empty string)");
    }
  }

  /**
   * Flushes all metadata tables.
   *
   * @param hbaseAdmin An hbase admin utility.
   * @param kijiConfiguration The kiji configuration.
   * @throws IOException If there is an error.
   * @throws InterruptedException If the thread is interrupted.
   */
  private void flushMetaTables(HBaseAdmin hbaseAdmin, KijiConfiguration kijiConfiguration)
      throws IOException, InterruptedException {
    LOG.debug("Flushing schema hash table");
    KijiManagedHBaseTableName hbaseTableName = KijiManagedHBaseTableName.getSchemaHashTableName(
        kijiConfiguration.getName());
    hbaseAdmin.flush(hbaseTableName.toString());

    LOG.debug("Flushing schema id table");
    hbaseTableName = KijiManagedHBaseTableName.getSchemaIdTableName(kijiConfiguration.getName());
    hbaseAdmin.flush(hbaseTableName.toString());

    LOG.debug("Flushing meta table");
    hbaseTableName = KijiManagedHBaseTableName.getMetaTableName(
        kijiConfiguration.getName());
    hbaseAdmin.flush(hbaseTableName.toString());

    LOG.debug("Flushing system table");
    hbaseTableName = KijiManagedHBaseTableName.getSystemTableName(kijiConfiguration.getName());
    hbaseAdmin.flush(hbaseTableName.toString());

    LOG.debug("Flushing -ROOT-");
    hbaseAdmin.flush("-ROOT-");

    LOG.debug("Flushing .META.");
    hbaseAdmin.flush(".META.");
  }

  /**
   * Flushes a kiji table with the name 'tableName'.
   *
   * @param hbaseAdmin An hbase admin utility.
   * @param kijiConfiguration The kiji configuration.
   * @param tableName The name of the kiji table to flush.
   * @throws IOException If there is an error.
   * @throws InterruptedException If the thread is interrupted.
   */
  private void flushTable(HBaseAdmin hbaseAdmin, KijiConfiguration kijiConfiguration,
      String tableName) throws IOException, InterruptedException {
    KijiManagedHBaseTableName hbaseTableName = KijiManagedHBaseTableName.getKijiTableName(
        kijiConfiguration.getName(), tableName);
    hbaseAdmin.flush(hbaseTableName.toString());
  }

  @Override
  protected void setup() throws Exception {
    super.setup();
    mHBaseAdmin = new HBaseAdmin(getConf());
  }

  @Override
  protected void cleanup() throws IOException {
    IOUtils.closeQuietly(mHBaseAdmin);
    super.cleanup();
  }

  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    if (mFlushMeta) {
      getPrintStream().println("Flushing metadata tables for kiji instance: "
          + getURI().toString());
      flushMetaTables(mHBaseAdmin, getKiji().getKijiConf());
    }

    if (null != mTableName) {
      setURI(getURI().setTableName(mTableName));
      getPrintStream().println("Flushing table: " + getURI().toString());
      flushTable(mHBaseAdmin, getKiji().getKijiConf(), mTableName);
    }

    getPrintStream().println("Flush operations successfully enqueued.");

    return 0;
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new FlushTableTool(), args));
  }
}
