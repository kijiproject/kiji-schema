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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.KijiManagedHBaseTableName;
import org.kiji.schema.KijiURI;

/**
 * Command-line tool for flushing kiji meta and user tables in hbase.
 */
@ApiAudience.Private
public final class FlushTableTool extends VersionValidatedTool {
  private static final Logger LOG = LoggerFactory.getLogger(FlushTableTool.class.getName());

  @Flag(name="table", usage="The name of a kiji user table to flush.")
  private String mTableName = null;

  @Flag(name="meta", usage="If true, flushes all kiji meta tables.")
  private boolean mFlushMeta = false;

  private HBaseAdmin mHBaseAdmin;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "flush-table";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Flush kiji user and meta table write-ahead logs.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Admin";
  }

  /** {@inheritDoc} */
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
   * @param instanceName The name of the Kiji instance.
   * @throws IOException If there is an error.
   * @throws InterruptedException If the thread is interrupted.
   */
  private void flushMetaTables(HBaseAdmin hbaseAdmin, String instanceName)
      throws IOException, InterruptedException {
    LOG.debug("Flushing schema hash table");
    KijiManagedHBaseTableName hbaseTableName = KijiManagedHBaseTableName.getSchemaHashTableName(
        instanceName);
    hbaseAdmin.flush(hbaseTableName.toString());

    LOG.debug("Flushing schema id table");
    hbaseTableName = KijiManagedHBaseTableName.getSchemaIdTableName(instanceName);
    hbaseAdmin.flush(hbaseTableName.toString());

    LOG.debug("Flushing meta table");
    hbaseTableName = KijiManagedHBaseTableName.getMetaTableName(
        instanceName);
    hbaseAdmin.flush(hbaseTableName.toString());

    LOG.debug("Flushing system table");
    hbaseTableName = KijiManagedHBaseTableName.getSystemTableName(instanceName);
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
   * @param instanceName The name of the kiji instance to flush.
   * @param tableName The name of the kiji table to flush.
   * @throws IOException If there is an error.
   * @throws InterruptedException If the thread is interrupted.
   */
  private void flushTable(HBaseAdmin hbaseAdmin, String instanceName, String tableName)
      throws IOException, InterruptedException {
    KijiManagedHBaseTableName hbaseTableName = KijiManagedHBaseTableName.getKijiTableName(
        instanceName, tableName);
    hbaseAdmin.flush(hbaseTableName.toString());
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() throws Exception {
    super.setup();
    mHBaseAdmin = new HBaseAdmin(getConf());
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup() throws IOException {
    IOUtils.closeQuietly(mHBaseAdmin);
    super.cleanup();
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    if (mFlushMeta) {
      getPrintStream().println("Flushing metadata tables for kiji instance: "
          + getURI().toString());
      flushMetaTables(mHBaseAdmin, getKiji().getURI().getInstance());
    }

    if (null != mTableName) {
      setURI(KijiURI.newBuilder(getURI()).withTableName(mTableName).build());
      getPrintStream().println("Flushing table: " + getURI().toString());
      flushTable(mHBaseAdmin, getKiji().getURI().getInstance(), mTableName);
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
    System.exit(new KijiToolLauncher().run(new FlushTableTool(), args));
  }
}
