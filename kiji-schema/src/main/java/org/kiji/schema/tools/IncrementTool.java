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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.ResourceUtils;

/**
 * Command-line tool to increment a counter in a cell of a kiji table.
 */
@ApiAudience.Private
public final class IncrementTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(IncrementTool.class);

  @Flag(name="column", usage="KijiURI of the column(s) to increment in.")
  private String mColumnsURIString;

  @Flag(name="entity-id", usage="(Unhashed) row entity id")
  private String mEntityId;

  @Flag(name="entity-hash", usage="Already-hashed row entity id")
  private String mEntityHash;

  @Flag(name="value", usage="Integer value to add to the counter(s).")
  private int mValue = 1;

  /** Opened Kiji to use. */
  private Kiji mKiji;
  /** KijiURI of the target column(s). */
  private KijiURI mURI;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "increment";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Increment a counter column in a kiji table.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Data";
  }

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    Preconditions.checkArgument(null != mEntityId || null != mEntityHash,
        "must specify an entity");
    Preconditions.checkArgument(null != mColumnsURIString, "must specify a column or columns");
  }

  /**
   * Opens a kiji instance.
   *
   * @return The opened kiji.
   * @throws IOException if there is an error.
   */
  private Kiji openKiji() throws IOException {
    return Kiji.Factory.open(getURI(), getConf());
  }

  /**
   * Retrieves the kiji instance used by this tool. On the first call to this method,
   * the kiji instance will be opened and will remain open until {@link #cleanup()} is called.
   *
   * @return The kiji instance.
   * @throws IOException if there is an error loading the kiji.
   */
  protected synchronized Kiji getKiji() throws IOException {
    if (null == mKiji) {
      mKiji = openKiji();
    }
    return mKiji;
  }

  /**
   * Returns the kiji URI of the target this tool operates on.
   *
   * @return The kiji URI of the target this tool operates on.
   */
  protected KijiURI getURI() {
    if (null == mURI) {
      getPrintStream().println("No URI specified.");
    }
    return mURI;
  }

  /**
   * Sets the kiji URI of the target this tool operates on.
   *
   * @param uri The kiji URI of the target this tool should operate on.
   */
  protected void setURI(KijiURI uri) {
    if (null == mURI) {
      mURI = uri;
    } else {
      getPrintStream().printf("URI is already set to: %s", mURI.toString());
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() {
    setURI(parseURI(mColumnsURIString));
    getConf().setInt(HConstants.ZOOKEEPER_CLIENT_PORT, mURI.getZookeeperClientPort());
    getConf().set(HConstants.ZOOKEEPER_QUORUM,
        Joiner.on(",").join(mURI.getZookeeperQuorumOrdered()));
    setConf(HBaseConfiguration.addHbaseResources(getConf()));
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup() {
    ResourceUtils.releaseOrLog(mKiji);
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    final KijiTableLayout tableLayout =
        getKiji().getMetaTable().getTableLayout(getURI().getTable());
    if (null == tableLayout) {
      LOG.error("No such table: {}", getURI());
      return 1;
    }

    // TODO Fix CLI with formatted row key format (https://jira.kiji.org/browse/SCHEMA-171)
    if (tableLayout.getDesc().getKeysFormat() instanceof RowKeyFormat2) {
      throw new RuntimeException("CLI does not support Formatted Row Key format as yet");
    }

    final KijiTable table = getKiji().openTable(getURI().getTable());
    final KijiTableWriter writer = table.openTableWriter();
    for (KijiColumnName column : getURI().getColumns()) {
      try {
        final CellSchema cellSchema = tableLayout.getCellSchema(column);
        if (cellSchema.getType() != SchemaType.COUNTER) {
          LOG.error("Can't increment non counter-type column: " + column);
        return 1;
        }
        final EntityId entityId = ToolUtils.createEntityIdFromUserInputs(
            mEntityId, mEntityHash, tableLayout.getDesc().getKeysFormat());
        writer.increment(entityId, column.getFamily(), column.getQualifier(), mValue);
      } catch (IOException ioe) {
      LOG.error("Error while incrementing column: " + column);
      return 1;
      } finally {
        ResourceUtils.closeOrLog(writer);
        ResourceUtils.closeOrLog(table);
      }
    }
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new IncrementTool(), args));
  }
}
