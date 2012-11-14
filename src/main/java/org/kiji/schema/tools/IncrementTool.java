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
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Command-line tool to increment a counter in a cell of a kiji table.
 */
public class IncrementTool extends VersionValidatedTool {
  private static final Logger LOG = LoggerFactory.getLogger(IncrementTool.class);

  @Flag(name="table", usage="kiji table name")
  private String mTableName = "";

  @Flag(name="entity-id", usage="(Unhashed) row entity id")
  private String mEntityId;

  @Flag(name="entity-hash", usage="Already-hashed row entity id")
  private String mEntityHash;

  @Flag(name="column", usage="kiji column name")
  private String mColName = "";

  @Flag(name="value", usage="Integer value to add to the counter.")
  private int mValue = 1;

  @Override
  protected void validateFlags() throws Exception {
    if (mTableName.isEmpty()) {
      throw new RuntimeException("Specify a table on the 'table' flag");
    }
  }

  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    final KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout(mTableName);
    if (null == tableLayout) {
      LOG.error("No such table: " + mTableName);
      return 1;
    }

    final KijiColumnName column = new KijiColumnName(mColName);
    if (null == column.getQualifier()) {
      LOG.error("Column name must be in the format 'family:qualifier'.");
      return 1;
    }
    final CellSchema cellSchema = tableLayout.getCellSchema(column);
    if (cellSchema.getType() != SchemaType.COUNTER) {
      LOG.error("Can't increment non counter-type column: " + column);
      return 1;
    }

    final EntityId entityId = ToolUtils.createEntityIdFromUserInputs(
        mEntityId, mEntityHash, tableLayout.getDesc().getKeysFormat());

    // Closeables.
    KijiTable table = null;
    KijiTableWriter writer = null;
    try {
      table = getKiji().openTable(mTableName);
      writer = table.openTableWriter();
      writer.increment(entityId, column.getFamily(), column.getQualifier(), mValue);
    } catch (IOException ioe) {
      LOG.error("Error while incrementing column.");
      return 1;
    } finally {
      IOUtils.closeQuietly(writer);
      IOUtils.closeQuietly(table);
    }
    return 0;
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new IncrementTool(), args));
  }
}
