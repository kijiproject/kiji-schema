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

import com.google.common.base.Preconditions;
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
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.util.ResourceUtils;

/**
 * Command-line tool to increment a counter in a cell of a kiji table.
 */
@ApiAudience.Private
public final class IncrementTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(IncrementTool.class);

  @Flag(name="cell", usage="URI of the cell(s) (qualified column) to increment, "
      + "such as --cell=kiji://hbase-address/kiji-instance/table/family:qualifier.")
  private String mCellURIFlag;

  @Flag(name="entity-id", usage="Row entity ID specification.")
  private String mEntityId;

  @Flag(name="value", usage="Integer amount to add to the counter(s). May be negative.")
  private int mValue = 1;

  /** URI specifying qualified columns with counters to increment. */
  private KijiURI mCellURI;

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
    Preconditions.checkArgument(null != mCellURIFlag, "Specify a cell address with "
        + "--cell=kiji://hbase-address/kiji-instance/table/family:qualifier");
    mCellURI = KijiURI.newBuilder(mCellURIFlag).build();

    Preconditions.checkArgument(null != mEntityId, "Specify an entity ID with --entity-id=...");
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    final Kiji kiji = Kiji.Factory.open(mCellURI, getConf());
    try {
      final KijiTable table = kiji.openTable(mCellURI.getTable());
      try {
        final KijiTableWriter writer = table.openTableWriter();
        try {
          for (KijiColumnName column : mCellURI.getColumns()) {
            try {
              final CellSchema cellSchema = table.getLayout().getCellSchema(column);
              if (cellSchema.getType() != SchemaType.COUNTER) {
                LOG.error("Can't increment non counter-type column '{}'", column);
                return FAILURE;
              }
              final EntityId entityId =
                  ToolUtils.createEntityIdFromUserInputs(mEntityId, table.getLayout());
              writer.increment(entityId, column.getFamily(), column.getQualifier(), mValue);

            } catch (IOException ioe) {
              LOG.error("Error while incrementing column '{}'", column);
              return FAILURE;
            }
          }
          return SUCCESS;

        } finally {
          ResourceUtils.closeOrLog(writer);
        }
      } finally {
        ResourceUtils.releaseOrLog(table);
      }
    } finally {
      ResourceUtils.releaseOrLog(kiji);
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
