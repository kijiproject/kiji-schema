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
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
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
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.ResourceUtils;

/**
 * Command-line tool for putting an Avro value into a kiji cell. The value is specified by the
 * user as a JSON string that matches the Avro-JSON-encoding of a piece of Avro data. The user
 * must also specify an Avro schema (as a JSON string) when writing a value with this tool.
 */
@ApiAudience.Private
public final class PutTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(PutTool.class.getName());

  @Flag(name="target", usage="Kiji URI of the target column.")
  private String mColumnURIFlag;

  @Flag(name="entity-id", usage="Row entity ID specification.")
  private String mEntityId;

  /** Defaults to -1 to indicate that no timestamp has been specified. */
  @Flag(name="timestamp", usage="Cell timestamp")
  private long mTimestamp = -1;

  @Flag(name="schema", usage="Avro writer schema")
  private String mSchemaFlag;

  /** Avro schema parsed from the command-line flag. */
  private Schema mSchema;

  @Flag(name="value", usage="JSON-encoded Avro value")
  private String mJsonValue;

  /** URI of the column into which to put the value. */
  private KijiURI mColumnURI = null;

  /** Kiji instance where the target table lives. */
  private Kiji mKiji = null;

  /** Kiji table to write to. */
  private KijiTable mTable = null;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "put";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Write a cell to a column in a kiji table.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Data";
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() throws IOException {
    Preconditions.checkArgument((mColumnURIFlag != null) && !mColumnURIFlag.isEmpty(),
        "Specify a target table to write synthesized data to with "
        + "--table=kiji://hbase-address/kiji-instance/table");
    mColumnURI = KijiURI.newBuilder(mColumnURIFlag).build();
    Preconditions.checkArgument(mColumnURI.getTable() != null,
        "No table specified in target URI '{}'. "
        + "Specify a target table to write synthesized data to with "
        + "--table=kiji://hbase-address/kiji-instance/table",
        mColumnURI);
    Preconditions.checkArgument(mColumnURI.getColumns().size() == 1,
        "Invalid target column '{}', specify exactly one column in URI with "
        + "--target=kiji://hbase-address/kiji-instance/table/family:qualifier",
        mColumnURI);
    Preconditions.checkArgument(mColumnURI.getColumns().get(0).isFullyQualified(),
        "Missing column qualifier in '{}', specify exactly one column in URI with "
        + "--target=kiji://hbase-address/kiji-instance/table/family:qualifier",
        mColumnURI);

    mKiji = Kiji.Factory.open(mColumnURI, getConf());
    mTable = mKiji.openTable(mColumnURI.getTable());
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    final KijiColumnName column = mColumnURI.getColumns().get(0);
    final KijiTableLayout layout = mTable.getLayout();
    final CellSchema cellSchema = layout.getCellSchema(column);

    final EntityId entityId =
        ToolUtils.createEntityIdFromUserInputs(mEntityId, layout);

    final KijiTableWriter writer = mTable.openTableWriter();
    try {
      if (cellSchema.getType() == SchemaType.COUNTER) {
        if (-1 == mTimestamp) {
          try {
            long value = Long.parseLong(mJsonValue);
            writer.put(entityId, column.getFamily(), column.getQualifier(), value);
          } catch (NumberFormatException nfe) {
            LOG.error("Could not parse value flag to a long: " + nfe.getMessage());
            return FAILURE;
          }
        } else {
          LOG.error("Counters do not support writing to a specific timestamp."
              + "  Remove the \"timestamp\" flag.");
          return FAILURE;
        }
      } else {
        // Get writer schema.
        // Otherwise, set writer Schema in mSchema in preparation to write an Avro record.
        if (null != mSchemaFlag) {
          try {
            LOG.debug("Schema is " + mSchemaFlag);
            mSchema = new Schema.Parser().parse(mSchemaFlag);
          } catch (AvroRuntimeException are) {
            LOG.error("Could not parse writer schema: " + are.toString());
            return FAILURE;
          }
        } else {
          try {
            mSchema = layout.getSchema(column);
          } catch (Exception e) {
            LOG.error(e.getMessage());
            return FAILURE;
          }
        }
        Preconditions.checkNotNull(mSchema);

        // Create the Avro record to write.
        GenericDatumReader<Object> reader = new GenericDatumReader<Object>(mSchema);
        Object datum = reader.read(null, new DecoderFactory().jsonDecoder(mSchema, mJsonValue));

        // Write the put.
        if (-1 == mTimestamp) {
          writer.put(entityId, column.getFamily(), column.getQualifier(), datum);
        } else {
          writer.put(entityId, column.getFamily(), column.getQualifier(), mTimestamp, datum);
        }
      }

    } finally {
      ResourceUtils.closeOrLog(writer);
    }

    return SUCCESS;
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup() {
    ResourceUtils.releaseOrLog(mTable);
    ResourceUtils.releaseOrLog(mKiji);
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new PutTool(), args));
  }
}
