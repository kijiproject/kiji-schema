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
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.layout.KijiTableLayout;


/**
 * Command-line tool for putting an Avro value into a kiji cell. The value is specified by the
 * user as a JSON string that matches the Avro-JSON-encoding of a piece of Avro data. The user
 * must also specify an Avro schema (as a JSON string) when writing a value with this tool.
 */
@ApiAudience.Private
public final class PutTool extends VersionValidatedTool {
  private static final Logger LOG = LoggerFactory.getLogger(PutTool.class.getName());

  @Flag(name="entity-id", usage="(Unhashed) row entity id")
  private String mEntityId;

  @Flag(name="entity-hash", usage="Already-hashed row entity id")
  private String mEntityHash;

  @Flag(name="column", usage="kiji column name")
  private String mColName = null;

  /** Defaults to -1 to indicate that no timestamp has been specified. */
  @Flag(name="timestamp", usage="Cell timestamp")
  private long mTimestamp = -1;

  @Flag(name="schema", usage="Avro writer schema")
  private String mSchemaStr;
  private Schema mSchema;

  @Flag(name="value", usage="JSON-encoded Avro value")
  private String mJsonValue;

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
  protected int run(List<String> nonFlagArgs) throws Exception {
    Preconditions.checkArgument((mColName != null) && !mColName.isEmpty(),
        "Specify a column with --column=family:qualifier");
    final KijiColumnName column = new KijiColumnName(mColName);
    Preconditions.checkArgument(column.getQualifier() != null,
        "Specify a column qualifier with --column=family:qualifier");

    final KijiTable table = getKiji().openTable(getURI().getTable());
    try {
      final KijiTableLayout tableLayout = table.getLayout();
      final CellSchema cellSchema = tableLayout.getCellSchema(column);

      final EntityId entityId = ToolUtils.createEntityIdFromUserInputs(
          mEntityId, mEntityHash, (RowKeyFormat)tableLayout.getDesc().getKeysFormat());

      final KijiTableWriter writer = table.openTableWriter();
      try {
        if (cellSchema.getType() == SchemaType.COUNTER) {
          if (-1 == mTimestamp) {
            try {
              long value = Long.parseLong(mJsonValue);
              writer.put(entityId, column.getFamily(), column.getQualifier(), value);
            } catch (NumberFormatException nfe) {
              LOG.error("Could not parse value flag to a long: " + nfe.getMessage());
              return 1;
            }
          } else {
            LOG.error("Counters do not support writing to a specific timestamp."
                + "  Remove the \"timestamp\" flag.");
            return 1;
          }
        } else {
          // Get writer schema.
          // Otherwise, set writer Schema in mSchema in preparation to write an Avro record.
          if (null != mSchemaStr) {
            try {
              LOG.debug("Schema is " + mSchemaStr);
              mSchema = new Schema.Parser().parse(mSchemaStr);
            } catch (AvroRuntimeException are) {
              LOG.error("Could not parse writer schema: " + are.toString());
              return 1;
            }
          } else {
            try {
              mSchema = tableLayout.getSchema(column);
            } catch (Exception e) {
              LOG.error(e.getMessage());
              return 1;
            }
          }
          assert null != mSchema;

          // Create the Avro record to write.
          GenericDatumReader<Object> reader =
              new GenericDatumReader<Object>(mSchema);
          Object datum = reader.read(null,
              new DecoderFactory().jsonDecoder(mSchema, mJsonValue));

          // Write the put.
          if (-1 == mTimestamp) {
            writer.put(entityId, column.getFamily(), column.getQualifier(), datum);
          } else {
            writer.put(entityId, column.getFamily(), column.getQualifier(), mTimestamp, datum);
          }
        }

      } finally {
        writer.close();
      }
    } finally {
      table.close();
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
    System.exit(new KijiToolLauncher().run(new PutTool(), args));
  }
}
