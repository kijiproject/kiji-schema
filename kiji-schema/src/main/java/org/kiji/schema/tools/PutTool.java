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

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.avro.CellSchema;
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

  @Flag(name="table", usage="kiji table name")
  private String mTableName = "";

  @Flag(name="entity-id", usage="(Unhashed) row entity id")
  private String mEntityId;

  @Flag(name="entity-hash", usage="Already-hashed row entity id")
  private String mEntityHash;

  @Flag(name="column", usage="kiji column name")
  private String mColName = "";

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

    final EntityId entityId = ToolUtils.createEntityIdFromUserInputs(
        mEntityId, mEntityHash, tableLayout.getDesc().getKeysFormat());

    // Closeables.
    KijiTable table = null;
    KijiTableWriter writer = null;
    try {
      table = getKiji().openTable(mTableName);
      writer = table.openTableWriter();

      if (cellSchema.getType() == SchemaType.COUNTER) {
        if (-1 == mTimestamp) {
          try {
            long value = Long.parseLong(mJsonValue);
            writer.setCounter(entityId, column.getFamily(), column.getQualifier(), value);
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

        KijiCell<Object> kijiCell = new KijiCell<Object>(mSchema, datum);

        // Write the put.
        if (-1 == mTimestamp) {
          writer.put(entityId, column.getFamily(), column.getQualifier(), kijiCell);
        } else {
          writer.put(entityId, column.getFamily(), column.getQualifier(), mTimestamp, kijiCell);
        }
      }
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
    System.exit(new KijiToolLauncher().run(new PutTool(), args));
  }
}
