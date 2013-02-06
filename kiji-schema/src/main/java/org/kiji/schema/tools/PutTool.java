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
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
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
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Command-line tool for putting an Avro value into a kiji cell. The value is specified by the
 * user as a JSON string that matches the Avro-JSON-encoding of a piece of Avro data. The user
 * must also specify an Avro schema (as a JSON string) when writing a value with this tool.
 */
@ApiAudience.Private
public final class PutTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(PutTool.class.getName());

  @Flag(name="kiji", usage="KijiURI of the target column")
  private String mColumnURIString;

  @Flag(name="entity-id", usage="(Unhashed) row entity id")
  private String mEntityId;

  @Flag(name="entity-hash", usage="Already-hashed row entity id")
  private String mEntityHash;

  /** Defaults to -1 to indicate that no timestamp has been specified. */
  @Flag(name="timestamp", usage="Cell timestamp")
  private long mTimestamp = -1;

  @Flag(name="schema", usage="Avro writer schema")
  private String mSchemaStr;
  private Schema mSchema;

  @Flag(name="value", usage="JSON-encoded Avro value")
  private String mJsonValue;

  /** Opened Kiji instance used by the tool. */
  private Kiji mKiji;
  /** KijiURI of the column into which to put the value. */
  private KijiURI mURI;

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
    setURI(parseURI(mColumnURIString));
    getConf().setInt(HConstants.ZOOKEEPER_CLIENT_PORT, mURI.getZookeeperClientPort());
    getConf().set(HConstants.ZOOKEEPER_QUORUM,
        Joiner.on(",").join(mURI.getZookeeperQuorumOrdered()));
    setConf(HBaseConfiguration.addHbaseResources(getConf()));
  }
  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    final KijiTableLayout tableLayout =
        getKiji().getMetaTable().getTableLayout(getURI().getTable());
    if (null == tableLayout) {
      LOG.error("No such table: " + getURI().getTable());
      return 1;
    }

    if (getURI().getColumns().size() != 1) {
      LOG.error("specify exaclty one column");
      return 1;
    }

    final KijiColumnName column = getURI().getColumns().get(0);
    if (null == column.getQualifier()) {
      LOG.error("Column name must be in the format 'family:qualifier'.");
      return 1;
    }

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
