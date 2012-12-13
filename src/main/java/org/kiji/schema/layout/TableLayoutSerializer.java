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

package org.kiji.schema.layout;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.avro.TableLayoutDesc;

/**
 * Serialize TableLayout instances into job configurations.
 */
@ApiAudience.Framework
public final class TableLayoutSerializer {
  /** The configuration variable that stores the avro json-encoded input table layout. */
  public static final String CONF_INPUT_TABLE_LAYOUT = "kiji.input.table.layout";

  /** The configuration variable that stores output table layout. */
  public static final String CONF_OUTPUT_TABLE_LAYOUT = "kiji.output.table.layout";

  /** No constructor since this is a utility class. */
  private TableLayoutSerializer() {}


  /**
   * Serializes the table layout into a configuration object.
   *
   * @param tableLayout The table layout to serialize.
   * @param confKey The configuration key for the table layout.
   * @param conf The configuration object to store the layout into.
   * @throws IOException If there is an error.
   */
  private static void serializeTableLayout(KijiTableLayout tableLayout, String confKey,
      Configuration conf) throws IOException {
    final ByteArrayOutputStream jsonOutputStream = new ByteArrayOutputStream();
    final JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(TableLayoutDesc.SCHEMA$,
        jsonOutputStream);
    final SpecificDatumWriter<TableLayoutDesc> writer = new SpecificDatumWriter<TableLayoutDesc>(
        TableLayoutDesc.class);
    writer.write(tableLayout.getDesc(), jsonEncoder);
    jsonEncoder.flush();
    conf.set(confKey, jsonOutputStream.toString("UTF-8"));
  }

  /**
   * Reads a table layout from the configuration.
   *
   * @param confKey The configuration key for the table layout.
   * @param conf The configuration storing the table layout.
   * @return the TableLayout.
   * @throws IOException If there is an error.
   */
  public static KijiTableLayout readTableLayout(String confKey, Configuration conf)
      throws IOException {
    String jsonTableLayout = conf.get(confKey);
    if (null == jsonTableLayout) {
      throw new RuntimeException("Job configuration did not include " + confKey);
    }
    final SpecificDatumReader<TableLayoutDesc> reader =
        new SpecificDatumReader<TableLayoutDesc>(TableLayoutDesc.class);
    final Decoder jsonDecoder =
        DecoderFactory.get().jsonDecoder(TableLayoutDesc.SCHEMA$, jsonTableLayout);
    return new KijiTableLayout(reader.read(null, jsonDecoder), null);
  }

  /**
   * Serializes the table layout into a configuration object for input.
   *
   * @param tableLayout The table layout to serialize.
   * @param conf The configuration object to store the layout into.
   * @throws IOException If there is an error.
   */
  public static void serializeInputTableLayout(KijiTableLayout tableLayout, Configuration conf)
      throws IOException {
    serializeTableLayout(tableLayout, CONF_INPUT_TABLE_LAYOUT, conf);
  }

  /**
   * Serializes the table layout into a configuration object for output.
   *
   * @param tableLayout The table layout to serialize.
   * @param conf The configuration object to store the layout into.
   * @throws IOException If there is an error.
   */
  public static void serializeOutputTableLayout(KijiTableLayout tableLayout, Configuration conf)
      throws IOException {
    serializeTableLayout(tableLayout, CONF_OUTPUT_TABLE_LAYOUT, conf);
  }

  /**
   * Reads an input table layout from the configuration.
   *
   * @param conf The configuration storing the table layout.
   * @return the TableLayout for the table being mapped over.
   * @throws IOException If there is an error.
   */
  public static KijiTableLayout readInputTableLayout(Configuration conf) throws IOException {
    return readTableLayout(CONF_INPUT_TABLE_LAYOUT, conf);
  }

  /**
   * Reads an output table layout from the configuration.
   *
   * @param conf The configuration storing the table layout.
   * @return the TableLayout for the table being mapped over.
   * @throws IOException If there is an error.
   */
  public static KijiTableLayout readOutputTableLayout(Configuration conf) throws IOException {
    return readTableLayout(CONF_OUTPUT_TABLE_LAYOUT, conf);
  }
}
