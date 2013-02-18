/**
 * (c) Copyright 2013 WibiData, Inc.
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiURI;
/**
 * Command-line tool for inspecting and modifying the Schema table.
 *
 * `--kiji=kiji://hbase-address/instance/` to specify the URI of the target instance.
 *
 * `--get-or-create=path/to/schema/definition` Adds the given schema to the schema table if
 *  it is not already present.  Returns the ID of the schema in all cases.
 *
 * `--get-schema=<long schemaId>` requires `--output=path/to/schema/definition` Gets the schema
 *  definition from the schema table for the given schema ID and writes it to the output file.
 */

@ApiAudience.Private
public final class SchemaTableTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaTableTool.class);

  @Flag(name="kiji", usage="URI of the target instance.")
  private String mURIFlag = null;

  @Flag(name="get-or-create", usage="Path to a file containing a schema defintion "
      + "to get the ID for the given schema or add it to the schema table.")
  private String mCreateFlag = null;

  @Flag(name="get-schema", usage="ID of the schema to retrieve from the schema table.")
  private long mGetFlag = -1L;

  @Flag(name="output", usage="Path to the file to write schema defintions retrieved from the "
      + "schema table.  (will append if the file already exists)")
  private String mOutputFlag = null;

  /** URI of the Kiji instance housing the target schema table. */
  private KijiURI mURI = null;

  /** Kiji instance housing the target schema table. */
  private Kiji mKiji = null;

  /** The Kiji Schema Table on which to operate. */
  private KijiSchemaTable mTable = null;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "schema-table";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Inspect and modify a Kiji schema table.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Metadata";
  }

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    Preconditions.checkNotNull(mURIFlag, "Must specify a target instance with "
        + "'--kiji=kiji://hbase-address/instance-name/'");
    mURI = KijiURI.newBuilder(mURIFlag).build();
    if (mCreateFlag == null || mCreateFlag.isEmpty()) {
      Preconditions.checkArgument(mGetFlag != -1,
          "Specify exactly one of '--get-or-create' and '--get-schema'");
      Preconditions.checkArgument(mOutputFlag != null && !mOutputFlag.isEmpty());
    } else {
      Preconditions.checkArgument(mGetFlag == -1,
          "Specify exactly one of '--get-or-create' and '--get-schema'");
      Preconditions.checkArgument(mOutputFlag == null || mOutputFlag.isEmpty());
    }
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    mKiji = Kiji.Factory.open(mURI);
    try {
      mTable = mKiji.getSchemaTable();
      if (mCreateFlag != null && !mCreateFlag.isEmpty()) {
        File file = new File(mCreateFlag);
        Schema schema = new Schema.Parser().parse(file);
        long id = mTable.getOrCreateSchemaId(schema);
        if (isInteractive()) {
          getPrintStream().println("Schema table schema ID for the given schema is: \n");
        }
        getPrintStream().println(id);
        return SUCCESS;
      } else {
        Schema schema = mTable.getSchema(mGetFlag);
        Preconditions.checkArgument(schema != null, "No schema definition with ID: %s", mGetFlag);
        try {
          File file = new File(mOutputFlag);
          boolean fileCreated = file.createNewFile();
          if (fileCreated) {
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            try {
              bw.write(schema.toString());
            } finally {
              bw.close();
            }
          } else {
            if (mayProceed("File: %s already exists, do you wish to overwrite the file?",
                mOutputFlag)) {
              FileWriter fw = new FileWriter(file.getAbsoluteFile());
              BufferedWriter bw = new BufferedWriter(fw);
              try {
                bw.write(schema.toString());
              } finally {
                bw.close();
              }
            } else {
              return ABORTED;
            }
          }
          if (isInteractive()) {
            getPrintStream().println(
                String.format("Schema definition written to: %s", mOutputFlag));
          }
          return SUCCESS;
        } catch (IOException ioe) {
          LOG.error(String.format("Error writing to file: %s", mOutputFlag));
          throw ioe;
        }
      }
    } finally {
      mKiji.release();
    }
  }
}
