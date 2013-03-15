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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.KConstants;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiURI;
/**
 * Command-line tool for inspecting and modifying the Schema table.
 *
 * --kiji=kiji://hbase-address/instance/ to specify the URI of the target instance.
 *
 * --register=path/to/schema/definition Adds the given schema to the schema table if
 *  it is not already present.  Returns the ID of the schema in all cases.
 *
 * --get-schema=<long schemaId> requires --output=path/to/schema/definition Gets the schema
 *  definition from the schema table for the given schema ID and writes it to the output file.
 */

@ApiAudience.Private
public final class SchemaTableTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaTableTool.class);

  @Flag(name="kiji", usage="URI of the target instance.", required=true)
  private String mURIFlag = KConstants.DEFAULT_INSTANCE_URI;

  @Flag(name="register", usage="Path to a file containing a schema defintion "
      + "to get the ID for the given schema or add it to the schema table.")
  private String mRegisterFlag = null;

  @Flag(name="get-schema", usage="ID of the schema to retrieve from the schema table.")
  private Long mGetFlag = null;

  @Flag(name="output", usage="Path to the file to write schema defintions retrieved from the "
      + "schema table.  (will append if the file already exists)")
  private String mOutputFlag = null;

  /** URI of the Kiji instance housing the target schema table. */
  private KijiURI mURI = null;

  /** Kiji instance housing the target schema table. */
  private Kiji mKiji = null;

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
    mURI = KijiURI.newBuilder(mURIFlag).build();
    if (mRegisterFlag == null || mRegisterFlag.isEmpty()) {
      Preconditions.checkArgument(mGetFlag != null,
          "Specify exactly one of '--register' and '--get-schema'");
      Preconditions.checkArgument(mOutputFlag != null && !mOutputFlag.isEmpty());
    } else {
      Preconditions.checkArgument(mGetFlag == null,
          "Specify exactly one of '--register' and '--get-schema'");
      Preconditions.checkArgument(mOutputFlag == null || mOutputFlag.isEmpty());
    }
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    mKiji = Kiji.Factory.open(mURI);
    try {
      final KijiSchemaTable table = mKiji.getSchemaTable();
      if (mRegisterFlag != null && !mRegisterFlag.isEmpty()) {
        final File file = new File(mRegisterFlag);
        final Schema schema = new Schema.Parser().parse(file);
        final long id = table.getOrCreateSchemaId(schema);
        if (isInteractive()) {
          getPrintStream().println("Schema table schema ID for the given schema is: \n");
        }
        getPrintStream().println(id);
        return SUCCESS;
      } else {
        final Schema schema = table.getSchema(mGetFlag);
        Preconditions.checkArgument(schema != null, "No schema definition with ID: %s", mGetFlag);
        try {
          final File file = new File(mOutputFlag);
          final boolean fileCreated = file.createNewFile();
          if (fileCreated) {
            final FileOutputStream fop = new FileOutputStream(file.getAbsoluteFile());
            try {
              fop.write(schema.toString().getBytes("utf-8"));
              fop.flush();
            } finally {
              fop.close();
            }
          } else {
            if (mayProceed("File: %s already exists, do you wish to overwrite the file?",
                mOutputFlag)) {
              final FileOutputStream fop = new FileOutputStream(file.getAbsoluteFile());
              try {
                fop.write(schema.toString().getBytes("utf-8"));
              } finally {
                fop.flush();
                fop.close();
              }
            } else {
              return FAILURE;
            }
          }
          if (isInteractive()) {
            getPrintStream().printf("Schema definition written to: %s", mOutputFlag);
          }
          return SUCCESS;
        } catch (IOException ioe) {
          LOG.error("Error writing to file: {}", mOutputFlag);
          throw ioe;
        }
      }
    } finally {
      mKiji.release();
    }
  }
}
