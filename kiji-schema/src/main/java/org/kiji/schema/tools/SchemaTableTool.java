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
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiSchemaTable.SchemaEntry;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ByteArrayFormatter;
import org.kiji.schema.util.BytesKey;
/**
 * Command-line tool for inspecting and modifying the Schema table.
 *
 * To register a new schema from a file:
 * <pre>
 *   kiji schema-table kiji://hbase-address/instance-name/ \
 *       --register=path/to/schema/definition
 * </pre>
 *
 * To look up a schema's id and hash from a schema definition:
 * <pre>
 *   kiji schema-table kiji://hbase-address/instance-name/ \
 *       --lookup=path/to/schema/definition
 * </pre>
 *
 * To get a schema definition and hash from a UID:
 * <pre>
 *   kiji schema-table kiji://hbase-address/instance-name/ \
 *       --get-schema-by-id=2 \
 *       [--output=path/to/schema/write/definition]
 * </pre>
 *
 * To get a schema definition and UID from a hash:
 * <pre>
 *   kiji schema-table kiji://hbase-address/instance-name/ \
 *       --get-schema-by-hash=ef:52:4e:a1:b9:1e:73:17:3d:93:8a:de:36:c1:db:32 \
 *       [--output=path/to/schema/write/definition]
 * </pre>
 *
 * To list all schemas in an instance:
 * <pre>
 *   kiji schema-table kiji://hbase-address/instance-name/ \
 *       --list=true
 * </pre>
 *
 * --interactive=false will suppress existing file warnings (old files will be overwritten by new
 * schema definition files).  Only UIDs and schema hashes will be printed to the console if
 * interactive is false.
 */

@ApiAudience.Private
public final class SchemaTableTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaTableTool.class);

  @Flag(name="register", usage="Path to a file containing a schema defintion "
      + "to add the given schema to the schema table and return it's UID and hash.")
  private String mRegisterFlag = null;

  @Flag(name="lookup", usage="Path to a file containing a schema defintion "
      + "to look up in the schema table and return it's UID and hash.")
  private String mLookupFlag = null;

  @Flag(name="get-schema-by-id", usage="ID of the schema to retrieve from the schema table.")
  private Long mGetByIdFlag = null;

  @Flag(name="get-schema-by-hash", usage="hash of the schema to retrieve from the schema table.")
  private String mGetByHashFlag = null;

  @Flag(name="list", usage="Set to true to list all the registered schemas in the kiji instance.")
  private Boolean mListFlag = null;

  @Flag(name="output", usage="Path to the file to write schema definitions retrieved from the "
      + "schema table (will overwrite if the file already exists, pending confirmation). "
      + "If not provided will output to standard out instead.")
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
    // Ensures that only one operation flag is specified at a time.
    int count = 0;
    count += (mRegisterFlag != null && !mRegisterFlag.isEmpty()) ? 1 : 0;
    count += (mLookupFlag != null && !mLookupFlag.isEmpty()) ? 1 : 0;
    count += (mGetByIdFlag != null) ? 1 : 0;
    count += (mGetByHashFlag != null && !mGetByHashFlag.isEmpty()) ? 1 : 0;
    count += (mListFlag != null && mListFlag) ? 1 : 0;
    Preconditions.checkArgument(
        count == 1, "Specify exactly one operation.");
  }

  /**
   * Writes a given schema to a file.
   *
   * @param schema The Avro Schema to write.
   * @return true if the file is written, false if it is not.
   * @throws IOException in case of an error writing the file.
   */
  private boolean writeDefinitionToFile(Schema schema) throws IOException {
    final File file = new File(mOutputFlag);
    final boolean fileCreated = file.createNewFile();
    if (!fileCreated && !mayProceed("File: %s already exists, do you wish to overwrite the file?",
          mOutputFlag)) {
        return false;
    } else {
      final FileOutputStream fop = new FileOutputStream(file.getAbsoluteFile());
      try {
        fop.write(schema.toString().getBytes("utf-8"));
        fop.flush();
      } finally {
        fop.close();
      }
    }
    return true;
  }

  /**
   * Register a schema.
   *
   * @return Tool exit code.
   * @throws IOException in case of an error.
   */
  private int registerSchema() throws IOException {
    final KijiSchemaTable table = mKiji.getSchemaTable();
    final File file = new File(mRegisterFlag);
    final Schema schema = new Schema.Parser().parse(file);
    final long id = table.getOrCreateSchemaId(schema);
    final String hash = table.getSchemaHash(schema).toString();
    if (isInteractive()) {
      getPrintStream().print("Schema ID for the given schema is: ");
    }
    getPrintStream().println(id);
    if (isInteractive()) {
      getPrintStream().print("Schema hash for the given schema is: ");
    }
    getPrintStream().println(hash);
    return SUCCESS;
  }

  /**
   * Lookup a schema.
   *
   * @return Tool exit code.
   * @throws IOException in case of an error.
   */
  private int lookupSchema() throws IOException {
    final KijiSchemaTable table = mKiji.getSchemaTable();
    final File file = new File(mLookupFlag);
    final Schema schema = new Schema.Parser().parse(file);
    final SchemaEntry sEntry = table.getSchemaEntry(schema);
    final long id = sEntry.getId();
    final BytesKey hash = sEntry.getHash();
    if (isInteractive()) {
      getPrintStream().print("Schema ID for the given schema is: ");
    }
    getPrintStream().println(id);
    if (isInteractive()) {
      getPrintStream().print("Schema hash for the given schema is: ");
    }
    getPrintStream().println(hash);
    return SUCCESS;
  }

  /**
   * Get a Schema by UID.
   *
   * @return Tool exit code.
   * @throws IOException in case of an error.
   */
  private int getById() throws IOException {
    final KijiSchemaTable table = mKiji.getSchemaTable();
    final Schema schema = table.getSchema(mGetByIdFlag);
    Preconditions.checkArgument(
        schema != null, "No schema definition with ID: %s", mGetByIdFlag);

    if (isInteractive()) {
      getPrintStream().print("Schema hash of the given schema is: ");
    }
    getPrintStream().println(table.getSchemaHash(schema));
    if (mOutputFlag != null && !mOutputFlag.isEmpty()) {
      // Attempt to write to the output file.
      try {
        if (writeDefinitionToFile(schema)) {
          if (isInteractive()) {
            getPrintStream().printf("Schema definition written to: %s%n", mOutputFlag);
          }
          return SUCCESS;
        } else {
          return FAILURE;
        }
      } catch (IOException ioe) {
        LOG.error("Error writing to file: {}", mOutputFlag);
        throw ioe;
      }
    } else {
      // Print the schema to standard out.
      if (isInteractive()) {
        getPrintStream().println("Schema:");
      }
      getPrintStream().println(schema.toString());
      return SUCCESS;
    }
  }

  /**
   * Get a Schema by hash.
   *
   * @return Tool exit code.
   * @throws IOException in case of an error.
   */
  private int getByHash() throws IOException {
    final KijiSchemaTable table = mKiji.getSchemaTable();
    final BytesKey bytesKey = new BytesKey(ByteArrayFormatter.parseHex(mGetByHashFlag, ':'));
    final SchemaEntry sEntry = table.getSchemaEntry(bytesKey);
    final Schema schema = sEntry.getSchema();

    if (isInteractive()) {
      getPrintStream().print("Schema ID for the given schema is: ");
    }
    getPrintStream().println(sEntry.getId());
    if (mOutputFlag != null && !mOutputFlag.isEmpty()) {
      // Attempt to write the definition to the output file.
      try {
        if (writeDefinitionToFile(schema)) {
          if (isInteractive()) {
            getPrintStream().printf("Schema definition written to: %s%n", mOutputFlag);
          }
          return SUCCESS;
        } else {
          return FAILURE;
        }
      } catch (IOException ioe) {
        LOG.error("Error writing to file: {}", mOutputFlag);
        throw ioe;
      }
    } else {
      // Print the schema to standard out.
      if (isInteractive()) {
        getPrintStream().println("Schema:");
      }
      getPrintStream().println(schema.toString());
      return SUCCESS;
    }
  }

  /**
   * List all schemas in the table.
   *
   * @return The Tool exit code.
   * @throws IOException in case of an error.
   */
  private int list() throws IOException {
    final KijiSchemaTable schemaTable = mKiji.getSchemaTable();
    long id = 0;
    Schema schema = schemaTable.getSchema(id);
    while (null != schema) {
      getPrintStream().printf("%d: %s%n", id, schema.toString());
      schema = schemaTable.getSchema(++id);
    }

    return SUCCESS;
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    Preconditions.checkArgument(nonFlagArgs.size() != 0, "Specify the KijiURI of your target "
        + "instance with `kiji get kiji://hbase-cluster/instance`");
    mURI = KijiURI.newBuilder(Preconditions.checkNotNull(nonFlagArgs.get(0))).build();
    mKiji = Kiji.Factory.open(mURI, getConf());
    try {
      if (mRegisterFlag != null && !mRegisterFlag.isEmpty()) {
        return registerSchema();
      } else if (mLookupFlag != null && !mLookupFlag.isEmpty()) {
        return lookupSchema();
      } else if (mGetByIdFlag != null) {
        return getById();
      } else if (mGetByHashFlag != null && !mGetByHashFlag.isEmpty()) {
        return getByHash();
      } else if (mListFlag) {
        return list();
      } else {
        throw new InternalKijiError("No operation specified.");
      }
    } finally {
      mKiji.release();
    }
  }
}
