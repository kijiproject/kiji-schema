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

import java.util.AbstractMap;
import java.util.List;
import java.util.Locale;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.CloseableIterable;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.schema.util.ResourceUtils;

/**
 * Command-line tool to inspect and modify Kiji system tables.
 *
 * --kiji to specify the target instance.
 *
 * --do=[put, get, put-version, get-version, get-all]
 *   put <key> <value> to assign the <key> property to <value>
 *   get <key> to return the value of the <key> property
 *   put-version <version> to assign a new version value
 *   get-version to return the version information
 *   get-all to return all system table properties, including version
 */
@ApiAudience.Private
public final class SystemTableTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(SystemTableTool.class);

  @Flag(name="kiji", usage="KijiURI of the kiji instance to inspect.")
  private String mURIFlag = "";

  @Flag(name="do", usage=
      "\"put <key> <value>\"; "
      + "\"get <key>\"; "
      + "\"put-version <version>\"; "
      + "\"get-version\""
      + "\"get-all\"")
  private String mDoFlag = "get-all";

  /** KijiURI of the target instance. */
  private KijiURI mKijiURI;

  /** Kiji instance of specified system table. */
  private Kiji mKiji;

  /** KijiSystemTable to inspect. */
  private KijiSystemTable mTable;

  /** Operation selector mode. */
  private static enum DoMode {
    GET, PUT, GET_VERSION, PUT_VERSION, GET_ALL
  }

  /** Operation mode. */
  private DoMode mDoMode;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "system-table";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Inspect or modify a kiji system table.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Metadata";
  }

  /** {@inheritDoc} */
  @Override
  public void validateFlags() throws Exception {
    mKijiURI = KijiURI.newBuilder(mURIFlag).build();
    Preconditions.checkNotNull(mKijiURI.getInstance(),
        "Specify a Kiji instance with --kiji=kiji://hbase-address/kiji-instance");
    Preconditions.checkNotNull(mDoFlag,
        "Specify an operation with --do=[put, get, put-version, get-version, get-all]");
    try {
      mDoMode = DoMode.valueOf(mDoFlag.toUpperCase(Locale.ROOT).replace("-", "_"));
    } catch (IllegalArgumentException iae) {
      getPrintStream().printf("Invalid --do command: '%s'.%n", mDoFlag);
      throw iae;
    }
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    mKiji = Kiji.Factory.open(mKijiURI, getConf());
    try {
      mTable = mKiji.getSystemTable();
      switch (mDoMode) {
        case GET: {
          Preconditions.checkArgument(nonFlagArgs.size() == 1,
              "Incorrect number of arguments for \"get <key>\".");
          String key = nonFlagArgs.get(0);
          String value = Bytes.toString(mTable.getValue(key));
          if (value == null || value.isEmpty()) {
            getPrintStream().printf("No system table property named '%s'.%n", key);
            return FAILURE;
          }
          getPrintStream().println(key + " = " + value);
          return SUCCESS;
        }
        case PUT: {
          Preconditions.checkArgument(nonFlagArgs.size() == 2,
              "Incorrect number of arguments for \"put <key> <value>\".");
          String key = nonFlagArgs.get(0);
          byte[] value = Bytes.toBytes(nonFlagArgs.get(1));
          byte[] originalValue = mTable.getValue(key);
          if (originalValue == null || mayProceed("There is an existing value assigned to %s."
              + " Do you want to overwrite \"%s\" with \"%s\".",
              key, Bytes.toString(originalValue), nonFlagArgs.get(1))) {
            mTable.putValue(key, value);
          }
          return SUCCESS;
        }
        case GET_VERSION: {
          Preconditions.checkArgument(nonFlagArgs.isEmpty(),
              "Incorrect number of arguments for \"get-version\".");
          String version = mTable.getDataVersion().toString();
          getPrintStream().println("Kiji data version = " + version);
          return SUCCESS;
        }
        case PUT_VERSION: {
          Preconditions.checkArgument(nonFlagArgs.size() == 1,
              "Incorrect number of arguments for \"put-version <version>\".");
          ProtocolVersion version = ProtocolVersion.parse(nonFlagArgs.get(0));
          if (isInteractive()) {
            if (yesNoPrompt("Changing the version information of a system table may cause "
                + "the kiji instance to become unresponsive. "
                + "Are you sure you want to proceed?")) {
              mTable.setDataVersion(version);
              return SUCCESS;
            } else {
              getPrintStream().println("Aborted");
              return SUCCESS;
            }
          } else {
            mTable.setDataVersion(version);
            return SUCCESS;
          }
        }
        case GET_ALL: {
          Preconditions.checkArgument(nonFlagArgs.isEmpty(),
              "Incorrect number of arguments for \"get-all\".");
          @SuppressWarnings("unchecked")
          CloseableIterable<AbstractMap.SimpleEntry<String, byte[]>> values = mTable.getAll();
          if (isInteractive()) {
            getPrintStream().println("Listing all system table properties:");
          }
          try {
            for (AbstractMap.SimpleEntry<String, byte[]> pair : values) {
              getPrintStream().println(pair.getKey() + " = " + Bytes.toString(pair.getValue()));
            }
            return SUCCESS;
          } finally {
            values.close();
          }
        }
        default: {
          throw new InternalKijiError("unsupported enum value: " + mDoMode);
        }
      }
    } finally {
      ResourceUtils.releaseOrLog(mKiji);
    }
  }
}
