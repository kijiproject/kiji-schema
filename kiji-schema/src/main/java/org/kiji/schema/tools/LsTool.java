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
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KConstants;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.hbase.HBaseFactory;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.util.ResourceUtils;

/**
 * Command-line tool to explore kiji table data like the 'ls' command of a unix shell.
 *
 * List all kiji instances:
 * <pre>
 *   kiji ls kiji://hbase-address/
 * </pre>
 *
 * List all kiji tables:
 * <pre>
 *   kiji ls kiji://hbase-address/kiji-instance/
 * </pre>
 *
 * List all columns in a kiji table 'foo':
 * <pre>
 *   kiji ls kiji://hbase-address/kiji-instance/foo/
 * </pre>
 *
 */
@ApiAudience.Private
public final class LsTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(LsTool.class);

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "ls";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "List kiji instances, tables and rows.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Data";
  }

  /**
   * Lists all kiji instances.
   *
   * @param hbaseURI URI of the HBase instance to list the content of.
   * @return A program exit code (zero on success).
   * @throws IOException If there is an error.
   */
  private int listInstances(KijiURI hbaseURI) throws IOException {
    for (String instanceName : getInstanceNames(hbaseURI)) {
      getPrintStream().println(KijiURI.newBuilder(hbaseURI).withInstanceName(instanceName).build());
    }
    return SUCCESS;
  }

  /**
   * Returns a set of instance names.
   *
   * @param hbaseURI URI of the HBase instance to list the content of.
   * @return ordered set of instance names.
   * @throws IOException on I/O error.
   */
  protected static Set<String> getInstanceNames(KijiURI hbaseURI) throws IOException {
    // TODO(SCHEMA-188): Consolidate this logic in a single central place:
    final Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.ZOOKEEPER_QUORUM,
        Joiner.on(",").join(hbaseURI.getZookeeperQuorumOrdered()));
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, hbaseURI.getZookeeperClientPort());
    final HBaseAdmin hbaseAdmin =
        HBaseFactory.Provider.get().getHBaseAdminFactory(hbaseURI).create(conf);

    try {
      final Set<String> instanceNames = Sets.newTreeSet();
      for (HTableDescriptor hTableDescriptor : hbaseAdmin.listTables()) {
        final String instanceName = parseInstanceName(hTableDescriptor.getNameAsString());
        if (null != instanceName) {
          instanceNames.add(instanceName);
        }
      }
      return instanceNames;
    } finally {
      ResourceUtils.closeOrLog(hbaseAdmin);
    }
  }

  /**
   * Parses a table name for a kiji instance name.
   *
   * @param kijiTableName The table name to parse
   * @return instance name (or null if none found)
   */
  protected static String parseInstanceName(String kijiTableName) {
    String[] parts = org.apache.hadoop.util.StringUtils.split(kijiTableName, '\u0000', '.');
    if (parts.length < 3 || !KijiURI.KIJI_SCHEME.equals(parts[0])) {
      return null;
    }
    return parts[1];
  }

  /**
   * Lists all the tables in a kiji instance.
   *
   * @param kiji Kiji instance to list the tables of.
   * @return A program exit code (zero on success).
   * @throws IOException If there is an error.
   */
  private int listTables(Kiji kiji) throws IOException {
    for (String name : kiji.getTableNames()) {
      getPrintStream().println(kiji.getURI() + name);
    }
    return SUCCESS;
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    int status = SUCCESS;
    if (nonFlagArgs.isEmpty()) {
      getPrintStream().printf("URI was unspecified, reverting to default: %s%n",
          KConstants.DEFAULT_URI);
      return run(KijiURI.newBuilder(KConstants.DEFAULT_URI).build());
    } else {
      for (String arg : nonFlagArgs) {
        status = Math.max(status, run(KijiURI.newBuilder(arg).build()));
      }
      return status;
    }
  }

  /**
   * Lists instances, tables, or columns in a kiji URI.
   * Can be recursively called by run(List<String>).
   *
   * @param argURI Kiji URI from which to list instances, tables, or columns.
   * @return A program exit code (zero on success).
   * @throws Exception If there is an error.
   */
  private int run(final KijiURI argURI) throws Exception {
    if (argURI.getZookeeperQuorum() == null) {
      getPrintStream().printf("Specify a cluster with argument: kiji://zookeeper-quorum%n");
      return FAILURE;
    }

    if (argURI.getInstance() == null) {
      // List instances in this kiji instance.
      return listInstances(argURI);
    }

    final Kiji kiji = Kiji.Factory.open(argURI);
    try {
      if (argURI.getTable() == null) {
        // List tables in this kiji instance.
        return listTables(kiji);
      }

      final KijiTable table = kiji.openTable(argURI.getTable());
      try {
        final KijiTableLayout tableLayout = table.getLayout();
        for (FamilyLayout family : tableLayout.getFamilies()) {
          if (family.isMapType()) {
            getPrintStream().println(KijiURI.newBuilder(table.getURI())
                .addColumnName(new KijiColumnName(family.getName()))
                .build());
          } else {
            for (ColumnLayout column : family.getColumns()) {
              getPrintStream().println(KijiURI.newBuilder(table.getURI())
                  .addColumnName(new KijiColumnName(family.getName(), column.getName()))
                  .build());
            }
          }
        }
        return SUCCESS;
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
    System.exit(new KijiToolLauncher().run(new LsTool(), args));
  }
}
