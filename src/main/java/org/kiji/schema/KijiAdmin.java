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

package org.kiji.schema;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.impl.HTableDescriptorComparator;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.ColumnId;
import org.kiji.schema.layout.impl.HTableSchemaTranslator;

/**
 * Administration API for managing a Kiji instance.
 */
@ApiAudience.Public
public final class KijiAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(KijiAdmin.class);

  /** HBase admin. */
  private final HBaseAdmin mHbaseAdmin;

  /** Meta table. */
  private final Kiji mKiji;

  /**
   * Creates a Kiji admin.
   *
   * <p>The parameters passed to the KijiAdmin must remain valid and open during the
   * lifetime of the KijiAdmin. It is the responsibility of the caller to close them.</p>
   *
   * @param hbaseAdmin The HBaseAdmin for the HBase cluster to use Kiji in.
   * @param kiji The Kiji instance to administer.
   */
  public KijiAdmin(HBaseAdmin hbaseAdmin, Kiji kiji) {
    mHbaseAdmin = hbaseAdmin;
    mKiji = kiji;
  }

  /**
   * Creates a Kiji table in an HBase instance.
   *
   * @param tableName The name of the table to create.
   * @param tableLayout The initial layout of the table (with unassigned column ids).
   * @param isRestore true if this is part of a metadata restore operation.
   *     This should be set to false by all callers except KijiMetadataTool.
   * @throws IOException If there is an error.
   */
  public void createTable(String tableName, KijiTableLayout tableLayout, boolean isRestore)
      throws IOException {
    createTable(tableName, tableLayout, isRestore, 1);
  }

  /**
   * Creates a Kiji table in an HBase instance.
   *
   * @param tableName The name of the table to create.
   * @param tableLayout The initial layout of the table (with unassigned column ids).
   * @param isRestore true if this is part of a metadata restore operation.
   * @param numRegions The initial number of regions to create.
   *
   * @throws IllegalArgumentException If numRegions is non-positive, or if numRegions is
   *     more than 1 and row key hashing on the table layout is disabled.
   * @throws IOException If there is an error.
   */
  public void createTable(String tableName, KijiTableLayout tableLayout, boolean isRestore,
      int numRegions) throws IOException {
    Preconditions.checkArgument((numRegions >= 1), "numRegions must be positive: " + numRegions);
    if (numRegions > 1) {
      if (tableLayout.getDesc().getKeysFormat().getEncoding() == RowKeyEncoding.RAW) {
        throw new IllegalArgumentException(
            "May not use numRegions > 1 if row key hashing is disabled in the layout");
      }
      createTable(tableName, tableLayout, isRestore, KijiRowKeySplitter.getSplitKeys(numRegions));
    } else {
      createTable(tableName, tableLayout, isRestore, null);
    }
  }

  /**
   * Creates a Kiji table in an HBase instance.
   *
   * @param tableName The name of the table to create.
   * @param tableLayout The initial layout of the table (with unassigned column ids).
   * @param isRestore true if this is part of a metadata restore operation.
   * @param splitKeys The initial key boundaries between regions.  There will be splitKeys
   *     + 1 regions created.  Pass null to specify the default single region.
   * @throws IOException If there is an error.
   */
  public void createTable(String tableName, KijiTableLayout tableLayout, boolean isRestore,
      byte[][] splitKeys) throws IOException {
    try {
      mKiji.getMetaTable().getTableLayout(tableName);
      throw new RuntimeException("Table " + tableName + " already exists");
    } catch (KijiTableNotFoundException e) {
      // Good.
    }

    if (!tableName.equals(tableLayout.getName())) {
      throw new RuntimeException(String.format(
          "Table name from layout descriptor '%s' does match table name '%s'.",
          tableLayout.getName(), tableName));
    }

    LOG.debug("Adding layout to the Kiji meta table");
    mKiji.getMetaTable().updateTableLayout(tableName, tableLayout.getDesc());

    LOG.debug("Creating table in HBase");
    try {
      final HTableSchemaTranslator translator = new HTableSchemaTranslator();
      final HTableDescriptor desc = translator.toHTableDescriptor(mKiji.getName(), tableLayout);
      if (null != splitKeys) {
        mHbaseAdmin.createTable(desc, splitKeys);
      } else {
        mHbaseAdmin.createTable(desc);
      }
    } catch (TableExistsException tee) {
      if (isRestore) {
        LOG.info("Table already exists in HBase. Continuing with restore operation.");
      } else {
        throw tee;
      }
    }
  }

  /**
   * Sets the layout of a table.
   *
   * @param tableName The name of the Kiji table to affect.
   * @param update Layout update.
   * @return the updated layout.
   * @throws IOException If there is an error.
   */
  public KijiTableLayout setTableLayout(String tableName, TableLayoutDesc update)
      throws IOException {
    return setTableLayout(tableName, update, false, null);
  }

  /**
   * Sets the layout of a table, or print the results of setting the table layout if
   * dryRun is true.
   *
   * @param tableName The name of the Kiji table to affect.
   * @param update Layout update.
   * @param dryRun true if this is a 'dry run', false if changes should be made.
   * @param printStream the print stream to use for information if dryRun is true.
   *     If null, stdout will be used.
   * @return the updated layout.
   * @throws IOException If there is an error.
   */
  public KijiTableLayout setTableLayout(
      String tableName,
      TableLayoutDesc update,
      boolean dryRun,
      PrintStream printStream)
      throws IOException {

    Preconditions.checkArgument((tableName != null) && !tableName.isEmpty());
    Preconditions.checkNotNull(update);

    if (dryRun && (null == printStream)) {
      printStream = System.out;
    }

    if (!tableName.equals(update.getName())) {
      throw new InvalidLayoutException(String.format(
          "Name of table in descriptor '%s' does not match table name '%s'.",
          update.getName(), tableName));
    }

    // Try to get the table layout first, which will throw a KijiTableNotFoundException if
    // there is no table.
    mKiji.getMetaTable().getTableLayout(tableName);

    KijiTableLayout newLayout = null;

    if (dryRun) {
      // Process column ids and perform validation, but don't actually update the meta table.
      final KijiMetaTable metaTable = mKiji.getMetaTable();
      final List<KijiTableLayout> layouts = metaTable.getTableLayoutVersions(tableName, 1);
      final KijiTableLayout currentLayout = layouts.isEmpty() ? null : layouts.get(0);
      newLayout = new KijiTableLayout(update, currentLayout);
    } else {
      // Actually set it.
      LOG.debug("Applying layout update: " + update);
      newLayout = mKiji.getMetaTable().updateTableLayout(tableName, update);
    }
    Preconditions.checkState(newLayout != null);

    if (dryRun) {
      printStream.println("This table layout is valid.");
    }

    LOG.debug("Computing new HBase schema");
    final HTableSchemaTranslator translator = new HTableSchemaTranslator();
    final HTableDescriptor newTableDescriptor =
        translator.toHTableDescriptor(mKiji.getName(), newLayout);

    LOG.debug("Reading existing HBase schema");
    final KijiManagedHBaseTableName hbaseTableName =
        KijiManagedHBaseTableName.getKijiTableName(mKiji.getName(), tableName);
    HTableDescriptor currentTableDescriptor = null;
    try {
      currentTableDescriptor = mHbaseAdmin.getTableDescriptor(hbaseTableName.toBytes());
    } catch (TableNotFoundException tnfe) {
      if (!dryRun) {
        throw tnfe; // Not in dry-run mode; table needs to exist. Rethrow exception.
      }
    }
    if (currentTableDescriptor == null) {
      if (dryRun) {
        printStream.println("Would create new table: " + tableName);
        currentTableDescriptor = HTableDescriptorComparator.makeEmptyTableDescriptor(
            hbaseTableName);
      } else {
        throw new RuntimeException("Table " + hbaseTableName.getKijiTableName()
            + " does not exist");
      }
    }
    LOG.debug("Existing table descriptor: " + currentTableDescriptor);
    LOG.debug("New table descriptor: " + newTableDescriptor);

    LOG.debug("Checking for differences between the new HBase schema and the existing one");
    final HTableDescriptorComparator comparator = new HTableDescriptorComparator();
    if (0 == comparator.compare(currentTableDescriptor, newTableDescriptor)) {
      LOG.debug("HBase schemas are the same.  No need to change HBase schema");
      if (dryRun) {
        printStream.println("This layout does not require any physical table schema changes.");
      }
    } else {
      LOG.debug("HBase schema must be changed, but no columns will be deleted");

      if (dryRun) {
        printStream.println("Changes caused by this table layout:");
      } else {
        LOG.debug("Disabling HBase table");
        mHbaseAdmin.disableTable(hbaseTableName.toString());
      }

      for (HColumnDescriptor newColumnDescriptor : newTableDescriptor.getFamilies()) {
        final String columnName = Bytes.toString(newColumnDescriptor.getName());
        final ColumnId columnId = ColumnId.fromString(columnName);
        final String lgName = newLayout.getLocalityGroupIdNameMap().get(columnId);
        final HColumnDescriptor currentColumnDescriptor =
            currentTableDescriptor.getFamily(newColumnDescriptor.getName());
        if (null == currentColumnDescriptor) {
          if (dryRun) {
            printStream.println("  Creating new locality group: " + lgName);
          } else {
            LOG.debug("Creating new column " + columnName);
            mHbaseAdmin.addColumn(hbaseTableName.toString(), newColumnDescriptor);
          }
        } else if (!newColumnDescriptor.equals(currentColumnDescriptor)) {
          if (dryRun) {
            printStream.println("  Modifying locality group: " + lgName);
          } else {
            LOG.debug("Modifying column " + columnName);
            mHbaseAdmin.modifyColumn(hbaseTableName.toString(), newColumnDescriptor);
          }
        } else {
          LOG.debug("No changes needed for column " + columnName);
        }
      }

      if (!dryRun) {
        LOG.debug("Re-enabling HBase table");
        mHbaseAdmin.enableTable(hbaseTableName.toString());
      }
    }

    return newLayout;
  }

  /**
   * Deletes a Kiji table.  Removes it from HBase.
   *
   * @param tableName The name of the Kiji table to delete.
   * @throws IOException If there is an error.
   */
  public void deleteTable(String tableName) throws IOException {
    // Delete from HBase.
    String hbaseTable = KijiManagedHBaseTableName.getKijiTableName(mKiji.getName(),
        tableName).toString();
    mHbaseAdmin.disableTable(hbaseTable);
    mHbaseAdmin.deleteTable(hbaseTable);

    // Delete from the meta table.
    mKiji.getMetaTable().deleteTable(tableName);

    // HBaseAdmin lies about deleteTable being a synchronous operation.  Let's wait until
    // the table is actually gone.
    while (mHbaseAdmin.tableExists(hbaseTable)) {
      LOG.debug("Waiting for HBase table " + hbaseTable + " to be deleted...");
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        // Oh well...
      }
    }
  }

  /**
   * Gets the list of Kiji table names.
   *
   * @return A list of the names of Kiji tables installed in the Kiji instance.
   * @throws IOException If there is an error.
   */
  public List<String> getTableNames() throws IOException {
    return mKiji.getMetaTable().listTables();
  }
}
