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

package org.kiji.schema.impl;

import java.io.Closeable;
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
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiAdmin;
import org.kiji.schema.KijiAlreadyExistsException;
import org.kiji.schema.KijiManagedHBaseTableName;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiRowKeySplitter;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.ColumnId;
import org.kiji.schema.layout.impl.HTableSchemaTranslator;

/**
 * Administration API for managing a Kiji instance that uses HBase as its underlying data store.
 */
@ApiAudience.Private
public final class HBaseKijiAdmin implements KijiAdmin, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(KijiAdmin.class);

  /** HBase admin. */
  private final HBaseAdmin mHBaseAdmin;

  /** Manages this Kiji instance. */
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
  HBaseKijiAdmin(HBaseAdmin hbaseAdmin, Kiji kiji) {
    mHBaseAdmin = hbaseAdmin;
    mKiji = kiji;
  }

  /** {@inheritDoc} */
  @Override
  public void createTable(String tableName, KijiTableLayout tableLayout, boolean isRestore)
      throws IOException {
    createTable(tableName, tableLayout, isRestore, 1);
  }

  /** {@inheritDoc} */
  @Override
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

  /** {@inheritDoc} */
  @Override
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
      final HTableDescriptor desc =
          translator.toHTableDescriptor(mKiji.getURI().getInstance(), tableLayout);
      if (null != splitKeys) {
        mHBaseAdmin.createTable(desc, splitKeys);
      } else {
        mHBaseAdmin.createTable(desc);
      }
    } catch (TableExistsException tee) {
      if (isRestore) {
        LOG.info("Table already exists in HBase. Continuing with restore operation.");
      } else {
        final KijiURI tableURI = mKiji.getURI().setTableName(tableName);
        throw new KijiAlreadyExistsException(String.format(
            "Kiji table '%s' already exists.", tableURI), tableURI);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableLayout setTableLayout(String tableName, TableLayoutDesc update)
      throws IOException {
    return setTableLayout(tableName, update, false, null);
  }

  /** {@inheritDoc} */
  @Override
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
        translator.toHTableDescriptor(mKiji.getURI().getInstance(), newLayout);

    LOG.debug("Reading existing HBase schema");
    final KijiManagedHBaseTableName hbaseTableName =
        KijiManagedHBaseTableName.getKijiTableName(mKiji.getURI().getInstance(), tableName);
    HTableDescriptor currentTableDescriptor = null;
    try {
      currentTableDescriptor = mHBaseAdmin.getTableDescriptor(hbaseTableName.toBytes());
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
        mHBaseAdmin.disableTable(hbaseTableName.toString());
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
            mHBaseAdmin.addColumn(hbaseTableName.toString(), newColumnDescriptor);
          }
        } else if (!newColumnDescriptor.equals(currentColumnDescriptor)) {
          if (dryRun) {
            printStream.println("  Modifying locality group: " + lgName);
          } else {
            LOG.debug("Modifying column " + columnName);
            mHBaseAdmin.modifyColumn(hbaseTableName.toString(), newColumnDescriptor);
          }
        } else {
          LOG.debug("No changes needed for column " + columnName);
        }
      }

      if (!dryRun) {
        LOG.debug("Re-enabling HBase table");
        mHBaseAdmin.enableTable(hbaseTableName.toString());
      }
    }

    return newLayout;
  }

  /** {@inheritDoc} */
  @Override
  public void deleteTable(String tableName) throws IOException {
    // Delete from HBase.
    String hbaseTable = KijiManagedHBaseTableName.getKijiTableName(mKiji.getURI().getInstance(),
        tableName).toString();
    mHBaseAdmin.disableTable(hbaseTable);
    mHBaseAdmin.deleteTable(hbaseTable);

    // Delete from the meta table.
    mKiji.getMetaTable().deleteTable(tableName);

    // TODO(KIJI-401): HBaseAdmin lies about deleteTable being a synchronous operation.  Let's wait
    // until the table is actually gone.
    while (mHBaseAdmin.tableExists(hbaseTable)) {
      LOG.debug("Waiting for HBase table " + hbaseTable + " to be deleted...");
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        // Being interrupted here is fine.
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public List<String> getTableNames() throws IOException {
    return mKiji.getMetaTable().listTables();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mHBaseAdmin.close();
  }

  /** @return the underlying HBaseAdmin. */
  public HBaseAdmin getHBaseAdmin() {
    return mHBaseAdmin;
  }
}
