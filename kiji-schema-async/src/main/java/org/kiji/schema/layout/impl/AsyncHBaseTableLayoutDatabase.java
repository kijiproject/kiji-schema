/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.schema.layout.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.stumbleupon.async.Deferred;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.SpecificCellDecoderFactory;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.SchemaStorage;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.avro.TableLayoutBackupEntry;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.avro.TableLayoutsBackup;
import org.kiji.schema.impl.AvroCellEncoder;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayoutDatabase;
import org.kiji.schema.layout.TableLayoutBuilder;
import org.kiji.schema.layout.TableLayoutBuilder.LayoutOptions;
import org.kiji.schema.layout.TableLayoutBuilder.LayoutOptions.SchemaFormat;
import org.kiji.schema.platform.SchemaPlatformBridge;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

/**
 * <p>Manages Kiji table layouts using a column family in an HBase table as a backing store.</p>
 *
 * <p>
 * The HTable row key is the name of the table, and the row has 3 columns:
 *   <li> the layout update, as specified by the user/submitter; </li>
 *   <li> the effective layout after applying the update; </li>
 *   <li> a hash of the effective layout. </li>
 * </p>
 *
 * <p>
 * Layouts and layout updates are encoded as Kiji cells, using Avro schema hashes, and as
 * TableLayoutDesc Avro records.
 * </p>
 *
 * <p>A static method, <code>getHColumnDescriptor</code> returns the description of an
 * HColumn that should be used to construct the HTable for the backing store.</p>
 */
@ApiAudience.Private
public final class AsyncHBaseTableLayoutDatabase implements KijiTableLayoutDatabase {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncHBaseTableLayoutDatabase.class);

  /**
   * HBase column qualifier used to store layout updates.
   * Layout updates are binary encoded TableLayoutDesc records.
   */
  public static final String QUALIFIER_UPDATE = "update";
  private static final byte[] QUALIFIER_UPDATE_BYTES = Bytes.UTF8(QUALIFIER_UPDATE);

  /**
   * HBase column qualifier used to store absolute layouts.
   * Table layouts are binary encoded TableLayoutDesc records.
   */
  public static final String QUALIFIER_LAYOUT = "layout";
  private static final byte[] QUALIFIER_LAYOUT_BYTES = Bytes.UTF8(QUALIFIER_LAYOUT);

  /**
   * HBase column qualifier used to store layout IDs.
   * Currently, IDs are assigned using a long counter starting at 1, and encoded as a string.
   */
  public static final String QUALIFIER_LAYOUT_ID = "layout_id";
  private static final byte[] QUALIFIER_LAYOUT_ID_BYTES = Bytes.UTF8(QUALIFIER_LAYOUT_ID);

  /** URI of the Kiji instance this layout database is for. */
  private final KijiURI mKijiURI;

  /** The HBaseClient of the Kiji instance this layout database belongs to. */
  private final HBaseClient mHBClient;

  /** HBase table name */
  private final byte[] mTableName;

  /** The column family in the HTable to use for storing layouts. */
  private final String mFamily;

  /** HBase column family, as bytes. */
  private final byte[] mFamilyBytes;

  /** The schema table. */
  private final KijiSchemaTable mSchemaTable;

  /** Kiji cell encoder. */
  private final KijiCellEncoder mCellEncoder;

  /** Decoder for concrete layout cells. */
  private final KijiCellDecoder<TableLayoutDesc> mCellDecoder;

  private static final CellSchema CELL_SCHEMA = CellSchema.newBuilder()
      .setStorage(SchemaStorage.HASH)
      .setType(SchemaType.CLASS)
      .setValue(TableLayoutDesc.SCHEMA$.getFullName())
      .build();


  /**
   * Creates a new <code>AsyncTableLayoutDatabase</code> instance.
   *
   * <p>This class does not take ownership of the HTable.  The caller should close it when
   * it is no longer needed.</p>
   *
   * @param kijiURI URI of the Kiji instance this layout database belongs to.
   * @param hbClient HBaseClient of the Kiji instance this layout database belongs to.
   * @param tableName The name of the layout database.
   * @param family The name of the column family within the HTable used to store layout data.
   * @param schemaTable The Kiji schema table.
   * @throws java.io.IOException on I/O error.
   */
  public AsyncHBaseTableLayoutDatabase(
      KijiURI kijiURI,
      HBaseClient hbClient,
      byte[] tableName,
      String family,
      KijiSchemaTable schemaTable
  )
      throws IOException {
    mKijiURI = kijiURI;
    mTableName = tableName;
    mHBClient = Preconditions.checkNotNull(hbClient);
    mFamily = Preconditions.checkNotNull(family);
    mFamilyBytes = Bytes.UTF8(mFamily);
    mSchemaTable = Preconditions.checkNotNull(schemaTable);
    final CellSpec cellSpec = CellSpec.fromCellSchema(CELL_SCHEMA, mSchemaTable);
    mCellEncoder = new AvroCellEncoder(cellSpec);
    mCellDecoder = SpecificCellDecoderFactory.get().create(cellSpec);
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableLayout updateTableLayout(String tableName, TableLayoutDesc layoutUpdate)
      throws IOException {

    // Normalize the new layout to use schema UIDs:
    TableLayoutBuilder layoutBuilder = new TableLayoutBuilder(mSchemaTable);
    final TableLayoutDesc update = layoutBuilder.normalizeTableLayoutDesc(
        layoutUpdate,
        new LayoutOptions()
            .setSchemaFormat(SchemaFormat.UID));

    // Fetch all the layout history:
    final List<KijiTableLayout> layouts =
        getTableLayoutVersions(tableName, HConstants.ALL_VERSIONS);
    final KijiTableLayout currentLayout = layouts.isEmpty() ? null : layouts.get(0);
    final KijiTableLayout tableLayout = KijiTableLayout.createUpdatedLayout(update, currentLayout);

    Preconditions.checkArgument(tableName.equals(tableLayout.getName()));

    // Set of all the former layout IDs:
    final Set<String> layoutIDs = Sets.newHashSet();
    for (KijiTableLayout layout : layouts) {
      layoutIDs.add(layout.getDesc().getLayoutId());
    }

    final String refLayoutIdStr = update.getReferenceLayout();

    final boolean hasCurrentLayout = (null != currentLayout);
    final boolean hasRefLayoutId = (null != refLayoutIdStr);
    if (hasCurrentLayout && !hasRefLayoutId) {
      throw new IOException(String.format(
          "Layout for table '%s' does not specify reference layout ID.", tableName));
    }
    if (!hasCurrentLayout && hasRefLayoutId) {
      throw new IOException(String.format(
          "Initial layout for table '%s' must not specify reference layout ID.", tableName));
    }

    final String layoutId = tableLayout.getDesc().getLayoutId();

    if (layoutIDs.contains(layoutId)) {
      throw new InvalidLayoutException(tableLayout,
          String.format("Layout ID '%s' already exists", layoutId));
    }

    // Construct the Put request to write the layout to the HTable.
    final byte[] rowNameBytes = Bytes.UTF8(tableName);
    final PutRequest put = new PutRequest(
      mTableName,
      rowNameBytes,
      mFamilyBytes,
        QUALIFIER_UPDATE_BYTES,
        encodeTableLayoutDesc(update));

    final PutRequest put2 = new PutRequest(
        mTableName,
        rowNameBytes,
        mFamilyBytes,
        QUALIFIER_LAYOUT_BYTES,
        encodeTableLayoutDesc(tableLayout.getDesc()));

    final PutRequest put3 = new PutRequest(
        mTableName,
        rowNameBytes,
        mFamilyBytes,
        QUALIFIER_LAYOUT_ID_BYTES,
        Bytes.UTF8(layoutId));

    // Flush the writer schema for the Avro table layout first so other readers can see it.
    mSchemaTable.flush();

    final ArrayList<Deferred<Object>> workers = Lists.newArrayList();
    // Write the new layout:
    if (!hasCurrentLayout) {
      // New table, no reference layout, this is the first layout:
      try {
        workers.add(mHBClient.put(put));
        workers.add(mHBClient.put(put2));
        workers.add(mHBClient.put(put3));
        Deferred.group(workers).join();
      } catch (Exception e) {
        ZooKeeperUtils.wrapAndRethrow(e);
      }
    } else {
      // Make sure nobody else is walking ahead of us:
      try {
        final byte[] refLayoutIdBytes = Bytes.UTF8(refLayoutIdStr);
        if (mHBClient.compareAndSet(put3, refLayoutIdBytes).join()) {
          workers.add(mHBClient.put(put));
          workers.add(mHBClient.put(put2));
          Deferred.group(workers).join();
        }
        else {
          throw new IOException(
              String.format(
                  "Unable to update layout for table '%s' based on reference layout with ID '%s'",
                  tableName, refLayoutIdStr));
        }
      } catch (Exception e) {
        ZooKeeperUtils.wrapAndRethrow(e);
      }
    }

    return tableLayout;
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableLayout getTableLayout(String table) throws IOException {
    final List<KijiTableLayout> layouts = getTableLayoutVersions(table, 1);
    if (layouts.isEmpty()) {
      throw new KijiTableNotFoundException(
          KijiURI.newBuilder(mKijiURI).withTableName(table).build());
    }
    return layouts.get(0);
  }

  /** {@inheritDoc} */
  @Override
  public List<KijiTableLayout> getTableLayoutVersions(String table, int numVersions)
      throws IOException {
    Preconditions.checkArgument(numVersions >= 1,  "numVersions must be positive");

    final GetRequest get = new GetRequest(
        mTableName,
        Bytes.UTF8(table),
        mFamilyBytes,
        QUALIFIER_LAYOUT_BYTES);
    get.maxVersions(numVersions);

    final List<KijiTableLayout> layouts = Lists.newArrayList();
    try {
      final ArrayList<KeyValue> results = mHBClient.get(get).join();
      for (KeyValue column : results) {
        assert(Arrays.equals(column.family(), mFamilyBytes));
        assert(Arrays.equals(column.qualifier(), QUALIFIER_LAYOUT_BYTES));
        layouts.add(KijiTableLayout.newLayout(decodeTableLayoutDesc(column.value())));
      }
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }

    return layouts;
  }

  /** {@inheritDoc} */
  @Override
  public NavigableMap<Long, KijiTableLayout> getTimedTableLayoutVersions(
      String table, int numVersions) throws IOException {
    Preconditions.checkArgument(numVersions >= 1, "numVersions must be positive");

    // Gather the layout data from the table.
    final GetRequest get = new GetRequest(
        mTableName,
        Bytes.UTF8(table),
        mFamilyBytes,
        QUALIFIER_LAYOUT_BYTES);
    get.maxVersions(numVersions);

    /** Map from timestamp to table layout. */
    final NavigableMap<Long, KijiTableLayout> timedLayouts = Maps.newTreeMap();
    try {
      ArrayList<KeyValue> results = mHBClient.get(get).join();

      for (KeyValue kv : results) {
        final long timestamp = kv.timestamp();
        final KijiTableLayout layout = KijiTableLayout.newLayout(decodeTableLayoutDesc(kv.value()));
        Preconditions.checkState(timedLayouts.put(timestamp, layout) == null);
      }

    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }

    return timedLayouts;
  }

  /** {@inheritDoc} */
  @Override
  public void removeAllTableLayoutVersions(String table) throws IOException {
    final DeleteRequest deleteUpdate = new DeleteRequest(
        mTableName,
        Bytes.UTF8(table),
        mFamilyBytes,
        QUALIFIER_UPDATE_BYTES);
    final DeleteRequest deleteLayout = new DeleteRequest(
        mTableName,
        Bytes.UTF8(table),
        mFamilyBytes,
        QUALIFIER_LAYOUT_BYTES);
    final DeleteRequest deleteLayoutId = new DeleteRequest(
        mTableName,
        Bytes.UTF8(table),
        mFamilyBytes,
        QUALIFIER_LAYOUT_ID_BYTES);
    try {
      ArrayList<Deferred<Object>> workers = Lists.newArrayList();
      workers.add(mHBClient.delete(deleteUpdate));
      workers.add(mHBClient.delete(deleteLayout));
      workers.add(mHBClient.delete(deleteLayoutId));
      Deferred.group(workers).join();
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void removeRecentTableLayoutVersions(String table, int numVersions) throws IOException {
    Preconditions.checkArgument(numVersions >= 1, "numVersions must be positive");
    ArrayList<Deferred<Object>> workers = Lists.newArrayList();
    for (int i = 0; i < numVersions; i++) {
      final DeleteRequest deleteUpdate = new DeleteRequest(
          mTableName,
          Bytes.UTF8(table),
          mFamilyBytes,
          QUALIFIER_UPDATE_BYTES);
      final DeleteRequest deleteLayout = new DeleteRequest(
          mTableName,
          Bytes.UTF8(table),
          mFamilyBytes,
          QUALIFIER_LAYOUT_BYTES);
      final DeleteRequest deleteLayoutId = new DeleteRequest(
          mTableName,
          Bytes.UTF8(table),
          mFamilyBytes,
          QUALIFIER_LAYOUT_ID_BYTES);
      workers.add(mHBClient.delete(deleteUpdate));
      workers.add(mHBClient.delete(deleteLayout));
      workers.add(mHBClient.delete(deleteLayoutId));
    }
    try {
      Deferred.group(workers).join();
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public List<String> listTables() throws IOException {
    final Scanner scanner = mHBClient.newScanner(mTableName);
    scanner.setFamily(mFamily);
    scanner.setQualifier(QUALIFIER_LAYOUT_BYTES);
    scanner.setMaxVersions(1);
    final List<String> tableNames = Lists.newArrayList();
    try {
      ArrayList<ArrayList<KeyValue>> resultsFull = scanner.nextRows(1).join();
      while (null != resultsFull && resultsFull.size() == 1 && resultsFull.get(0).size() == 1) {
        KeyValue kv = resultsFull.get(0).get(0);
        tableNames.add(new String(kv.key()));

        resultsFull = scanner.nextRows(1).join();
      }
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }
    scanner.close();
    return tableNames;
  }

  /** {@inheritDoc} */
  @Override
  public boolean tableExists(String tableName) throws IOException {
    // TODO(gabe): Review
    boolean retval = false;
    Scanner scanner = mHBClient.newScanner(mTableName);
    scanner.setMaxVersions(1);
    scanner.setFamily(mFamilyBytes);
    scanner.setQualifier(QUALIFIER_LAYOUT_BYTES);

    try {
      ArrayList<ArrayList<KeyValue>> resultsFull = scanner.nextRows(1).join();
      while (null != resultsFull && resultsFull.size() == 1) {
        byte[] key = resultsFull.get(0).get(0).key();
        if (tableName.equals(new String(key))) {
          retval = true;
          break;
        }

        resultsFull = scanner.nextRows(1).join();
      }
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    } finally {
      if (null != scanner) {
        scanner.close();
      }
    }
    return retval;
  }

  /**
   * Gets the description of an HColumn suitable for storing the table layout database.
   *
   * @param family The family within the HTable used to store layout data.
   * @return The HColumn descriptor.
   */
  public static HColumnDescriptor getHColumnDescriptor(String family) {
    return SchemaPlatformBridge.get().createHColumnDescriptorBuilder(Bytes.UTF8(family))
        .setMaxVersions(HConstants.ALL_VERSIONS)
        .setCompressionType("none")
        .setInMemory(false)
        .setBlockCacheEnabled(true)
        .setTimeToLive(HConstants.FOREVER)
        .setBloomType("NONE")
        .build();
  }

  /** {@inheritDoc} */
  @Override
  public TableLayoutsBackup layoutsToBackup(String table) throws IOException {
    //TODO(gabe): REVIEW THIS CAREFULLY!!!
    byte[][] qualifiers = new byte[2][];
    qualifiers[0] = QUALIFIER_UPDATE_BYTES;
    qualifiers[1] = QUALIFIER_LAYOUT_BYTES;

    GetRequest getUpdates = new GetRequest(
        mTableName,
        Bytes.UTF8(table),
        mFamilyBytes,
        QUALIFIER_UPDATE_BYTES);

    GetRequest getLayout = new GetRequest(
        mTableName,
        Bytes.UTF8(table),
        mFamilyBytes,
        QUALIFIER_LAYOUT_BYTES);

    final List<TableLayoutBackupEntry> history = Lists.newArrayList();
    try {
      ArrayList<KeyValue> updateResults = mHBClient.get(getUpdates).join();
      ArrayList<KeyValue> layoutResults = mHBClient.get(getLayout).join();
      if (layoutResults.isEmpty() && updateResults.isEmpty()) {
        LOG.info(String.format("There is no row in the MetaTable named '%s'.", table));
      }
      int i = 0;
      for (KeyValue kv : layoutResults) {
        final TableLayoutDesc layout = decodeTableLayoutDesc(kv.value());
        TableLayoutDesc update = null;
        while (i < updateResults.size() && updateResults.get(i).timestamp()
            <= kv.timestamp()) { //TODO: Double check this. Should it be >=???
          KeyValue updateKV = updateResults.get(i);
          if (updateKV.timestamp() == kv.timestamp()) {
            update = decodeTableLayoutDesc(updateKV.value());
          }
          i += 1;
        }
        history.add(
            TableLayoutBackupEntry.newBuilder()
                .setLayout(layout)
                .setUpdate(update)
                .setTimestamp(kv.timestamp())
                .build());
      }
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }
    TableLayoutsBackup backup = TableLayoutsBackup.newBuilder().setLayouts(history).build();
    return backup;
  }

  /** {@inheritDoc} */
  @Override
  public void restoreLayoutsFromBackup(String tableName, TableLayoutsBackup layoutBackup) throws
      IOException {
    // TODO(gabe): Review
    LOG.info(String.format("Restoring layout history for table '%s'.", tableName));
    ArrayList<Deferred<Object>> workers = Lists.newArrayList();
    for (TableLayoutBackupEntry lbe : layoutBackup.getLayouts()) {
      final byte[] layoutBytes = encodeTableLayoutDesc(lbe.getLayout());
      final PutRequest putLayout = new PutRequest(
          mTableName,
          Bytes.UTF8(tableName),
          mFamilyBytes,
          QUALIFIER_LAYOUT_BYTES,
          layoutBytes);
        if (lbe.getUpdate() != null) {
          final byte[] updateBytes = encodeTableLayoutDesc(lbe.getUpdate());
          final long timestamp = lbe.getTimestamp();
          PutRequest putUpdate = new PutRequest(
              mTableName,
              Bytes.UTF8(tableName),
              mFamilyBytes,
              QUALIFIER_UPDATE_BYTES,
              updateBytes,
              timestamp);
          workers.add(mHBClient.put(putUpdate));
        }
        workers.add(mHBClient.put(putLayout));
    }
    try {
      workers.add(mHBClient.flush());
      Deferred.group(workers).join();
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }
  }

  /**
   * Decodes a table layout descriptor from binary.
   *
   * @param bytes Serialized table layout descriptor.
   * @return Deserialized table layout descriptor.
   * @throws java.io.IOException on I/O or decoding error.
   */
  private TableLayoutDesc decodeTableLayoutDesc(byte[] bytes) throws IOException {
    return mCellDecoder.decodeValue(bytes);
  }

  /**
   * Encodes a table layout descriptor to binary.
   *
   * @param desc Table layout descriptor to serialize.
   * @return Table layout descriptor encoded as bytes.
   * @throws java.io.IOException on I/O or encoding error.
   */
  private byte[] encodeTableLayoutDesc(TableLayoutDesc desc) throws IOException {
    return mCellEncoder.encode(desc);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(AsyncHBaseTableLayoutDatabase.class)
        .add("uri", mKijiURI)
        .add("family", mFamily)
        .toString();
  }
}
