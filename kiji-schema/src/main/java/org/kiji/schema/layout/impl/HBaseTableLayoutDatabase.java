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

package org.kiji.schema.layout.impl;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
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
import org.kiji.schema.util.ResourceUtils;

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
public final class HBaseTableLayoutDatabase implements KijiTableLayoutDatabase {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseTableLayoutDatabase.class);

  /**
   * HBase column qualifier used to store layout updates.
   * Layout updates are binary encoded TableLayoutDesc records.
   */
  public static final String QUALIFIER_UPDATE = "update";
  private static final byte[] QUALIFIER_UPDATE_BYTES = Bytes.toBytes(QUALIFIER_UPDATE);

  /**
   * HBase column qualifier used to store absolute layouts.
   * Table layouts are binary encoded TableLayoutDesc records.
   */
  public static final String QUALIFIER_LAYOUT = "layout";
  private static final byte[] QUALIFIER_LAYOUT_BYTES = Bytes.toBytes(QUALIFIER_LAYOUT);

  /**
   * HBase column qualifier used to store layout IDs.
   * Currently, IDs are assigned using a long counter starting at 1, and encoded as a string.
   */
  public static final String QUALIFIER_LAYOUT_ID = "layout_id";
  private static final byte[] QUALIFIER_LAYOUT_ID_BYTES = Bytes.toBytes(QUALIFIER_LAYOUT_ID);

  /** URI of the Kiji instance this layout database is for. */
  private final KijiURI mKijiURI;

  /** The HTable to use to store the layouts. */
  private final HTableInterface mTable;

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
   * Creates a new <code>HBaseTableLayoutDatabase</code> instance.
   *
   * <p>This class does not take ownership of the HTable.  The caller should close it when
   * it is no longer needed.</p>
   *
   * @param kijiURI URI of the Kiji instance this layout database belongs to.
   * @param htable The HTable used to store the layout data.
   * @param family The name of the column family within the HTable used to store layout data.
   * @param schemaTable The Kiji schema table.
   * @throws IOException on I/O error.
   */
  public HBaseTableLayoutDatabase(
      KijiURI kijiURI,
      HTableInterface htable,
      String family,
      KijiSchemaTable schemaTable)
      throws IOException {
    mKijiURI = kijiURI;
    mTable = Preconditions.checkNotNull(htable);
    mFamily = Preconditions.checkNotNull(family);
    mFamilyBytes = Bytes.toBytes(mFamily);
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
    final byte[] tableNameBytes = Bytes.toBytes(tableName);
    final Put put = new Put(tableNameBytes)
        .add(mFamilyBytes, QUALIFIER_UPDATE_BYTES, encodeTableLayoutDesc(update))
        .add(mFamilyBytes, QUALIFIER_LAYOUT_BYTES, encodeTableLayoutDesc(tableLayout.getDesc()))
        .add(mFamilyBytes, QUALIFIER_LAYOUT_ID_BYTES, Bytes.toBytes(layoutId));

    // Flush the writer schema for the Avro table layout first so other readers can see it.
    mSchemaTable.flush();

    // Write the new layout:
    if (!hasCurrentLayout) {
      // New table, no reference layout, this is the first layout:
      mTable.put(put);
    } else {
      // Make sure nobody else is walking ahead of us:
      final byte[] refLayoutIdBytes = Bytes.toBytes(refLayoutIdStr);
      if (!mTable.checkAndPut(
          tableNameBytes, mFamilyBytes, QUALIFIER_LAYOUT_ID_BYTES, refLayoutIdBytes, put)) {
        throw new IOException(String.format(
            "Unable to update layout for table '%s' based on reference layout with ID '%s'",
            tableName, refLayoutIdStr));
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

    final Get get = new Get(Bytes.toBytes(table))
        .addColumn(mFamilyBytes, QUALIFIER_LAYOUT_BYTES)
        .setMaxVersions(numVersions);
    final Result result = mTable.get(get);

    final List<KijiTableLayout> layouts = Lists.newArrayList();
    for (KeyValue column : result.getColumn(mFamilyBytes, QUALIFIER_LAYOUT_BYTES)) {
      layouts.add(KijiTableLayout.newLayout(decodeTableLayoutDesc(column.getValue())));
    }
    return layouts;
  }

  /** {@inheritDoc} */
  @Override
  public NavigableMap<Long, KijiTableLayout> getTimedTableLayoutVersions(
      String table, int numVersions) throws IOException {
    Preconditions.checkArgument(numVersions >= 1, "numVersions must be positive");

    // Gather the layout data from the Htable.
    final Get get = new Get(Bytes.toBytes(table))
        .addColumn(mFamilyBytes, QUALIFIER_LAYOUT_BYTES)
        .setMaxVersions(numVersions);
    final Result result = mTable.get(get);

    /** Map from timestamp to table layout. */
    final NavigableMap<Long, KijiTableLayout> timedLayouts = Maps.newTreeMap();

    // Pull out the full map: family -> qualifier -> timestamp -> TableLayoutDesc.
    // Family and qualifier are already specified : the 2 outer maps must be size 11.
    final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap =
        result.getMap();
    Preconditions.checkState(familyMap.size() == 1);
    final NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifierMap =
        familyMap.get(familyMap.firstKey());
    Preconditions.checkState(qualifierMap.size() == 1);
    final NavigableMap<Long, byte[]> timeSerieMap = qualifierMap.get(qualifierMap.firstKey());
    for (Map.Entry<Long, byte[]> timeSerieEntry : timeSerieMap.entrySet()) {
      final long timestamp = timeSerieEntry.getKey();
      final byte[] bytes = timeSerieEntry.getValue();
      final KijiTableLayout layout = KijiTableLayout.newLayout(decodeTableLayoutDesc(bytes));
      Preconditions.checkState(timedLayouts.put(timestamp, layout) == null);
    }
    return timedLayouts;
  }

  /** {@inheritDoc} */
  @Override
  public void removeAllTableLayoutVersions(String table) throws IOException {
    final Delete delete = new Delete(Bytes.toBytes(table))
        .deleteColumns(mFamilyBytes, QUALIFIER_UPDATE_BYTES)
        .deleteColumns(mFamilyBytes, QUALIFIER_LAYOUT_BYTES)
        .deleteColumns(mFamilyBytes, QUALIFIER_LAYOUT_ID_BYTES);
    mTable.delete(delete);
  }

  /** {@inheritDoc} */
  @Override
  public void removeRecentTableLayoutVersions(String table, int numVersions) throws IOException {
    Preconditions.checkArgument(numVersions >= 1, "numVersions must be positive");
    final Delete delete = new Delete(Bytes.toBytes(table));
    for (int i = 0; i < numVersions; i++) {
      delete
          .deleteColumn(mFamilyBytes, QUALIFIER_UPDATE_BYTES)
          .deleteColumn(mFamilyBytes, QUALIFIER_LAYOUT_BYTES)
          .deleteColumn(mFamilyBytes, QUALIFIER_LAYOUT_ID_BYTES);
    }
    mTable.delete(delete);
  }

  /** {@inheritDoc} */
  @Override
  public List<String> listTables() throws IOException {
    final Scan scan = new Scan()
        .addColumn(mFamilyBytes, QUALIFIER_LAYOUT_BYTES)
        .setMaxVersions(1);

    final ResultScanner resultScanner = mTable.getScanner(scan);

    final List<String> tableNames = Lists.newArrayList();
    for (Result result : resultScanner) {
      tableNames.add(Bytes.toString(result.getRow()));
    }
    ResourceUtils.closeOrLog(resultScanner);
    return tableNames;
  }

  /** {@inheritDoc} */
  @Override
  public boolean tableExists(String tableName) throws IOException {
    boolean retval = false;
    final Scan scan = new Scan()
        .addColumn(mFamilyBytes, QUALIFIER_LAYOUT_BYTES)
        .setMaxVersions(1);

    ResultScanner resultScanner = null;
    try {
      resultScanner = mTable.getScanner(scan);

      for (Result result : resultScanner) {
        if (tableName.equals(Bytes.toString(result.getRow()))) {
          retval = true;
          break;
        }
      }
    } finally {
      if (null != resultScanner) {
        resultScanner.close();
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
    return SchemaPlatformBridge.get().createHColumnDescriptorBuilder(Bytes.toBytes(family))
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
    Get get = new Get(Bytes.toBytes(table));
    get.addColumn(mFamilyBytes, QUALIFIER_UPDATE_BYTES)
       .addColumn(mFamilyBytes, QUALIFIER_LAYOUT_BYTES)
       .setMaxVersions();
    Result result = mTable.get(get);
       if (result.isEmpty()) {
         LOG.info(String.format("There is no row in the MetaTable named '%s'.", table));
       }
       final Map<byte[], NavigableMap<Long, byte[]>> qualifierMap =
           result.getMap().get(mFamilyBytes);
       final List<TableLayoutBackupEntry> history = Lists.newArrayList();
       if ((qualifierMap == null) || qualifierMap.isEmpty()) {
         LOG.info(String.format("Empty layout row for table '%s'.", table));
       } else {
         final Map<Long, byte[]> updateSerieMap = qualifierMap.get(QUALIFIER_UPDATE_BYTES);
         final Map<Long, byte[]> layoutSerieMap = qualifierMap.get(QUALIFIER_LAYOUT_BYTES);

         for (Map.Entry<Long, byte[]> serieEntry : layoutSerieMap.entrySet()) {
           final long timestamp = serieEntry.getKey();
           final TableLayoutDesc layout = decodeTableLayoutDesc(serieEntry.getValue());
           TableLayoutDesc update = null;
           if (updateSerieMap != null) {
             final byte[] bytes = updateSerieMap.get(timestamp);
             if (bytes != null) {
               update = decodeTableLayoutDesc(bytes);
             }
           }
           history.add(TableLayoutBackupEntry.newBuilder()
               .setLayout(layout)
               .setUpdate(update)
               .setTimestamp(timestamp)
               .build());
         }
       }
       TableLayoutsBackup backup = TableLayoutsBackup.newBuilder().setLayouts(history).build();
       return backup;
  }

  /** {@inheritDoc} */
  @Override
  public void restoreLayoutsFromBackup(String tableName, TableLayoutsBackup layoutBackup) throws
      IOException {
    LOG.info(String.format("Restoring layout history for table '%s'.", tableName));
    for (TableLayoutBackupEntry lbe : layoutBackup.getLayouts()) {
      final byte[] layoutBytes = encodeTableLayoutDesc(lbe.getLayout());
      final Put put = new Put(Bytes.toBytes(tableName))
          .add(mFamilyBytes, QUALIFIER_LAYOUT_BYTES, layoutBytes);
      if (lbe.getUpdate() != null) {
        final byte[] updateBytes = encodeTableLayoutDesc(lbe.getUpdate());
        final long timestamp = lbe.getTimestamp();
        put.add(mFamilyBytes, QUALIFIER_UPDATE_BYTES, timestamp, updateBytes);
      }
      mTable.put(put);
    }
    mTable.flushCommits();
  }

  /**
   * Decodes a table layout descriptor from binary.
   *
   * @param bytes Serialized table layout descriptor.
   * @return Deserialized table layout descriptor.
   * @throws IOException on I/O or decoding error.
   */
  private TableLayoutDesc decodeTableLayoutDesc(byte[] bytes) throws IOException {
    return mCellDecoder.decodeValue(bytes);
  }

  /**
   * Encodes a table layout descriptor to binary.
   *
   * @param desc Table layout descriptor to serialize.
   * @return Table layout descriptor encoded as bytes.
   * @throws IOException on I/O or encoding error.
   */
  private byte[] encodeTableLayoutDesc(TableLayoutDesc desc) throws IOException {
    return mCellEncoder.encode(desc);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(HBaseTableLayoutDatabase.class)
        .add("uri", mKijiURI)
        .add("family", mFamily)
        .toString();
  }
}
