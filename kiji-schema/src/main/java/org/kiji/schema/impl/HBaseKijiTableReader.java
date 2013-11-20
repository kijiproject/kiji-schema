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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestValidator;
import org.kiji.schema.KijiReaderFactory;
import org.kiji.schema.KijiReaderFactory.KijiTableReaderOptions;
import org.kiji.schema.KijiReaderFactory.KijiTableReaderOptions.OnDecoderCacheMiss;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.SpecificCellDecoderFactory;
import org.kiji.schema.filter.KijiRowFilter;
import org.kiji.schema.filter.KijiRowFilterApplicator;
import org.kiji.schema.hbase.HBaseScanOptions;
import org.kiji.schema.impl.HBaseKijiTable.LayoutCapsule;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.ColumnReaderSpec;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.layout.impl.ColumnNameTranslator;

/**
 * Reads from a kiji table by sending the requests directly to the HBase tables.
 */
@ApiAudience.Private
public final class HBaseKijiTableReader implements KijiTableReader {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseKijiTableReader.class);

  /** HBase KijiTable to read from. */
  private final HBaseKijiTable mTable;
  /** Behavior when a cell decoder cannot be found. */
  private final OnDecoderCacheMiss mOnDecoderCacheMiss;

  /** States of a kiji table reader instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this KijiTableReader instance. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** Map of overridden CellSpecs to use when reading. Null when mOverrides is not null. */
  private final Map<KijiColumnName, CellSpec> mCellSpecOverrides;

  /** Map of overridden column read specifications. Null when mCellSpecOverrides is not null. */
  private final Map<KijiColumnName, BoundColumnReaderSpec> mOverrides;

  /** Map of backup column read specifications. Null when mCellSpecOverrides is not null. */
  private final Collection<BoundColumnReaderSpec> mAlternatives;

  /** Object which processes layout update from the KijiTable from which this Reader reads. */
  private final InnerLayoutUpdater mInnerLayoutUpdater = new InnerLayoutUpdater();

  /**
   * Encapsulation of all table layout related state necessary for the operation of this reader.
   * Can be hot swapped to reflect a table layout update.
   */
  private ReaderLayoutCapsule mReaderLayoutCapsule = null;

  /**
   * Container class encapsulating all reader state which must be updated in response to a table
   * layout update.
   */
  private static final class ReaderLayoutCapsule {
    private final CellDecoderProvider mCellDecoderProvider;
    private final KijiTableLayout mLayout;
    private final ColumnNameTranslator mTranslator;

    /**
     * Default constructor.
     *
     * @param cellDecoderProvider the CellDecoderProvider to cache.  This provider should reflect
     *     all overrides appropriate to this reader.
     * @param layout the KijiTableLayout to cache.
     * @param translator the ColumnNameTranslator to cache.
     */
    private ReaderLayoutCapsule(
        final CellDecoderProvider cellDecoderProvider,
        final KijiTableLayout layout,
        final ColumnNameTranslator translator) {
      mCellDecoderProvider = cellDecoderProvider;
      mLayout = layout;
      mTranslator = translator;
    }

    /**
     * Get the column name translator for the current layout.
     * @return the column name translator for the current layout.
     */
    private ColumnNameTranslator getColumnNameTranslator() {
      return mTranslator;
    }

    /**
     * Get the current table layout for the table to which this reader is associated.
     * @return the current table layout for the table to which this reader is associated.
     */
    private KijiTableLayout getLayout() {
      return mLayout;
    }

    /**
     * Get the CellDecoderProvider including CellSpec overrides for providing cell decoders for the
     * current layout.
     * @return the CellDecoderProvider including CellSpec overrides for providing cell decoders for
     * the current layout.
     */
    private CellDecoderProvider getCellDecoderProvider() {
      return mCellDecoderProvider;
    }
  }

  /** Provides for the updating of this Reader in response to a table layout update. */
  private final class InnerLayoutUpdater implements LayoutConsumer {
    /** {@inheritDoc} */
    @Override
    public void update(LayoutCapsule capsule) throws IOException {
      final CellDecoderProvider provider;
      if (null != mCellSpecOverrides) {
        provider = new CellDecoderProvider(
            capsule.getLayout(),
            mTable.getKiji().getSchemaTable(),
            SpecificCellDecoderFactory.get(),
            mCellSpecOverrides);
      } else {
        provider = new CellDecoderProvider(
            capsule.getLayout(),
            mOverrides,
            mAlternatives,
            mOnDecoderCacheMiss);
      }
      if (mReaderLayoutCapsule != null) {
        LOG.debug(
            "Updating layout used by KijiTableReader: {} for table: {} from version: {} to: {}",
            this,
            mTable.getURI(),
            mReaderLayoutCapsule.getLayout().getDesc().getLayoutId(),
            capsule.getLayout().getDesc().getLayoutId());
      } else {
        // If the capsule is null this is the initial setup and we need a different log message.
        LOG.debug("Initializing KijiTableReader: {} for table: {} with table layout version: {}",
            this,
            mTable.getURI(),
            capsule.getLayout().getDesc().getLayoutId());
      }
      mReaderLayoutCapsule = new ReaderLayoutCapsule(
          provider,
          capsule.getLayout(),
          capsule.getColumnNameTranslator());
    }
  }

  /**
   * Creates a new <code>HBaseKijiTableReader</code> instance that sends the read requests
   * directly to HBase.
   *
   * @param table Kiji table from which to read.
   * @throws IOException on I/O error.
   * @return a new HBaseKijiTableReader.
   */
  public static HBaseKijiTableReader create(
      final HBaseKijiTable table
  ) throws IOException {
    return new HBaseKijiTableReader(table, KijiTableReaderOptions.ALL_DEFAULTS);
  }

  /**
   * Creates a new <code>HbaseKijiTableReader</code> instance that sends read requests directly to
   * HBase.
   *
   * @param table Kiji table from which to read.
   * @param overrides layout overrides to modify read behavior.
   * @return a new HBaseKijiTableReader.
   * @throws IOException in case of an error opening the reader.
   */
  public static HBaseKijiTableReader createWithCellSpecOverrides(
      final HBaseKijiTable table,
      final Map<KijiColumnName, CellSpec> overrides
  ) throws IOException {
    return new HBaseKijiTableReader(table, overrides);
  }

  /**
   * Create a new <code>HBaseKijiTableReader</code> instance that sends read requests directly to
   * HBase.
   *
   * @param table Kiji table from which to read.
   * @param options configuration options for this KijiTableReader.
   * @return a new HBaseKijiTableReader.
   * @throws IOException in case of an error opening the reader.
   */
  public static HBaseKijiTableReader createWithOptions(
      final HBaseKijiTable table,
      final KijiTableReaderOptions options
  ) throws IOException {
    return new HBaseKijiTableReader(table, options);
  }

  /**
   * Open a table reader whose behavior is customized by overriding CellSpecs.
   *
   * @param table Kiji table from which this reader will read.
   * @param cellSpecOverrides specifications of overriding read behaviors.
   * @throws IOException in case of an error opening the reader.
   */
  private HBaseKijiTableReader(
      final HBaseKijiTable table,
      final Map<KijiColumnName, CellSpec> cellSpecOverrides
  ) throws IOException {
    mTable = table;
    mCellSpecOverrides = cellSpecOverrides;
    mOnDecoderCacheMiss = KijiReaderFactory.KijiTableReaderOptions.Builder.DEFAULT_CACHE_MISS;
    mOverrides = null;
    mAlternatives = null;

    mTable.registerLayoutConsumer(mInnerLayoutUpdater);
    Preconditions.checkState(mReaderLayoutCapsule != null,
        "KijiTableReader for table: %s failed to initialize.", mTable.getURI());

    // Retain the table only when everything succeeds.
    mTable.retain();
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open KijiTableReader instance in state %s.", oldState);
  }

  /**
   * Creates a new <code>HBaseKijiTableReader</code> instance that sends read requests directly to
   * HBase.
   *
   * @param table Kiji table from which to read.
   * @param options configuration options for this KijiTableReader.
   * @throws IOException on I/O error.
   */
  private HBaseKijiTableReader(
      final HBaseKijiTable table,
      final KijiTableReaderOptions options
  ) throws IOException {
    mTable = table;
    mOnDecoderCacheMiss = options.getOnDecoderCacheMiss();

    final KijiTableLayout layout = mTable.getLayout();
    final Set<KijiColumnName> layoutColumns = layout.getColumnNames();
    final Map<KijiColumnName, BoundColumnReaderSpec> boundOverrides = Maps.newHashMap();
    for (Map.Entry<KijiColumnName, ColumnReaderSpec> override
        : options.getColumnReaderSpecOverrides().entrySet()) {
      final KijiColumnName column = override.getKey();
      if (!layoutColumns.contains(column)
          && !layoutColumns.contains(new KijiColumnName(column.getFamily()))) {
        throw new NoSuchColumnException(String.format(
            "KijiTableLayout: %s does not contain column: %s", layout, column));
      } else {
        boundOverrides.put(column,
            BoundColumnReaderSpec.create(override.getValue(), column));
      }
    }
    mOverrides = boundOverrides;
    final Collection<BoundColumnReaderSpec> boundAlternatives = Sets.newHashSet();
    for (Map.Entry<KijiColumnName, ColumnReaderSpec> altsEntry
        : options.getColumnReaderSpecAlternatives().entries()) {
      final KijiColumnName column = altsEntry.getKey();
      if (!layoutColumns.contains(column)
          && !layoutColumns.contains(new KijiColumnName(column.getFamily()))) {
        throw new NoSuchColumnException(String.format(
            "KijiTableLayout: %s does not contain column: %s", layout, column));
      } else {
        boundAlternatives.add(
            BoundColumnReaderSpec.create(altsEntry.getValue(), altsEntry.getKey()));
      }
    }
    mAlternatives = boundAlternatives;
    mCellSpecOverrides = null;

    mTable.registerLayoutConsumer(mInnerLayoutUpdater);
    Preconditions.checkState(mReaderLayoutCapsule != null,
        "KijiTableReader for table: %s failed to initialize.", mTable.getURI());

    // Retain the table only when everything succeeds.
    mTable.retain();
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open KijiTableReader instance in state %s.", oldState);
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowData get(EntityId entityId, KijiDataRequest dataRequest)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get row from KijiTableReader instance %s in state %s.", this, state);

    final ReaderLayoutCapsule capsule = mReaderLayoutCapsule;
    // Make sure the request validates against the layout of the table.
    final KijiTableLayout tableLayout = capsule.getLayout();
    validateRequestAgainstLayout(dataRequest, tableLayout);

    // Construct an HBase Get to send to the HTable.
    HBaseDataRequestAdapter hbaseRequestAdapter =
        new HBaseDataRequestAdapter(dataRequest, capsule.getColumnNameTranslator());
    Get hbaseGet;
    try {
      hbaseGet = hbaseRequestAdapter.toGet(entityId, tableLayout);
    } catch (InvalidLayoutException e) {
      // The table layout should never be invalid at this point, since we got it from a valid
      // opened table.  If it is, there's something seriously wrong.
      throw new InternalKijiError(e);
    }
    // Send the HTable Get.
    final Result result = hbaseGet.hasFamilies() ? doHBaseGet(hbaseGet) : new Result();

    // Parse the result.
    return new HBaseKijiRowData(
        mTable, dataRequest, entityId, result, capsule.getCellDecoderProvider());
  }

  /** {@inheritDoc} */
  @Override
  public List<KijiRowData> bulkGet(List<EntityId> entityIds, KijiDataRequest dataRequest)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get rows from KijiTableReader instance %s in state %s.", this, state);

    // Bulk gets have some overhead associated with them,
    // so delegate work to get(EntityId, KijiDataRequest) if possible.
    if (entityIds.size() == 1) {
      return Collections.singletonList(this.get(entityIds.get(0), dataRequest));
    }
    final ReaderLayoutCapsule capsule = mReaderLayoutCapsule;
    final KijiTableLayout tableLayout = capsule.getLayout();
    validateRequestAgainstLayout(dataRequest, tableLayout);
    final HBaseDataRequestAdapter hbaseRequestAdapter =
        new HBaseDataRequestAdapter(dataRequest, capsule.getColumnNameTranslator());

    // Construct a list of hbase Gets to send to the HTable.
    final List<Get> hbaseGetList = makeGetList(entityIds, tableLayout, hbaseRequestAdapter);

    // Send the HTable Gets.
    final Result[] results = doHBaseGet(hbaseGetList);
    Preconditions.checkState(entityIds.size() == results.length);

    // Parse the results.  If a Result is null, then the corresponding KijiRowData should also
    // be null.  This indicates that there was an error retrieving this row.
    List<KijiRowData> rowDataList = parseResults(results, entityIds, dataRequest, tableLayout);

    return rowDataList;
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowScanner getScanner(KijiDataRequest dataRequest) throws IOException {
    return getScanner(dataRequest, new KijiScannerOptions());
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowScanner getScanner(
      KijiDataRequest dataRequest,
      KijiScannerOptions kijiScannerOptions)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get scanner from KijiTableReader instance %s in state %s.", this, state);

    try {
      EntityId startRow = kijiScannerOptions.getStartRow();
      EntityId stopRow = kijiScannerOptions.getStopRow();
      KijiRowFilter rowFilter = kijiScannerOptions.getKijiRowFilter();
      HBaseScanOptions scanOptions = kijiScannerOptions.getHBaseScanOptions();

      final ReaderLayoutCapsule capsule = mReaderLayoutCapsule;
      final HBaseDataRequestAdapter dataRequestAdapter =
          new HBaseDataRequestAdapter(dataRequest, capsule.getColumnNameTranslator());
      final KijiTableLayout tableLayout = capsule.getLayout();
      validateRequestAgainstLayout(dataRequest, tableLayout);
      final Scan scan = dataRequestAdapter.toScan(tableLayout, scanOptions);

      if (null != startRow) {
        scan.setStartRow(startRow.getHBaseRowKey());
      }
      if (null != stopRow) {
        scan.setStopRow(stopRow.getHBaseRowKey());
      }
      scan.setCaching(kijiScannerOptions.getRowCaching());

      if (null != rowFilter) {
        final KijiRowFilterApplicator applicator = KijiRowFilterApplicator.create(
            rowFilter, tableLayout, mTable.getKiji().getSchemaTable());
        applicator.applyTo(scan);
      }

      return new HBaseKijiRowScanner(new HBaseKijiRowScanner.Options()
          .withDataRequest(dataRequest)
          .withTable(mTable)
          .withScan(scan)
          .withCellDecoderProvider(capsule.getCellDecoderProvider())
          .withReopenScannerOnTimeout(kijiScannerOptions.getReopenScannerOnTimeout()));
    } catch (InvalidLayoutException e) {
      // The table layout should never be invalid at this point, since we got it from a valid
      // opened table.  If it is, there's something seriously wrong.
      throw new InternalKijiError(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(HBaseKijiTableReader.class)
        .add("id", System.identityHashCode(this))
        .add("table", mTable.getURI())
        .add("layout-version", mReaderLayoutCapsule.getLayout().getDesc().getLayoutId())
        .add("state", mState.get())
        .toString();
  }

  /**
   * Parses an array of hbase Results, returned from a bulk get, to a List of
   * KijiRowData.
   *
   * @param results The results to parse.
   * @param entityIds The matching set of EntityIds.
   * @param dataRequest The KijiDataRequest.
   * @param tableLayout The table layout.
   * @return The list of KijiRowData returned by these results.
   * @throws IOException If there is an error.
   */
  private List<KijiRowData> parseResults(Result[] results, List<EntityId> entityIds,
      KijiDataRequest dataRequest, KijiTableLayout tableLayout) throws IOException {
    List<KijiRowData> rowDataList = new ArrayList<KijiRowData>(results.length);

    for (int i = 0; i < results.length; i++) {
      Result result = results[i];
      EntityId entityId = entityIds.get(i);

      final HBaseKijiRowData rowData = (null == result)
          ? null
          : new HBaseKijiRowData(mTable, dataRequest, entityId, result,
                mReaderLayoutCapsule.getCellDecoderProvider());
      rowDataList.add(rowData);
    }
    return rowDataList;
  }

  /**
   * Creates a list of hbase Gets for a set of entityIds.
   *
   * @param entityIds The set of entityIds to collect.
   * @param tableLayout The table layout specifying constraints on what data to return for a row.
   * @param hbaseRequestAdapter The HBaseDataRequestAdapter.
   * @return A list of hbase Gets-- one for each entity id.
   * @throws IOException If there is an error.
   */
  private static List<Get> makeGetList(List<EntityId> entityIds, KijiTableLayout tableLayout,
      HBaseDataRequestAdapter hbaseRequestAdapter)
      throws IOException {
    List<Get> hbaseGetList = new ArrayList<Get>(entityIds.size());
    try {
      for (EntityId entityId : entityIds) {
        hbaseGetList.add(hbaseRequestAdapter.toGet(entityId, tableLayout));
      }
      return hbaseGetList;
    } catch (InvalidLayoutException ile) {
      // The table layout should never be invalid at this point, since we got it from a valid
      // opened table.  If it is, there's something seriously wrong.
      throw new InternalKijiError(ile);
    }
  }

  /**
   * Validate a data request against a table layout.
   *
   * @param dataRequest A KijiDataRequest.
   * @param layout the KijiTableLayout of the table against which to validate the data request.
   */
  private void validateRequestAgainstLayout(KijiDataRequest dataRequest, KijiTableLayout layout) {
    // TODO(SCHEMA-263): This could be made more efficient if the layout and/or validator were
    // cached.
    KijiDataRequestValidator.validatorForLayout(layout).validate(dataRequest);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close KijiTableReader instance %s in state %s.", this, oldState);
    mTable.unregisterLayoutConsumer(mInnerLayoutUpdater);
    mTable.release();
  }

  /**
   * Sends an HBase Get request.
   *
   * @param get HBase Get request.
   * @return the HBase Result.
   * @throws IOException on I/O error.
   */
  private Result doHBaseGet(Get get) throws IOException {
    final HTableInterface htable = mTable.openHTableConnection();
    try {
      LOG.debug("Sending HBase Get: {}", get);
      return htable.get(get);
    } finally {
      htable.close();
    }
  }

  /**
   * Sends a batch of HBase Get requests.
   *
   * @param get HBase Get requests.
   * @return the HBase Results.
   * @throws IOException on I/O error.
   */
  private Result[] doHBaseGet(List<Get> get) throws IOException {
    final HTableInterface htable = mTable.openHTableConnection();
    try {
      LOG.debug("Sending bulk HBase Get: {}", get);
      return htable.get(get);
    } finally {
      htable.close();
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    final State state = mState.get();
    if (state != State.CLOSED) {
      LOG.warn("Finalizing unclosed KijiTableReader {} in state {}.", this, state);
      close();
    }
    super.finalize();
  }
}
