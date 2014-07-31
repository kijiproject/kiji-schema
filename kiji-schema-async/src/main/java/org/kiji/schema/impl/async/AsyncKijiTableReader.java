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

package org.kiji.schema.impl.async;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestValidator;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReaderBuilder;
import org.kiji.schema.KijiTableReaderBuilder.OnDecoderCacheMiss;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.SpecificCellDecoderFactory;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.impl.BoundColumnReaderSpec;
import org.kiji.schema.impl.KijiResultRowData;
import org.kiji.schema.impl.KijiResultRowScanner;
import org.kiji.schema.impl.LayoutConsumer;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.ColumnReaderSpec;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.util.DebugResourceTracker;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

/**
 * Reads from a kiji table by sending the requests directly to the HBase tables.
 */
@ApiAudience.Private
public final class AsyncKijiTableReader implements KijiTableReader {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncKijiTableReader.class);

  /** HBase KijiTable to read from. */
  private final AsyncKijiTable mTable;
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

  /** Layout consumer registration resource. */
  private final LayoutConsumer.Registration mLayoutConsumerRegistration;

  /** Shared HBaseClient connection. */
  private final HBaseClient mHBClient;

  /** HBase table name */
  private final byte[] mTableName;

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
    private final HBaseColumnNameTranslator mTranslator;

    /**
     * Default constructor.
     *
     * @param cellDecoderProvider the CellDecoderProvider to cache.  This provider should reflect
     *     all overrides appropriate to this reader.
     * @param layout the KijiTableLayout to cache.
     * @param translator the HBaseColumnNameTranslator to cache.
     */
    private ReaderLayoutCapsule(
        final CellDecoderProvider cellDecoderProvider,
        final KijiTableLayout layout,
        final HBaseColumnNameTranslator translator) {
      mCellDecoderProvider = cellDecoderProvider;
      mLayout = layout;
      mTranslator = translator;
    }

    /**
     * Get the column name translator for the current layout.
     * @return the column name translator for the current layout.
     */
    private HBaseColumnNameTranslator getColumnNameTranslator() {
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
    public void update(KijiTableLayout layout) throws IOException {
      if (mState.get() == State.CLOSED) {
        LOG.debug("KijiTableReader instance is closed; ignoring layout update.");
        return;
      }
      final CellDecoderProvider provider;
      if (null != mCellSpecOverrides) {
        provider = CellDecoderProvider.create(
            layout,
            mTable.getKiji().getSchemaTable(),
            SpecificCellDecoderFactory.get(),
            mCellSpecOverrides);
      } else {
        provider = CellDecoderProvider.create(
            layout,
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
            layout.getDesc().getLayoutId());
      } else {
        // If the capsule is null this is the initial setup and we need a different log message.
        LOG.debug("Initializing KijiTableReader: {} for table: {} with table layout version: {}",
            this,
            mTable.getURI(),
            layout.getDesc().getLayoutId());
      }
      mReaderLayoutCapsule = new ReaderLayoutCapsule(
          provider,
          layout,
          HBaseColumnNameTranslator.from(layout));
    }
  }

  /**
   * Creates a new <code>AsyncKijiTableReader</code> instance that sends the read requests
   * directly to HBase.
   *
   * @param table Kiji table from which to read.
   * @throws IOException on I/O error.
   * @return a new AsyncKijiTableReader.
   */
  public static AsyncKijiTableReader create(
      final AsyncKijiTable table
  ) throws IOException {
    return AsyncKijiTableReaderBuilder.create(table).buildAndOpen();
  }

  /**
   * Creates a new <code>AsyncKijiTableReader</code> instance that sends read requests directly to
   * HBase.
   *
   * @param table Kiji table from which to read.
   * @param overrides layout overrides to modify read behavior.
   * @return a new AsyncKijiTableReader.
   * @throws IOException in case of an error opening the reader.
   * @deprecated use {@link #createWithOptions}.
   */
  @Deprecated
  public static AsyncKijiTableReader createWithCellSpecOverrides(
      final AsyncKijiTable table,
      final Map<KijiColumnName, CellSpec> overrides
  ) throws IOException {
    return new AsyncKijiTableReader(table, overrides);
  }

  /**
   * Create a new <code>AsyncKijiTableReader</code> instance that sends read requests directly to
   * HBase.
   *
   * @param table Kiji table from which to read.
   * @param onDecoderCacheMiss behavior to use when a {@link ColumnReaderSpec} override
   *     specified in a {@link KijiDataRequest} cannot be found in the prebuilt cache of cell
   *     decoders.
   * @param overrides mapping from columns to overriding read behavior for those columns.
   * @param alternatives mapping from columns to reader spec alternatives which the
   *     KijiTableReader will accept as overrides in data requests.
   * @return a new AsyncKijiTableReader.
   * @throws IOException in case of an error opening the reader.
   */
  public static AsyncKijiTableReader createWithOptions(
      final AsyncKijiTable table,
      final OnDecoderCacheMiss onDecoderCacheMiss,
      final Map<KijiColumnName, ColumnReaderSpec> overrides,
      final Multimap<KijiColumnName, ColumnReaderSpec> alternatives
  ) throws IOException {
    return new AsyncKijiTableReader(table, onDecoderCacheMiss, overrides, alternatives);
  }

  /**
   * Open a table reader whose behavior is customized by overriding CellSpecs.
   *
   * @param table Kiji table from which this reader will read.
   * @param cellSpecOverrides specifications of overriding read behaviors.
   * @throws IOException in case of an error opening the reader.
   */
  private AsyncKijiTableReader(
      final AsyncKijiTable table,
      final Map<KijiColumnName, CellSpec> cellSpecOverrides
  ) throws IOException {
    mTable = table;
    mCellSpecOverrides = cellSpecOverrides;
    mOnDecoderCacheMiss = KijiTableReaderBuilder.DEFAULT_CACHE_MISS;
    mOverrides = null;
    mAlternatives = null;

    mLayoutConsumerRegistration = mTable.registerLayoutConsumer(new InnerLayoutUpdater());
    Preconditions.checkState(mReaderLayoutCapsule != null,
        "KijiTableReader for table: %s failed to initialize.", mTable.getURI());

    mHBClient = table.getHBClient();
    mTableName = KijiManagedHBaseTableName
        .getKijiTableName(mTable.getURI().getInstance(), mTable.getURI().getTable()).toBytes();

    // Retain the table only when everything succeeds.
    mTable.retain();
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open KijiTableReader instance in state %s.", oldState);
    DebugResourceTracker.get().registerResource(this);
  }

  /**
   * Creates a new <code>AsyncKijiTableReader</code> instance that sends read requests directly to
   * HBase.
   *
   * @param table Kiji table from which to read.
   * @param onDecoderCacheMiss behavior to use when a {@link ColumnReaderSpec} override
   *     specified in a {@link KijiDataRequest} cannot be found in the prebuilt cache of cell
   *     decoders.
   * @param overrides mapping from columns to overriding read behavior for those columns.
   * @param alternatives mapping from columns to reader spec alternatives which the
   *     KijiTableReader will accept as overrides in data requests.
   * @throws IOException on I/O error.
   */
  private AsyncKijiTableReader(
      final AsyncKijiTable table,
      final OnDecoderCacheMiss onDecoderCacheMiss,
      final Map<KijiColumnName, ColumnReaderSpec> overrides,
      final Multimap<KijiColumnName, ColumnReaderSpec> alternatives
  ) throws IOException {
    mTable = table;
    mOnDecoderCacheMiss = onDecoderCacheMiss;

    final KijiTableLayout layout = mTable.getLayout();
    final Set<KijiColumnName> layoutColumns = layout.getColumnNames();
    final Map<KijiColumnName, BoundColumnReaderSpec> boundOverrides = Maps.newHashMap();
    for (Map.Entry<KijiColumnName, ColumnReaderSpec> override
        : overrides.entrySet()) {
      final KijiColumnName column = override.getKey();
      if (!layoutColumns.contains(column)
          && !layoutColumns.contains(KijiColumnName.create(column.getFamily()))) {
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
        : alternatives.entries()) {
      final KijiColumnName column = altsEntry.getKey();
      if (!layoutColumns.contains(column)
          && !layoutColumns.contains(KijiColumnName.create(column.getFamily()))) {
        throw new NoSuchColumnException(String.format(
            "KijiTableLayout: %s does not contain column: %s", layout, column));
      } else {
        boundAlternatives.add(
            BoundColumnReaderSpec.create(altsEntry.getValue(), altsEntry.getKey()));
      }
    }
    mAlternatives = boundAlternatives;
    mCellSpecOverrides = null;

    mHBClient = table.getHBClient();
    mTableName = KijiManagedHBaseTableName
        .getKijiTableName(mTable.getURI().getInstance(), mTable.getURI().getTable()).toBytes();

    mLayoutConsumerRegistration = mTable.registerLayoutConsumer(new InnerLayoutUpdater());
    Preconditions.checkState(mReaderLayoutCapsule != null,
        "KijiTableReader for table: %s failed to initialize.", mTable.getURI());

    // Retain the table only when everything succeeds.
    mTable.retain();
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open KijiTableReader instance in state %s.", oldState);
    DebugResourceTracker.get().registerResource(this);
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowData get(EntityId entityId, KijiDataRequest dataRequest)
      throws IOException {
    return new KijiResultRowData(mTable.getLayout(), getResult(entityId, dataRequest));
  }

  /**
   * Get a KijiResult for the given EntityId and data request.
   *
   * <p>
   *   This method allows the caller to specify a type-bound on the values of the {@code KijiCell}s
   *   of the returned {@code KijiResult}. The caller should be careful to only specify an
   *   appropriate type. If the type is too specific (or wrong), a runtime
   *   {@link java.lang.ClassCastException} will be thrown when the returned {@code KijiResult} is
   *   used. See the 'Type Safety' section of {@link KijiResult}'s documentation for more details.
   * </p>
   *
   * @param entityId EntityId of the row from which to get data.
   * @param dataRequest Specification of the data to get from the given row.
   * @param <T> type {@code KijiCell} value returned by the {@code KijiResult}.
   * @return a new KijiResult for the given EntityId and data request.
   * @throws IOException in case of an error getting the data.
   */
  public <T> KijiResult<T> getResult(
      final EntityId entityId,
      final KijiDataRequest dataRequest
  ) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get row from KijiTableReader instance %s in state %s.", this, state);
    final ReaderLayoutCapsule capsule = mReaderLayoutCapsule;
    final KijiTableLayout tableLayout = capsule.getLayout();
    validateRequestAgainstLayout(dataRequest, tableLayout);
    final AsyncDataRequestAdapter asyncDataRequestAdapter = AsyncDataRequestAdapter.create(
        dataRequest,
        capsule.getColumnNameTranslator(),
        mHBClient,
        mTableName);
    final Scanner scanner = asyncDataRequestAdapter.toScanner(tableLayout);
    scanner.setStartKey(entityId.getHBaseRowKey());
    scanner.setStopKey(
        Arrays.copyOf(entityId.getHBaseRowKey(), entityId.getHBaseRowKey().length + 1));
    ArrayList<KeyValue> result = Lists.newArrayList();
    try {
      final ArrayList<ArrayList<KeyValue>> fullResults = scanner.nextRows(1).join();
      if (null != fullResults && !fullResults.isEmpty()) {
        result = fullResults.get(0);
      }
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }
    return AsyncKijiResult.create(
        entityId,
        dataRequest,
        result,
        mTable,
        capsule.getLayout(),
        capsule.getColumnNameTranslator(),
        capsule.getCellDecoderProvider());
  }

  /** {@inheritDoc} */
  @Override
  public List<KijiRowData> bulkGet(List<EntityId> entityIds, KijiDataRequest dataRequest)
      throws IOException {
    // TODO: Implement with AsyncHBase
    throw new UnsupportedOperationException("Not yet supported by AsyncKiji");
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
    return new KijiResultRowScanner(
        mTable.getLayout(),
        getKijiResultScanner(dataRequest, kijiScannerOptions));
  }

  /**
   * Get a KijiResultScanner for the given data request and scan options.
   *
   * <p>
   *   This method allows the caller to specify a type-bound on the values of the {@code KijiCell}s
   *   of the returned {@code KijiResult}s. The caller should be careful to only specify an
   *   appropriate type. If the type is too specific (or wrong), a runtime
   *   {@link java.lang.ClassCastException} will be thrown when the returned {@code KijiResult} is
   *   used. See the 'Type Safety' section of {@code KijiResult}'s documentation for more details.
   * </p>
   *
   * @param request Data request defining the data to retrieve from each row.
   * @param scannerOptions Options to control the operation of the scanner.
   * @param <T> type {@code KijiCell} value returned by the {@code KijiResult}.
   * @return A new KijiResultScanner.
   * @throws IOException in case of an error creating the scanner.
   */
  public <T> AsyncKijiResultScanner<T> getKijiResultScanner(
      final KijiDataRequest request,
      final KijiScannerOptions scannerOptions
  ) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get scanner from KijiTableReader instance %s in state %s.", this, state);

    final ReaderLayoutCapsule capsule = mReaderLayoutCapsule;
    final AsyncDataRequestAdapter adapter = AsyncDataRequestAdapter.create(
            request,
            capsule.getColumnNameTranslator(),
            mHBClient,
            mTableName);
    final KijiTableLayout layout = capsule.getLayout();
    validateRequestAgainstLayout(request, layout);
    final Scanner scanner = adapter.toScanner(layout, scannerOptions.getHBaseScanOptions());
    if (null != scannerOptions.getStartRow()) {
      scanner.setStartKey(scannerOptions.getStartRow().getHBaseRowKey());
    }
    if (null != scannerOptions.getStopRow()) {
      scanner.setStopKey(scannerOptions.getStopRow().getHBaseRowKey());
    }
    if (scannerOptions.getRowCaching() > 0) {
      scanner.setMaxNumRows(scannerOptions.getRowCaching());
    }

    // TODO(SCHEMA-890): Add ability to use KijiRowFilters with AsyncHBase.
    if (null != scannerOptions.getKijiRowFilter()) {
      throw new UnsupportedOperationException("KijiRowFilters are not supported by AsyncKiji");
    }

    return new AsyncKijiResultScanner<T>(
        request,
        mTable,
        scanner,
        scannerOptions.getStartRow(),
        scannerOptions.getStopRow(),
        capsule.getLayout(),
        capsule.getCellDecoderProvider(),
        capsule.getColumnNameTranslator(),
        scannerOptions.getReopenScannerOnTimeout());
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(AsyncKijiTableReader.class)
        .add("id", System.identityHashCode(this))
        .add("table", mTable.getURI())
        .add("layout-version", mReaderLayoutCapsule.getLayout().getDesc().getLayoutId())
        .add("state", mState.get())
        .toString();
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
    DebugResourceTracker.get().unregisterResource(this);
    mLayoutConsumerRegistration.close();
    mTable.release();
  }
}
