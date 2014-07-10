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
package org.kiji.schema.layout.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.DecoderNotFoundException;
import org.kiji.schema.GenericCellDecoderFactory;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.KijiCellDecoderFactory;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiTableReaderBuilder;
import org.kiji.schema.KijiTableReaderBuilder.OnDecoderCacheMiss;
import org.kiji.schema.SpecificCellDecoderFactory;
import org.kiji.schema.impl.BoundColumnReaderSpec;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.ColumnReaderSpec;
import org.kiji.schema.layout.ColumnReaderSpec.AvroDecoderType;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Provider for cell decoders of a given table.
 *
 * <p>
 *   Cell decoders for all columns in the table are pro-actively created when the
 *   CellDecoderProvider is constructed.
 *   Cell decoders are cached and reused.
 * </p>
 * <p>
 *   At construction time, cell decoders may be customized by specifying BoundColumnReaderSpec
 *   instances to overlay on top of the actual table layout, using the constructor:
 *   {@link #create(KijiTableLayout, Map, Collection, KijiTableReaderBuilder.OnDecoderCacheMiss)}.
 * </p>
 * <p>
 *   CellSpec customizations include:
 *   <ul>
 *     <li> choosing between generic and specific Avro records. </li>
 *     <li> choosing a different Avro reader schema. </li>
 *     <li> using the Avro writer schema (this forces using generic records). </li>
 *   </ul>
 * </p>
 *
 * <h2>Thread Safety</h2>
 * <p>
 *   {@code CellDecoderProvider} is not thread safe.
 * </p>
 */
@ApiAudience.Private
public final class CellDecoderProvider {
  private static final Logger LOG = LoggerFactory.getLogger(CellDecoderProvider.class);

  /** Layout of the table for which decoders are provided. */
  private final KijiTableLayout mLayout;

  /** Cell decoders for columns, including data request and table reader overrides. */
  private final ImmutableMap<KijiColumnName, KijiCellDecoder<?>> mColumnDecoders;

  /**
   * Cell decoders for alternative columns specified in
   * {@link #create(KijiTableLayout, Map, Collection, KijiTableReaderBuilder.OnDecoderCacheMiss)}.
   */
  private final Map<BoundColumnReaderSpec, KijiCellDecoder<?>> mReaderSpecDecoders;

  /** Behavior when a decoder cannot be found. */
  private final OnDecoderCacheMiss mOnDecoderCacheMiss;

  /**
   * Constructor for {@code CellDecoderProvider}s.
   *
   * @param layout {@code KijiTableLayout} of the cell decoder provider's table.
   * @param columnDecoders Individual cell decoders for the columns of the table.
   * @param readerSpecDecoders Alternative cell decoders for columns of the table.
   * @param onDecoderCacheMiss Determines behavior when a decoder cannot be found.
   */
  private CellDecoderProvider(
      final KijiTableLayout layout,
      final ImmutableMap<KijiColumnName, KijiCellDecoder<?>> columnDecoders,
      final Map<BoundColumnReaderSpec, KijiCellDecoder<?>> readerSpecDecoders,
      final OnDecoderCacheMiss onDecoderCacheMiss
  ) {
    mLayout = layout;
    mColumnDecoders = columnDecoders;
    mReaderSpecDecoders = readerSpecDecoders;
    mOnDecoderCacheMiss = onDecoderCacheMiss;
  }

  /**
   * Initialize a provider for cell decoders.
   *
   * @param layout the layout for which to provide decoders.
   * @param schemaTable the schema table from which to retrieve cell schemas.
   * @param factory Default factory for cell decoders.
   * @param overrides Column specification overlay/override map.
   *     Specifications from this map override the actual specification from the table.
   * @throws IOException in case of an error creating the cached decoders.
   * @return a new {@code CellDecoderProvider}.
   */
  public static CellDecoderProvider create(
      final KijiTableLayout layout,
      final KijiSchemaTable schemaTable,
      final KijiCellDecoderFactory factory,
      final Map<KijiColumnName, CellSpec> overrides
  ) throws IOException {
    // Note: nothing prevents one from overriding the specification for one specific qualifier
    // in a map-type family.
    final Set<KijiColumnName> columns =
        Sets.newHashSet(Iterables.concat(overrides.keySet(), layout.getColumnNames()));

    // Pro-actively build cell decoders for all columns in the table:
    final ImmutableMap.Builder<KijiColumnName, KijiCellDecoder<?>> decoderMap =
        ImmutableMap.builder();
    for (KijiColumnName column : columns) {
      // Gets the specification for this column,
      // from the overlay map or else from the actual table layout:
      CellSpec cellSpec = overrides.get(column);
      if (null == cellSpec) {
        cellSpec = layout.getCellSpec(column);
      } else {
        // Deep-copy the user-provided CellSpec:
        cellSpec = CellSpec.copy(cellSpec);
      }

      // Fills in the missing details to build the decoder:
      if (cellSpec.getSchemaTable() == null) {
        cellSpec.setSchemaTable(schemaTable);
      }
      if (cellSpec.getDecoderFactory() == null) {
        cellSpec.setDecoderFactory(factory);
      }

      final KijiCellDecoder<?> decoder = cellSpec.getDecoderFactory().create(cellSpec);
      decoderMap.put(column, decoder);
    }
    return new CellDecoderProvider(
        layout,
        decoderMap.build(),
        Maps.<BoundColumnReaderSpec, KijiCellDecoder<?>>newHashMap(),
        KijiTableReaderBuilder.DEFAULT_CACHE_MISS);
  }

  /**
   * Create a provider for cell decoders.
   *
   * @param layout the layout for which to provide decoders.
   * @param overrides Column specification overlay/override map. Specifications from this map
   *     override the actual specification from the table.
   * @param onDecoderCacheMiss behavior to use when a decoder cannot be found.
   * @param alternatives alternate column specifications for which decoders should be provided.
   * @throws IOException in case of an error creating the cached decoders.
   * @return a new {@code CellDecoderProvider}.
   */
  public static CellDecoderProvider create(
      final KijiTableLayout layout,
      final Map<KijiColumnName, BoundColumnReaderSpec> overrides,
      final Collection<BoundColumnReaderSpec> alternatives,
      final OnDecoderCacheMiss onDecoderCacheMiss
  ) throws IOException {
    // Pro-actively build cell decoders for all columns in the table and spec overrides:
    return new CellDecoderProvider(
        layout,
        makeColumnDecoderMap(layout, overrides),
        makeSpecDecoderMap(layout, overrides.values(), alternatives),
        onDecoderCacheMiss);
  }

  /**
   * Create a new {@link KijiCellDecoder} from a {@link BoundColumnReaderSpec}.
   *
   * @param layout {@code KijiTableLayout} from which storage information will be retrieved to build
   *     decoders.
   * @param spec specification of column read properties from which to build a cell decoder.
   * @return a new cell decoder based on the specification.
   * @throws IOException in case of an error making the decoder.
   */
  private static KijiCellDecoder<?> createDecoderFromSpec(
      final KijiTableLayout layout,
      final BoundColumnReaderSpec spec
  ) throws IOException {
    final AvroDecoderType decoderType = spec.getColumnReaderSpec().getAvroDecoderType();
    if (null != decoderType) {
      switch (decoderType) {
        case GENERIC: {
          return GenericCellDecoderFactory.get().create(layout, spec);
        }
        case SPECIFIC: {
          return SpecificCellDecoderFactory.get().create(layout, spec);
        }
        default: throw new InternalKijiError("Unknown decoder type: " + decoderType);
      }
    } else {
      // If the decoder type is null, we can use the generic factory.
      return GenericCellDecoderFactory.get().create(layout, spec);
    }
  }

  /**
   * Build a map of {@link BoundColumnReaderSpec} to {@link KijiCellDecoder} from a collection of
   * specs.
   *
   * All columns in overrides and alternatives are assumed to be included in the table layout
   * because of prior validation.
   *
   * @param layout KijiTableLayout from which storage information will be retrieved to build
   *     decoders.
   * @param overrides specifications of column read properties from which to build decoders.
   * @param alternatives further specifications of column reader properties from which to build
   *     decoders.
   * @return a map from specification to decoders which follow those specifications.
   * @throws IOException in case of an error making decoders.
   */
  private static Map<BoundColumnReaderSpec, KijiCellDecoder<?>> makeSpecDecoderMap(
      final KijiTableLayout layout,
      final Collection<BoundColumnReaderSpec> overrides,
      final Collection<BoundColumnReaderSpec> alternatives
  ) throws IOException {
    final Map<BoundColumnReaderSpec, KijiCellDecoder<?>> decoderMap = Maps.newHashMap();
    for (BoundColumnReaderSpec spec : overrides) {
      Preconditions.checkState(null == decoderMap.put(spec, createDecoderFromSpec(layout, spec)));
    }
    for (BoundColumnReaderSpec spec : alternatives) {
      Preconditions.checkState(null == decoderMap.put(spec, createDecoderFromSpec(layout, spec)));
    }

    return decoderMap;
  }

  /**
   * Build a map of column names to {@link KijiCellDecoder} for all columns in a given layout and
   * set of overrides.
   *
   * All columns in overrides are assumed to be included in the table layout because of prior
   * validation.
   *
   * @param layout layout from which to get column names and column specifications.
   * @param overrides overridden column read properties.
   * @return a map from all columns in a table and overrides to decoders for those columns.
   * @throws IOException in case of an error making decoders.
   */
  private static ImmutableMap<KijiColumnName, KijiCellDecoder<?>> makeColumnDecoderMap(
      final KijiTableLayout layout,
      final Map<KijiColumnName, BoundColumnReaderSpec> overrides
  ) throws IOException {
    final Set<KijiColumnName> columns = layout.getColumnNames();
    final ImmutableMap.Builder<KijiColumnName, KijiCellDecoder<?>> decoderMap =
        ImmutableMap.builder();
    for (KijiColumnName column : columns) {
      // Gets the specification for this column,
      // from the overlay map or else from the actual table layout:
      final BoundColumnReaderSpec spec = overrides.get(column);
      if (null != spec) {
        decoderMap.put(column, createDecoderFromSpec(layout, spec));
      } else {
        final CellSpec cellSpec = layout.getCellSpec(column);
        decoderMap.put(column, cellSpec.getDecoderFactory().create(cellSpec));
      }
    }
    return decoderMap.build();
  }

  /**
   * Get a decoder from a {@link BoundColumnReaderSpec}. Creates a new decoder if one does not
   * already exist.
   *
   * @param spec specification of column read properties from which to get a decoder.
   * @param <T> the type of the value encoded in the cell.
   * @return a new or cached cell decoder corresponding to the given specification.
   * @throws IOException in case of an error create a new decoder.
   */
  @SuppressWarnings("unchecked")
  private <T> KijiCellDecoder<T> getDecoder(BoundColumnReaderSpec spec) throws IOException {
    final KijiCellDecoder<T> decoder = (KijiCellDecoder<T>) mReaderSpecDecoders.get(spec);
    if (null != decoder) {
      return decoder;
    } else {
      switch (mOnDecoderCacheMiss) {
        case FAIL: {
          throw new DecoderNotFoundException(
              "Could not find cell decoder for BoundColumnReaderSpec: " + spec);
        }
        case BUILD_AND_CACHE: {
          LOG.debug(
              "Building and caching new cell decoder from ColumnReaderSpec: {} for column: {}",
              spec.getColumnReaderSpec(), spec.getColumn());
          final KijiCellDecoder<T> newDecoder =
              (KijiCellDecoder<T>) createDecoderFromSpec(mLayout, spec);
          mReaderSpecDecoders.put(spec, newDecoder);
          return newDecoder;
        }
        case BUILD_DO_NOT_CACHE: {
          LOG.debug(
              "Building and not caching new cell decoder from ColumnReaderSpec: {} for column: {}",
              spec.getColumnReaderSpec(), spec.getColumn());
          return (KijiCellDecoder<T>) createDecoderFromSpec(mLayout, spec);
        }
        default: {
          throw new InternalKijiError("Unknown OnDecoderCacheMiss: " + mOnDecoderCacheMiss);
        }
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Public interface

  /**
   * Get a {@code CellDecoderProvider} with overrides applied from the provided request.
   *
   * @param request to overlay overrides from.
   * @return a {@code CellDecoderProvider} for the provided request.
   * @throws IOException on unrecoverable IO error.
   */
  public CellDecoderProvider getDecoderProviderForRequest(
      final KijiDataRequest request
  ) throws IOException {
    final List<BoundColumnReaderSpec> readerSpecs = Lists.newArrayList();
    for (Column columnRequest : request.getColumns()) {
      final ColumnReaderSpec readerSpec = columnRequest.getReaderSpec();
      if (readerSpec != null) {
        final KijiColumnName column = columnRequest.getColumnName();
        readerSpecs.add(BoundColumnReaderSpec.create(readerSpec, column));
      }
    }

    if (readerSpecs.isEmpty()) {
      return this;
    }

    final Map<KijiColumnName, KijiCellDecoder<?>> columnDecoders = Maps.newHashMap();
    columnDecoders.putAll(this.mColumnDecoders);

    for (BoundColumnReaderSpec readerSpec : readerSpecs) {
      columnDecoders.put(readerSpec.getColumn(), getDecoder(readerSpec));
    }

    return new CellDecoderProvider(
        mLayout,
        ImmutableMap.copyOf(columnDecoders),
        mReaderSpecDecoders,
        mOnDecoderCacheMiss);
  }

  /**
   * Gets a cell decoder for the specified column or (map-type) family.
   *
   * <p>
   *   When requesting a decoder for a column within a map-type family, the decoder for the
   *   entire map-type family will be returned unless an override has been specified for the
   *   exact fully-qualified column.
   * </p>
   *
   * @param column to look up.
   * @param <T> Type of the data to decode.
   * @return a cell decoder for the specified column. {@code null} if the column does not exist or
   *     if the family is not map-type.
   */
  @SuppressWarnings("unchecked")
  public <T> KijiCellDecoder<T> getDecoder(final KijiColumnName column) {
    final KijiCellDecoder<T> decoder = (KijiCellDecoder<T>) mColumnDecoders.get(column);
    if (decoder != null) {
      // There already exists a decoder for this column:
      return decoder;
    }

    if (column.isFullyQualified()) {
      // There is no decoder for the specified fully-qualified column.
      // Try the family (this will only work for map-type families):
      return getDecoder(KijiColumnName.create(column.getFamily(), null));
    }

    return null;
  }
}
