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
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.KijiCellDecoderFactory;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiTable;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;

/**
 * Provider for cell decoders of a given table.
 *
 * <p>
 *   Cell decoders for all columns in the table are pro-actively created when the
 *   CellDecoderProvider is constructed.
 *   Cell decoders are cached and reused.
 * </p>
 * <p>
 *   At construction time, cell decoders may be customized by specifying CellSpec instances
 *   to overlay on top of the actual table layout, using the constructors:
 *   {@link #CellDecoderProvider(KijiTable, KijiCellDecoderFactory, Map)}.
 * </p>
 * <p>
 *   CellSpec customizations include:
 *   <ul>
 *     <li> choosing between generic and specific Avro records. </li>
 *     <li> choosing a different Avro reader schema. </li>
 *     <li> using the Avro writer schema (this forces using generic records). </li>
 *   </ul>
 * </p>
 */
@ApiAudience.Private
public final class CellDecoderProvider {

  /** Layout of the table to provide decoders for. */
  private final KijiTableLayout mLayout;

  /** Table to resolve Avro schemas. */
  private final KijiSchemaTable mSchemaTable;

  /** Default cell decoder factory (either specific or generic). */
  private final KijiCellDecoderFactory mCellDecoderFactory;

  /** Maps column names to decoders. */
  private final ImmutableMap<String, KijiCellDecoder<?>> mDecoderMap;

  /**
   * Initializes a provider for cell decoders.
   *
   * @param table Kiji table to provide cell decoders for.
   * @param cellDecoderFactory Default factory for cell decoders.
   * @param cellSpecs Column specification overlay/override map.
   *     Specifications from this map override the actual specification from the table.
   * @throws IOException on I/O error.
   */
  public CellDecoderProvider(
      KijiTable table,
      KijiCellDecoderFactory cellDecoderFactory,
      Map<KijiColumnName, CellSpec> cellSpecs)
      throws IOException {

    mLayout = table.getLayout();
    mSchemaTable = table.getKiji().getSchemaTable();
    mCellDecoderFactory = cellDecoderFactory;

    // Compute the set of all the column names (map-type families and fully-qualified columns).
    // Note: nothing prevents one from overriding the specification for one specific qualifier
    // in a map-type family.
    final Set<KijiColumnName> columns = Sets.newHashSet(cellSpecs.keySet());
    for (FamilyLayout fLayout : mLayout.getFamilies()) {
      if (fLayout.isMapType()) {
        columns.add(new KijiColumnName(fLayout.getName(), null));
      } else if (fLayout.isGroupType()) {
        for (ColumnLayout cLayout : fLayout.getColumns()) {
          columns.add(new KijiColumnName(fLayout.getName(), cLayout.getName()));
        }
      } else {
        throw new InternalKijiError(
            String.format("Family '%s' is neither map-type nor group-type.", fLayout.getName()));
      }
    }

    // Pro-actively build cell decoders for all columns in the table:
    final Map<String, KijiCellDecoder<?>> decoderMap = Maps.newHashMap();
    for (KijiColumnName column : columns) {
      // Gets the specification for this column,
      // from the overlay map or else from the actual table layout:
      CellSpec cellSpec = cellSpecs.get(column);
      if (null == cellSpec) {
        cellSpec = mLayout.getCellSpec(column);
      } else {
        // Deep-copy the user-provided CellSpec:
        cellSpec = CellSpec.copy(cellSpec);
      }

      // Fills in the missing details to build the decoder:
      cellSpec.setSchemaTable(mSchemaTable);
      if (cellSpec.getDecoderFactory() == null) {
        cellSpec.setDecoderFactory(mCellDecoderFactory);
      }

      final KijiCellDecoder<?> decoder = cellSpec.getDecoderFactory().create(cellSpec);
      decoderMap.put(column.getName(), decoder);
    }
    mDecoderMap = ImmutableMap.copyOf(decoderMap);
  }

  // -----------------------------------------------------------------------------------------------
  // Public interface

  /**
   * Gets a cell decoder for the specified column or (map-type) family.
   *
   * <p>
   *   When requesting a decoder for a column within a map-type family, the decoder for the
   *   entire map-type family will be returned unless an override has been specified for the
   *   exact fully-qualified column.
   * </p>
   *
   * @param family Family of the column to look up.
   * @param qualifier Qualifier of the column to look up.
   *     Null means no qualifier, ie. get a decoder for a (map-type) family.
   * @return a cell decoder for the specified column.
   *     Null if the column does not exist or if the family is not map-type.
   * @throws IOException on I/O error.
   *
   * @param <T> Type of the data to decode.
   */
  @SuppressWarnings("unchecked")
  public <T> KijiCellDecoder<T> getDecoder(String family, String qualifier) throws IOException {
    final String column = (qualifier != null) ? (family + ":" + qualifier) : family;
    final KijiCellDecoder<T> decoder = (KijiCellDecoder<T>) mDecoderMap.get(column);
    if (decoder != null) {
      // There already exists a decoder for this column:
      return decoder;
    }

    if (qualifier != null) {
      // There is no decoder for the specified fully-qualified column.
      // Try the family (this will only work for map-type families):
      return getDecoder(family, null);
    }

    return null;
  }
}
