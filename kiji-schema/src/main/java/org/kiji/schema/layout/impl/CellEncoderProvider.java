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
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiCellEncoderFactory;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiTable;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;

/**
 * Provider for cell encoders of a given table.
 *
 * <p>
 *   Cell encoders for all columns in the table are pro-actively created when the
 *   CellEncoderProvider is constructed.
 *   Cell encoders are cached and reused.
 * </p>
 */
@ApiAudience.Private
public final class CellEncoderProvider {

  /** Layout of the table to provide encoders for. */
  private final KijiTableLayout mLayout;

  /** Table to resolve Avro schemas. */
  private final KijiSchemaTable mSchemaTable;

  /** Default cell encoder factory (either specific or generic). */
  private final KijiCellEncoderFactory mCellEncoderFactory;

  /** Maps column names to encoders. */
  private final ImmutableMap<String, KijiCellEncoder> mEncoderMap;

  /**
   * Initializes a provider for cell encoders.
   *
   * @param table to provide cell encoders for.
   * @param cellEncoderFactory is the factory to use to create cell encoders.
   * @throws IOException on I/O error.
   */
  public CellEncoderProvider(
      KijiTable table,
      KijiCellEncoderFactory cellEncoderFactory)
      throws IOException {
    mLayout = table.getLayout();
    mSchemaTable = table.getKiji().getSchemaTable();
    mCellEncoderFactory = cellEncoderFactory;

    // Compute the set of all the column names (map-type families and fully-qualified columns from
    // group-type families).
    final Set<KijiColumnName> columns = Sets.newHashSet();
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

    // Pro-actively build cell encoders for all columns in the table:
    final Map<String, KijiCellEncoder> encoderMap = Maps.newHashMap();
    for (KijiColumnName column : columns) {
      final CellSpec cellSpec = mLayout.getCellSpec(column);

      cellSpec.setSchemaTable(mSchemaTable);
      cellSpec.setEncoderFactory(mCellEncoderFactory);

      final KijiCellEncoder encoder = cellSpec.getEncoderFactory().create(cellSpec);
      encoderMap.put(column.getName(), encoder);
    }
    mEncoderMap = ImmutableMap.copyOf(encoderMap);
  }

  // -----------------------------------------------------------------------------------------------
  // Public interface

  /**
   * Gets a cell encoder for the specified column or (map-type) family.
   *
   * <p>
   *   When requesting a encoder for a column within a map-type family, the encoder for the
   *   entire map-type family will be returned.
   * </p>
   *
   * @param family Family of the column to look up.
   * @param qualifier Qualifier of the column to look up.
   *     Null means no qualifier, ie. get a encoder for a (map-type) family.
   * @return a cell encoder for the specified column.
   *     Null if the column does not exist or if a family level encoder is requested from a group
   *     type family.
   * @throws IOException on I/O error.
   */
  public KijiCellEncoder getEncoder(String family, String qualifier) throws IOException {
    final String column = (qualifier != null) ? (family + ":" + qualifier) : family;
    final KijiCellEncoder encoder = mEncoderMap.get(column);
    if (encoder != null) {
      // There already exists a encoder for this column:
      return encoder;
    }

    if (qualifier != null) {
      // There is no encoder for the specified fully-qualified column.
      // Try the family (this will only work for map-type families):
      return getEncoder(family, null);
    }

    return null;
  }
}
