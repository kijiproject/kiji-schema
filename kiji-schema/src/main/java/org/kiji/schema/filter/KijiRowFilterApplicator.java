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

package org.kiji.schema.filter;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.impl.hbase.HBaseDataRequestAdapter;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Applies a KijiRowFilter to various row-savvy objects.
 *
 * <p>There are several limitations when filtering cells this way, as the filter relies on byte
 * comparisons, which does not play well with Avro records.</p>
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class KijiRowFilterApplicator {
  /** The row filter to be applied by this applicator. */
  private final KijiRowFilter mRowFilter;

  /** The layout of the table the row filter will be applied to. */
  private final KijiTableLayout mTableLayout;

  /** Schema table. */
  private final KijiSchemaTable mSchemaTable;

  /**
   * An implementation of KijiRowFilter.Context that translates kiji entityIds, column
   * names, and cell values to their HBase counterparts.
   */
  @ApiAudience.Private
  private final class KijiRowFilterContext extends KijiRowFilter.Context {
    private final HBaseColumnNameTranslator mColumnNameTranslator;

    /**
     * Constructs a KijiRowFilterContext.
     *
     * @param columnNameTranslator Column name translator for the table to apply filter to.
     */
    private KijiRowFilterContext(HBaseColumnNameTranslator columnNameTranslator) {
      mColumnNameTranslator = columnNameTranslator;
    }

    /** {@inheritDoc} */
    @Override
    public byte[] getHBaseRowKey(String kijiRowKey) {
      return Bytes.toBytes(kijiRowKey);
    }

    /** {@inheritDoc} */
    @Override
    public HBaseColumnName getHBaseColumnName(KijiColumnName kijiColumnName)
        throws NoSuchColumnException {
      return mColumnNameTranslator.toHBaseColumnName(kijiColumnName);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] getHBaseCellValue(KijiColumnName column, DecodedCell<?> kijiCell)
        throws IOException {
      final CellSpec cellSpec = mColumnNameTranslator.getTableLayout().getCellSpec(column)
          .setSchemaTable(mSchemaTable);
      final KijiCellEncoder encoder = DefaultKijiCellEncoderFactory.get().create(cellSpec);
      return encoder.encode(kijiCell);
    }
  }

  /**
   * Creates a new <code>KijiRowFilterApplicator</code> instance.
   * This private constructor is used by the <code>create()</code> factory method.
   *
   * @param rowFilter The row filter to be applied.
   * @param tableLayout The layout of the table this filter applies to.
   * @param schemaTable The kiji schema table.
   */
  private KijiRowFilterApplicator(KijiRowFilter rowFilter, KijiTableLayout tableLayout,
      KijiSchemaTable schemaTable) {
    mRowFilter = rowFilter;
    mTableLayout = tableLayout;
    mSchemaTable = schemaTable;
  }

  /**
   * Creates a new <code>KijiRowFilterApplicator</code> instance.
   *
   * @param rowFilter The row filter to be applied.
   * @param schemaTable The kiji schema table.
   * @param tableLayout The layout of the table this filter applies to.
   * @return a new KijiRowFilterApplicator instance.
   */
  public static KijiRowFilterApplicator create(KijiRowFilter rowFilter, KijiTableLayout tableLayout,
      KijiSchemaTable schemaTable) {
    return new KijiRowFilterApplicator(rowFilter, tableLayout, schemaTable);
  }

  /**
   * Applies the row filter to an HBase scan object.
   *
   * <p>This will tell HBase region servers to filter rows on the server-side, so filtered
   * rows will not even need to get sent across the network back to the client.</p>
   *
   * @param scan An HBase scan descriptor.
   * @throws IOException If there is an IO error.
   */
  public void applyTo(Scan scan) throws IOException {
    // The filter might need to request data that isn't already requested by the scan, so add
    // it here if needed.
    try {
      // TODO: SCHEMA-444 Avoid constructing a new KijiColumnNameTranslator below.
      new HBaseDataRequestAdapter(
          mRowFilter.getDataRequest(),
          HBaseColumnNameTranslator.from(mTableLayout))
          .applyToScan(scan, mTableLayout);
    } catch (InvalidLayoutException e) {
      throw new InternalKijiError(e);
    }

    // Set the filter.
    final KijiRowFilter.Context context =
        new KijiRowFilterContext(HBaseColumnNameTranslator.from(mTableLayout));
    scan.setFilter(mRowFilter.toHBaseFilter(context));
  }
}
