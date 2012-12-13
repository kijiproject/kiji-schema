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
import org.kiji.schema.HBaseColumnName;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiCellFormat;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.impl.HBaseDataRequestAdapter;
import org.kiji.schema.layout.ColumnNameTranslator;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;


/**
 * Applies a KijiRowFilter to various row-savvy objects.
 */
@ApiAudience.Private
public final class KijiRowFilterApplicator {
  /** The row filter to be applied by this applicator. */
  private final KijiRowFilter mRowFilter;

  /** The kiji schema table. */
  private final KijiSchemaTable mSchemaTable;

  /** The layout of the table the row filter will be applied to. */
  private final KijiTableLayout mTableLayout;

  /**
   * An implementation of KijiRowFilter.Context that translates kiji entityIds, column
   * names, and cell values to their HBase counterparts.
   */
  private class KijiRowFilterContext extends KijiRowFilter.Context {
    private final ColumnNameTranslator mColumnNameTranslator;
    private final KijiCellEncoder mCellEncoder;

    /**
     * Constructs a KijiRowFilterContext.
     *
     * @param columnNameTranslator A column name translator for the table the filter will
     *     be applied to.
     * @param cellEncoder A kiji cell encoder.
     */
    public KijiRowFilterContext(
        ColumnNameTranslator columnNameTranslator, KijiCellEncoder cellEncoder) {
      mColumnNameTranslator = columnNameTranslator;
      mCellEncoder = cellEncoder;
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
    public byte[] getHBaseCellValue(KijiColumnName column, KijiCell<?> kijiCell)
        throws IOException {
      final KijiCellFormat format = mColumnNameTranslator.getTableLayout().getCellFormat(column);
      return mCellEncoder.encode(kijiCell, format);
    }
  }

  /**
   * Creates a new <code>KijiRowFilterApplicator</code> instance.
   *
   * @param rowFilter The row filter to be applied.
   * @param schemaTable The kiji schema table.
   * @param tableLayout The layout of the table this filter applies to.
   */
  public KijiRowFilterApplicator(
      KijiRowFilter rowFilter, KijiSchemaTable schemaTable, KijiTableLayout tableLayout) {
    mRowFilter = rowFilter;
    mSchemaTable = schemaTable;
    mTableLayout = tableLayout;
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
      new HBaseDataRequestAdapter(mRowFilter.getDataRequest()).applyToScan(scan, mTableLayout);
    } catch (InvalidLayoutException e) {
      throw new InternalKijiError(e);
    }

    // Set the filter.
    KijiRowFilter.Context context = new KijiRowFilterContext(
        new ColumnNameTranslator(mTableLayout), new KijiCellEncoder(mSchemaTable));
    scan.setFilter(mRowFilter.toHBaseFilter(context));
  }
}
