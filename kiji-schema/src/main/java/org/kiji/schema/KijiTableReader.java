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

package org.kiji.schema;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.filter.KijiRowFilter;
import org.kiji.schema.hbase.HBaseScanOptions;

/**
 * Interface for reading data from a Kiji table.
 *
 * <p>
 *   Utilizes {@link org.kiji.schema.EntityId} and {@link org.kiji.schema.KijiDataRequest}
 *   to return {@link org.kiji.schema.KijiRowData} or a {@link org.kiji.schema.KijiRowScanner}
 *   to iterate across rows in a Kiji table.
 * </p>
 *
 * <p>To get the three most recent versions of cell data from a column <code>bar</code> from
 * the family <code>foo</code> within the time range (123, 456):
 * <pre>
 *   KijiDataRequestBuilder builder = KijiDataRequest.builder()
 *     .withTimeRange(123L, 456L);
 *     .newColumnsDef()
 *     .withMaxVersions(3)
 *     .add("foo", "bar");
 *   final KijiDataRequest request = builder.build();
 *
 *   final KijiTableReader reader = myKijiTable.openTableReader();
 *   final KijiRowData data = reader.get(myEntityId, request);
 * </pre>
 * </p>
 *
 * <p>To get a row scanner across many records using the same column and version restrictions
 * from above:
 * <pre>
 *   final KijiRowScanner scanner = reader.getScanner(request);
 *
 *   final KijiScannerOptions options = new KijiScannerOptions();
 *   options.setStartRow(myStartRow);
 *   options.setStopRow(myStopRow);
 *   final KijiRowScanner limitedScanner = reader.getScanner(request, options);
 * </pre>
 *   If a KijiScannerOptions is not set, the scanner will iterate over all rows
 *   in the table (as in the case of <code>scanner</code>
 * </p>
 *
 * Instantiated in Kiji Schema via {@link org.kiji.schema.KijiTable#openTableReader()}.
 */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Sealed
public interface KijiTableReader extends Closeable {

  /**
   * Retrieves data from a single row in the kiji table.
   *
   * @param entityId The entity id for the row to get data from.
   * @param dataRequest Specifies the columns of data to retrieve.
   * @return The requested data. If there is no row for the specified entityId, this
   *     will return an empty KijiRowData. (containsColumn() will return false for all
   *     columns.)
   * @throws IOException If there is an IO error.
   */
  KijiRowData get(EntityId entityId, KijiDataRequest dataRequest)
      throws IOException;

  /**
   * Retrieves data from a list of rows in the kiji table.
   *
   * @param entityIds The list of entity ids to collect data for.
   * @param dataRequest Specifies constraints on the data to retrieve for each entity id.
   * @return The requested data.  If an EntityId specified in <code>entityIds</code>
   *     does not exist, then the corresponding KijiRowData will be empty.
   *     If a get fails, then the corresponding KijiRowData will be null (instead of empty).
   * @throws IOException If there is an IO error.
   */
  List<KijiRowData> bulkGet(List<EntityId> entityIds, KijiDataRequest dataRequest)
      throws IOException;

  /**
   * Gets a KijiRowScanner with the specified data request.
   *
   * @param dataRequest The data request to scan for.
   * @return The KijiRowScanner.
   * @throws IOException If there is an IO error.
   * @throws KijiDataRequestException If the data request is invalid.
   */
  KijiRowScanner getScanner(KijiDataRequest dataRequest)
      throws IOException;

  /**
   * Gets a KijiRowScanner using the specified data request and options.
   *
   * @param dataRequest The data request to scan for.
   * @param scannerOptions Other options for the scanner.
   * @return The KijiRowScanner.
   * @throws IOException If there is an IO error.
   * @throws KijiDataRequestException If the data request is invalid.
   */
  KijiRowScanner getScanner(KijiDataRequest dataRequest, KijiScannerOptions scannerOptions)
      throws IOException;

  /**
   * Options for KijiRowScanners.
   */
  @ApiAudience.Public
  public static final class KijiScannerOptions {
    /** The start row for the scan. */
    private EntityId mStartRow = null;
    /** The stop row for the scan. */
    private EntityId mStopRow = null;
    /** The row filter for the scan. */
    private KijiRowFilter mRowFilter = null;
    /**
     * The HBaseScanOptions to scan with for KijiRowScanners
     * backed by an HBase scan.
     *
     * Defaults to the default HBaseScanOptions if not set.
     */
    private HBaseScanOptions mHBaseScanOptions = new HBaseScanOptions();

    /**
     * Creates KijiScannerOptions with uninitialized options
     * and default HBaseScanOptions.
     */
    public KijiScannerOptions() {}

    /**
     * Sets the start row used by the scanner,
     * and returns this KijiScannerOptions to allow chaining.
     *
     * @param startRow The row to start scanning from.
     * @return This KijiScannerOptions with the start row set.
     */
    public KijiScannerOptions setStartRow(EntityId startRow) {
      mStartRow = startRow;
      return this;
    }

    /**
     * Gets the start row set in these options.
     *
     * @return The start row to use, null if unset.
     */
    public EntityId getStartRow() {
      return mStartRow;
    }

    /**
     * Sets the stop row used by the scanner,
     * and returns this KijiScannerOptions to allow chaining.
     *
     * @param stopRow The last row to scan.
     * @return This KijiScannerOptions with the stop row set.
     */
    public KijiScannerOptions setStopRow(EntityId stopRow) {
      mStopRow = stopRow;
      return this;
    }

    /**
     * Gets the stop row set in these options.
     *
     * @return The stop row to use, null if unset.
     */
    public EntityId getStopRow() {
      return mStopRow;
    }

    /**
     * Sets the row filter used by the scanner,
     * and returns this KijiScannerOptions to allow chaining.
     *
     * @param rowFilter The row filter to use.
     * @return This KijiScannerOptions with the row filter set.
     */
    public KijiScannerOptions setKijiRowFilter(KijiRowFilter rowFilter) {
      mRowFilter = rowFilter;
      return this;
    }

    /**
     * Gets the row filter set in these options.
     *
     * @return The row filter to use, null if unset.
     */
    public KijiRowFilter getKijiRowFilter() {
      return mRowFilter;
    }

    /**
     * Sets the HBaseScanOptions used by a HBase backed scanner.
     * The default is the default HBaseScanOptions.
     *
     * @param hBaseScanOptions The HBaseScanOptions to use.
     * @return This KijiScannerOptions with the HBaseScanOptions set.
     */
    public KijiScannerOptions setHBaseScanOptions(HBaseScanOptions hBaseScanOptions) {
      mHBaseScanOptions = hBaseScanOptions;
      return this;
    }

    /**
     * Gets the HBaseScanOptions set in these options.
     *
     * @return The HBaseScanOptions to use; if unset, the default HbaseScanOptions.
     */
    public HBaseScanOptions getHBaseScanOptions() {
      return mHBaseScanOptions;
    }
  }
}
