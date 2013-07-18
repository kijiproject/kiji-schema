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
 * <pre>{@code
 *   KijiDataRequestBuilder builder = KijiDataRequest.builder()
 *     .withTimeRange(123L, 456L);
 *     .newColumnsDef()
 *     .withMaxVersions(3)
 *     .add("foo", "bar");
 *   final KijiDataRequest request = builder.build();
 *
 *   final KijiTableReader reader = myKijiTable.openTableReader();
 *   final KijiRowData data = reader.get(myEntityId, request);
 * }</pre>
 * </p>
 *
 * <p>To get a row scanner across many records using the same column and version restrictions
 * from above:
 * <pre>{@code
 *   final KijiRowScanner scanner = reader.getScanner(request);
 *
 *   final KijiScannerOptions options = new KijiScannerOptions()
 *       .setStartRow(myStartRow)
 *       .setStopRow(myStopRow);
 *   final KijiRowScanner limitedScanner = reader.getScanner(request, options);
 * }</pre>
 *
 * If a KijiScannerOptions is not set, the scanner will iterate over all rows in the table
 * (as in the case of <code>scanner</code>).
 *
 * By default, Kiji row scanners automatically handle HBase scanner timeouts and reopen the
 * HBase scanner as needed. This behavior may be disabled using
 * {@link KijiScannerOptions#setReopenScannerOnTimeout(boolean)}.
 *
 * Finally, row caching may be configured via KijiScannerOptions.
 * By default, row caching is configured from the Hadoop Configuration property
 * {@code hbase.client.scanner.caching}.
 * </p>
 *
 * <p> Instantiated in Kiji Schema via {@link org.kiji.schema.KijiTable#openTableReader()}. </p>
 * <p>
 *   Unless otherwise specified, readers are not thread-safe and must be synchronized externally.
 * </p>
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

    /** When set, re-open scanners automatically on scanner timeout. */
    private boolean mReopenScannerOnTimeout = true;

    /**
     * Number of rows to fetch per RPC.
     * <ul>
     *   <li> N&gt;1 means fetch N rows per RPC; </li>
     *   <li> N=1 means no caching; </li>
     *   <li> N&lt;1 means use the value from the configuration property
     *        {@code hbase.client.scanner.caching}. </li>
     * </ul>
     */
    private int mRowCaching = -1;

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

    /**
     * Configures whether the underlying HBase scanner should be automatically re-opened on timeout.
     *
     * <p> By default, timeouts are handled and the HBase scanner is automatically reopened. </p>
     *
     * @param reopenScannerOnTimeout True means HBase scanner timeouts are automatically
     *     handled to reopen new scanners. Otherwise ScannerTimeoutExceptions will be surfaced to
     *     the user.
     * @return this KijiScannerOptions.
     */
    public KijiScannerOptions setReopenScannerOnTimeout(boolean reopenScannerOnTimeout) {
      mReopenScannerOnTimeout = reopenScannerOnTimeout;
      return this;
    }

    /**
     * Reports whether the underlying HBase scanner should be reopened on timeout.
     *
     * @return whether the underlying HBase scanner should be reopened on timeout.
     */
    public boolean getReopenScannerOnTimeout() {
      return mReopenScannerOnTimeout;
    }

    /**
     * Configures the row caching, ie. number of rows to fetch per RPC to the region server.
     *
     * <ul>
     *   <li> N&gt;1 means fetch N rows per RPC; </li>
     *   <li> N=1 means no caching; </li>
     *   <li> N&lt;1 means use the value from the configuration property
     *        {@code hbase.client.scanner.caching}. </li>
     * </ul>
     * <p>
     *   By default, this is the value of the configuration property
     *   {@code hbase.client.scanner.caching}.
     * </p>
     *
     * @param rowCaching Number of rows to fetch per RPC to the region server.
     * @return this KijiScannerOptions.
     */
    public KijiScannerOptions setRowCaching(int rowCaching) {
      mRowCaching = rowCaching;
      return this;
    }

    /**
     * Reports the number of rows to fetch per RPC to the region server.
     *
     * <ul>
     *   <li> N&gt;1 means fetch N rows per RPC; </li>
     *   <li> N=1 means no caching; </li>
     *   <li> N&lt;1 means use the value from the configuration property
     *        {@code hbase.client.scanner.caching}. </li>
     * </ul>
     *
     * @return the number of rows to fetch per RPC to the region server.
     */
    public int getRowCaching() {
      return mRowCaching;
    }

  }
}
