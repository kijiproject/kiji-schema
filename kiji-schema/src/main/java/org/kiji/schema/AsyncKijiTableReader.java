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

package org.kiji.schema;

import java.io.Closeable;
import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;

/**
 * Interface for asyncrhonously reading data from a Kiji table.
 *
 * <p>
 *   Utilizes {@link EntityId} and {@link KijiDataRequest}
 *   to return {@link KijiResult} or a {@link KijiResultScanner}
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
 *   final KijiResult data = reader.getKijiResult(myEntityId, request);
 * }</pre>
 * </p>
 *
 * <p>To get a row scanner across many records using the same column and version restrictions
 * from above:
 * <pre>{@code
 *   final KijiResultScanner scanner = reader.getResultScanner(request);
 *
 *   final KijiScannerOptions options = new KijiScannerOptions()
 *       .setStartRow(myStartRow)
 *       .setStopRow(myStopRow);
 *   final KijiResultScanner limitedScanner = reader.getResultScanner(request, options);
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
 * <p> Instantiated in Kiji Schema via {@link KijiTable#openAsyncTableReader()}. </p>
 * <p>
 *   Unless otherwise specified, readers are not thread-safe and must be synchronized externally.
 * </p>
 */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Sealed
public interface AsyncKijiTableReader extends Closeable {

  /**
   * Get a KijiFuture of a KijiResult for the given EntityId and data request.
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
   * @return a KijiFuture of a new KijiResult for the given EntityId and data request.
   * @throws IOException in case of an error getting the data.
   */
  KijiFuture<KijiResult> getResult(EntityId entityId, KijiDataRequest dataRequest)
      throws IOException;

  /**
   * Get a KijiResultScanner of KijiFuture<KijiResult>s for the given data request and scan options.
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
   * @return A new KijiResultScanner.
   * @throws IOException in case of an error creating the scanner.
   */
  public KijiResultScanner getKijiResultScanner(
      final KijiDataRequest request,
      final KijiScannerOptions scannerOptions
  ) throws IOException;

}
