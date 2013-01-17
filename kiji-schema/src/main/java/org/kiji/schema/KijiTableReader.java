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
import org.kiji.annotations.Inheritance;
import org.kiji.schema.filter.KijiRowFilter;

/**
 * Interface for reading data from a kiji table.
 *
 * Instantiated from {@link org.kiji.schema.KijiTable#openTableReader()}
 */
@ApiAudience.Public
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
   * Gets a KijiRowScanner using default options.
   *
   * @param dataRequest Specifies the columns of data to retrieve.
   * @return The KijiRowScanner.
   * @throws IOException If there is an IO error.
   * @throws KijiDataRequestException If the data request is invalid.
   */
  KijiRowScanner getScanner(KijiDataRequest dataRequest)
      throws IOException;

  /**
   * Gets a KijiRowScanner using default options.
   *
   * @param dataRequest Specifies the columns of data to retrieve.
   * @param startRow The entity id for the row to start the scan from.  If null, the scanner
   *     will read all rows up to the stopRow.
   * @param stopRow The entity id for the row to end the scan at.  If null, the scanner will
   *     read all rows after the startRow.
   * @return The KijiRowScanner.
   * @throws IOException If there is an IO error.
   * @throws KijiDataRequestException If the data request is invalid.
   */
  KijiRowScanner getScanner(KijiDataRequest dataRequest, EntityId startRow, EntityId stopRow)
      throws IOException;

  /**
   * Gets a KijiRowScanner using the specified HBaseScanOptions.
   *
   * @param dataRequest Specifies the columns of data to retrieve.
   * @param startRow The entity id for the row to start the scan from.  If null, the scanner
   *     will read all rows up to the stopRow.
   * @param stopRow The entity id for the row to end the scan at.  If null, the scanner will
   *     read all rows after the startRow.
   * @param scanOptions The custom scanner configuration to use.
   * @return The KijiRowScanner.
   * @throws IOException If there is an IO error.
   * @throws KijiDataRequestException If the data request is invalid.
   */
  KijiRowScanner getScanner(KijiDataRequest dataRequest, EntityId startRow, EntityId stopRow,
      HBaseScanOptions scanOptions) throws IOException;

  /**
   * Gets a KijiRowScanner using a KijiRowFilter and the specified HBaseScanOptions.
   *
   * @param dataRequest Specifies the columns of data to retrieve.
   * @param startRow The entity id for the row to start the scan from.  If null, the scanner
   *     will read all rows up to the stopRow.
   * @param stopRow The entity id for the row to end the scan at.  If null, the scanner will
   *     read all rows after the startRow.
   * @param rowFilter The KijiRowFilter to filter these results on
   * @param scanOptions The custom scanner configuration to use.
   * @return The KijiRowScanner.
   * @throws IOException If there is an IO error.
   * @throws KijiDataRequestException If the data request is invalid.
   */
  KijiRowScanner getScanner(KijiDataRequest dataRequest, EntityId startRow, EntityId stopRow,
      KijiRowFilter rowFilter, HBaseScanOptions scanOptions) throws IOException;
}
