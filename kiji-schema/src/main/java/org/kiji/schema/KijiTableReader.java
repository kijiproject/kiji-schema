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

import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public abstract class KijiTableReader implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(KijiTableReader.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger(KijiTableReader.class.getName() + ".Cleanup");

  /** The kiji table being read from. */
  private KijiTable mTable;

  /** The cell decoder factory to use. */
  private KijiCellDecoderFactory mKijiCellDecoderFactory;

  /** Whether the writer is open. */
  private boolean mIsOpen;

  /** For debugging finalize(). */
  private String mConstructorStack = "";

  /**
   * Creates a reader over a kiji table.
   *
   * @param table The kiji table to read from.
   */
  protected KijiTableReader(KijiTable table) {
    mTable = table;
    mIsOpen = true;
    if (CLEANUP_LOG.isDebugEnabled()) {
      try {
        throw new Exception();
      } catch (Exception e) {
        mConstructorStack = StringUtils.stringifyException(e);
      }
    }
  }

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
  public abstract KijiRowData get(EntityId entityId, KijiDataRequest dataRequest)
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
  public abstract List<KijiRowData> bulkGet(List<EntityId> entityIds, KijiDataRequest dataRequest)
      throws IOException;

  /**
   * Gets the table this reads from.
   *
   * @return The kiji table.
   */
  public KijiTable getTable() {
    return mTable;
  }

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
  public KijiRowScanner getScanner(KijiDataRequest dataRequest, EntityId startRow,
      EntityId stopRow) throws IOException {
    return getScanner(dataRequest, startRow, stopRow, null, new HBaseScanOptions());
  }

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
  public KijiRowScanner getScanner(KijiDataRequest dataRequest, EntityId startRow,
      EntityId stopRow, HBaseScanOptions scanOptions)
      throws IOException {
    return getScanner(dataRequest, startRow, stopRow, null, scanOptions);
  }

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
  public abstract KijiRowScanner getScanner(KijiDataRequest dataRequest, EntityId startRow,
      EntityId stopRow, KijiRowFilter rowFilter, HBaseScanOptions scanOptions)
      throws IOException;

  /**
   * Gets the KijiCellDecoderFactory to use for decoding KijiCells.
   *
   * Defaults to SpecificCellDecoderFactory (creating it if necessary).
   * This behavior can be changed by calling
   *   {@link #setKijiCellDecoderFactory(KijiCellDecoderFactory)}
   * or overriding this method in a subclass.
   *
   * @return The KijiCellDecoderFactory to be used when retrieving a KijiCell.
   * @throws IOException if there is an error retrieving the KijiSchemaTable
   */
  public KijiCellDecoderFactory getKijiCellDecoderFactory() throws IOException {
    if (mKijiCellDecoderFactory == null) {
      mKijiCellDecoderFactory = SpecificCellDecoderFactory.get();
    }
    return mKijiCellDecoderFactory;
  }

  /**
   * Sets the KijiCellDecoderFactory to be used for retrieving a KijiCell.
   *
   * @param kijiCellDecoderFactory the KijiCellDecoderFactory to use for decoding KijiCells.
   */
  public void setKijiCellDecoderFactory(KijiCellDecoderFactory kijiCellDecoderFactory) {
    this.mKijiCellDecoderFactory = kijiCellDecoderFactory;
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    if (!mIsOpen) {
      LOG.warn("Called close() on KijiTableWriter more than once.");
    }
    mIsOpen = false;
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    if (mIsOpen) {
      CLEANUP_LOG.warn("Closing KijiTableReader in finalize(). You should close it explicitly.");
      CLEANUP_LOG.debug(mConstructorStack);
      close();
    }
    super.finalize();
  }
}
