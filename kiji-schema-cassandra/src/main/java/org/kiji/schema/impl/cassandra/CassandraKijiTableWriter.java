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

package org.kiji.schema.impl.cassandra;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiTableWriter;

/**
 * Makes modifications to a Kiji table by sending requests directly to Cassandra from the local
 * client.
 *
 */
@ApiAudience.Private
@ThreadSafe
public final class CassandraKijiTableWriter implements KijiTableWriter {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiTableWriter.class);

  private final CassandraKijiBufferedWriter mBufferedWriter;

  /** The kiji table instance. */
  private final CassandraKijiTable mTable;

  /**
   * Creates a non-buffered kiji table writer that sends modifications directly to Kiji.
   *
   * @param table A kiji table.
   * @throws java.io.IOException on I/O error.
   */
  public CassandraKijiTableWriter(CassandraKijiTable table) throws IOException {
    mTable = table;
    mBufferedWriter = table.getWriterFactory().openBufferedWriter();

    // Flush immediately
    mBufferedWriter.setBufferSize(0);

    // Retain the table only when everything succeeds.
    mTable.retain();
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(
      final EntityId entityId,
      final String family,
      final String qualifier,
      final T value
  ) throws IOException {
    mBufferedWriter.put(entityId, family, qualifier, value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(
      final EntityId entityId,
      final String family,
      final String qualifier,
      final long timestamp,
      final T value
  ) throws IOException {
    mBufferedWriter.put(entityId, family, qualifier, timestamp, value);
  }

  // ----------------------------------------------------------------------------------------------
  // Counter set, get, increment.

  /** {@inheritDoc} */
  @Override
  public KijiCell<Long> increment(
      final EntityId entityId,
      final String family,
      final String qualifier,
      final long amount
  ) throws IOException {
    throw new UnsupportedOperationException("Cassandra Kiji does not support counter columns.");
  }

  // ----------------------------------------------------------------------------------------------
  // Deletes

  /** {@inheritDoc} */
  @Override
  public void deleteRow(final EntityId entityId) throws IOException {
    mBufferedWriter.deleteRow(entityId);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteRow(final EntityId entityId, final long upToTimestamp) throws IOException {
    mBufferedWriter.deleteRow(entityId, upToTimestamp);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(final EntityId entityId, final String family) throws IOException {
    mBufferedWriter.deleteFamily(entityId, family);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(
      final EntityId entityId,
      final String family,
      final long upToTimestamp
  ) throws IOException {
    mBufferedWriter.deleteFamily(entityId, family, upToTimestamp);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(
      final EntityId entityId,
      final String family,
      final String qualifier
  ) throws IOException {
    mBufferedWriter.deleteColumn(entityId, family, qualifier);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(
      final EntityId entityId,
      final String family,
      final String qualifier,
      final long upToTimestamp
  ) throws IOException {
    mBufferedWriter.deleteColumn(entityId, family, qualifier, upToTimestamp);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(
      final EntityId entityId,
      final String family,
      final String qualifier
  ) throws IOException {
    mBufferedWriter.deleteCell(entityId, family, qualifier);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(
      final EntityId entityId,
      final String family,
      final String qualifier,
      final long timestamp
  ) throws IOException {
    mBufferedWriter.deleteCell(entityId, family, qualifier, timestamp);
  }

  // ----------------------------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public void flush() throws IOException {
    LOG.debug("KijiTableWriter does not need to be flushed.");
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mBufferedWriter.close();
    mTable.release();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(CassandraKijiTableWriter.class)
        .add("id", System.identityHashCode(this))
        .add("table", mTable.getURI())
        .toString();
  }
}
