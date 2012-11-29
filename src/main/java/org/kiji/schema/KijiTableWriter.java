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
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface for performing writes to a Kiji table.
 *
 * Instantiated from {@link org.kiji.schema.KijiTable#openTableWriter()}
 */
public abstract class KijiTableWriter implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(KijiTableWriter.class);

  /** Whether the writer is open. */
  private boolean mIsOpen;
  /** For debugging finalize(). */
  private String mConstructorStack = "";

  /**
   * Creates a buffered writer that can be used to modify a Kiji table.
   */
  protected KijiTableWriter() {
    mIsOpen = true;
    if (LOG.isDebugEnabled()) {
      try {
        throw new Exception();
      } catch (Exception e) {
        mConstructorStack = StringUtils.stringifyException(e);
      }
    }
  }

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the write is interrupted.
   */
  public void put(EntityId entityId, String family, String qualifier, CharSequence value)
      throws IOException, InterruptedException {
    put(entityId, family, qualifier, Long.MAX_VALUE, value);
  }

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the write is interrupted.
   */
  public void put(EntityId entityId, String family, String qualifier, GenericContainer value)
      throws IOException, InterruptedException {
    put(entityId, family, qualifier, Long.MAX_VALUE, value);
  }

  /**
   * Puts a cell into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param cell The data to write.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the write is interrupted.
   */
  public void put(EntityId entityId, String family, String qualifier, KijiCell<?> cell)
      throws IOException, InterruptedException {
    put(entityId, family, qualifier, Long.MAX_VALUE, cell);
  }

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the write is interrupted.
   */
  public void put(EntityId entityId, String family, String qualifier, boolean value)
      throws IOException, InterruptedException {
    put(entityId, family, qualifier, Long.MAX_VALUE, value);
  }

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the write is interrupted.
   */
  public void put(EntityId entityId, String family, String qualifier, byte[] value)
      throws IOException, InterruptedException {
    put(entityId, family, qualifier, Long.MAX_VALUE, value);
  }

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the write is interrupted.
   */
  public void put(EntityId entityId, String family, String qualifier, double value)
      throws IOException, InterruptedException {
    put(entityId, family, qualifier, Long.MAX_VALUE, value);
  }

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the write is interrupted.
   */
  public void put(EntityId entityId, String family, String qualifier, float value)
      throws IOException, InterruptedException {
    put(entityId, family, qualifier, Long.MAX_VALUE, value);
  }

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the write is interrupted.
   */
  public void put(EntityId entityId, String family, String qualifier, int value)
      throws IOException, InterruptedException {
    put(entityId, family, qualifier, Long.MAX_VALUE, value);
  }

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the write is interrupted.
   */
  public void put(EntityId entityId, String family, String qualifier, long value)
      throws IOException, InterruptedException {
    put(entityId, family, qualifier, Long.MAX_VALUE, value);
  }

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp The timestamp to put the data at
   *     (use HConstants.LATEST_TIMESTAMP for current time).
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the write is interrupted.
   */
  public void put(EntityId entityId, String family, String qualifier, long timestamp,
      CharSequence value) throws IOException, InterruptedException {
    put(entityId, family, qualifier, timestamp,
        new KijiCell<CharSequence>(Schema.create(Schema.Type.STRING), value));
  }

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp The timestamp to put the data at
   *     (use HConstants.LATEST_TIMESTAMP for current time).
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the write is interrupted.
   */
  public void put(EntityId entityId, String family, String qualifier, long timestamp,
      GenericContainer value) throws IOException, InterruptedException {
    put(entityId, family, qualifier, timestamp,
        new KijiCell<GenericContainer>(value.getSchema(), value));
  }

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp The timestamp to put the data at
   *     (use HConstants.LATEST_TIMESTAMP for current time).
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the write is interrupted.
   */
  public void put(EntityId entityId, String family, String qualifier, long timestamp, boolean value)
      throws IOException, InterruptedException {
    put(entityId, family, qualifier, timestamp,
        new KijiCell<Boolean>(Schema.create(Schema.Type.BOOLEAN), value));
  }

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp The timestamp to put the data at
   *     (use HConstants.LATEST_TIMESTAMP for current time).
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the write is interrupted.
   */
  public void put(EntityId entityId, String family, String qualifier, long timestamp, byte[] value)
      throws IOException, InterruptedException {
    put(entityId, family, qualifier, timestamp,
        new KijiCell<ByteBuffer>(Schema.create(Schema.Type.BYTES), ByteBuffer.wrap(value)));
  }

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp The timestamp to put the data at
   *     (use HConstants.LATEST_TIMESTAMP for current time).
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the write is interrupted.
   */
  public void put(EntityId entityId, String family, String qualifier, long timestamp, double value)
      throws IOException, InterruptedException {
    put(entityId, family, qualifier, timestamp,
        new KijiCell<Double>(Schema.create(Schema.Type.DOUBLE), value));
  }

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp The timestamp to put the data at
   *     (use HConstants.LATEST_TIMESTAMP for current time).
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the write is interrupted.
   */
  public void put(EntityId entityId, String family, String qualifier, long timestamp, float value)
      throws IOException, InterruptedException {
    put(entityId, family, qualifier, timestamp,
        new KijiCell<Float>(Schema.create(Schema.Type.FLOAT), value));
  }

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp The timestamp to put the data at
   *     (use HConstants.LATEST_TIMESTAMP for current time).
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the write is interrupted.
   */
  public void put(EntityId entityId, String family, String qualifier, long timestamp, int value)
      throws IOException, InterruptedException {
    put(entityId, family, qualifier, timestamp,
        new KijiCell<Integer>(Schema.create(Schema.Type.INT), value));
  }

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp The timestamp to put the data at
   *     (use HConstants.LATEST_TIMESTAMP for current time).
   * @param value The data to write.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the write is interrupted.
   */
  public void put(EntityId entityId, String family, String qualifier, long timestamp, long value)
      throws IOException, InterruptedException {
    put(entityId, family, qualifier, timestamp,
        new KijiCell<Long>(Schema.create(Schema.Type.LONG), value));
  }

  /**
   * Puts a cell into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp The timestamp to put the data at
   *     (use HConstants.LATEST_TIMESTAMP for current time).
   * @param cell The data to write.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the write is interrupted.
   */
  public abstract void put(EntityId entityId, String family, String qualifier, long timestamp,
      KijiCell<?> cell) throws IOException, InterruptedException;

  /**
   * Increments a counter in a kiji table.
   *
   * <p>This method will throw an exception if called on a column that isn't a counter.</p>
   *
   * @param entityId The entity (row) that contains the counter.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param amount The amount to increment the counter (may be negative).
   * @return The new counter value, post increment.
   * @throws IOException If there is an IO error.
   */
  public abstract KijiCounter increment(EntityId entityId, String family, String qualifier,
      long amount) throws IOException;

  /**
   * Sets a counter value in a kiji table.
   *
   * <p>This method will throw an exception if called on a column that isn't a counter.</p>
   *
   * @param entityId The entity (row) that contains the counter.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param value The value to set the counter to.
   * @throws IOException If there is an IO error.
   */
  public abstract void setCounter(EntityId entityId, String family, String qualifier, long value)
      throws IOException;

  /**
   * Deletes everything from a row.
   *
   * @param entityId The entity (row) to delete.
   * @throws IOException If there is an IO error.
   */
  public void deleteRow(EntityId entityId) throws IOException {
    deleteRow(entityId, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Delete all cells from a row with a timestamp less than or equal to the specified timestamp.
   *
   * @param entityId The entity (row) to delete data from.
   * @param upToTimestamp A timestamp.
   * @throws IOException If there is an IO error.
   */
  public abstract void deleteRow(EntityId entityId, long upToTimestamp) throws IOException;

  /**
   * Deletes all versions of all cells in a family.
   *
   * @param entityId The entity (row) to delete data from.
   * @param family A column family.
   * @throws IOException If there is an IO error.
   */
  public void deleteFamily(EntityId entityId, String family) throws IOException {
    deleteFamily(entityId, family, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Deletes all cells from a family with a timestamp less than or equal to the specified timestamp.
   *
   * @param entityId The entity (row) to delete data from.
   * @param family A column family.
   * @param upToTimestamp A timestamp.
   * @throws IOException If there is an IO error.
   */
  public abstract void deleteFamily(EntityId entityId, String family, long upToTimestamp)
      throws IOException;

  /**
   * Deletes all versions of all cells in a column.
   *
   * @param entityId The entity (row) to delete data from.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @throws IOException If there is an IO error.
   */
  public void deleteColumn(EntityId entityId, String family, String qualifier) throws IOException {
    deleteColumn(entityId, family, qualifier, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Deletes all cells with a timestamp less than or equal to the specified timestamp.
   *
   * @param entityId The entity (row) to delete data from.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param upToTimestamp A timestamp.
   * @throws IOException If there is an IO error.
   */
  public abstract void deleteColumn(
      EntityId entityId, String family, String qualifier, long upToTimestamp) throws IOException;

  /**
   * Deletes the most recent version of the cell in a column.
   *
   * @param entityId The entity (row) to delete data from.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @throws IOException If there is an IO error.
   */
  public void deleteCell(EntityId entityId, String family, String qualifier) throws IOException {
    deleteCell(entityId, family, qualifier, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Deletes a single cell with a specified timestamp.
   *
   * @param entityId The entity (row) to delete data from.
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @param timestamp The timestamp of the cell to delete (use HConstants.LATEST_TIMESTAMP
   *     to delete the most recent cell in the column).
   * @throws IOException If there is an IO error.
   */
  public abstract void deleteCell(
      EntityId entityId, String family, String qualifier, long timestamp) throws IOException;

  /**
   * Flushes the pending modifications to the table.
   *
   * @throws IOException If there is an error.
   * @throws InterruptedException If the flushing is interrupted.
   */
  public void flush() throws IOException, InterruptedException {
    // No-op by default.
  }

  @Override
  public void close() throws IOException {
    if (!mIsOpen) {
      LOG.warn("Called close() on KijiTableWriter more than once.");
    }

    mIsOpen = false;

    // Overriding {@link Closeable#close()} only allows for throwing an IOException, but we could
    // throw an InterruptedException while flushing to the underlying context.
    try {
      flush();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void finalize() throws Throwable {
    if (mIsOpen) {
      LOG.warn("Closing KijiTableWriter in finalize(). You should close it explicitly.");
      LOG.debug(mConstructorStack);
      close();
    }
    super.finalize();
  }
}
