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

/**
 * Base class for writers of Kiji data to different output sinks.
 *
 * <p>A data writer must be configured with the name of a Kiji column family writes will target. A
 * data writer may optionally be configured with the name of a Kiji column qualifier writes will
 * target. If the writer has not been configured with a Kiji column qualifier, all writes must
 * specify the Kiji column qualifier.</p>
 *
 * <p>A KijiDataWriter may buffer writes before output. Clients should use the
 * {@link KijiDataWriter#flush()} method to force the data writer to output its buffered writes.
 * Closing a data writer using {@link KijiDataWriter#close()} will flush all writes and release
 * any resources used by the data writer.</p>
 */
public abstract class KijiDataWriter
    implements Closeable {
  /** A factory to use when creating entity ids used for writing. */
  private final EntityIdFactory mEntityIdFactory;
  /** Family specified for this writer. Is null if not specified in constructor. */
  private String mFamily;
  /** Qualifier specified for this writer. Is null if not specified in constructor. */
  private String mQualifier;

  /**
   * Describes the options that can be configured on the {@link KijiDataWriter}.
   */
  public static class Options {
    private EntityIdFactory mEntityIdFactory;
    private String mFamily;
    private String mQualifier;

    /**
     * @param entityIdFactory The factory to use when creating entity ids used for writing.
     * @return This object to allow chaining of setter methods.
     */
    public Options withEntityIdFactory(EntityIdFactory entityIdFactory) {
      mEntityIdFactory = entityIdFactory;
      return this;
    }

    /**
     * @param family The Family to use when writing data. Set this to null if writes are to occur in
     * multiple families.
     * @return This object to allow chaining of setter methods.
     */
    public Options withFamily(String family) {
      mFamily = family;
      return this;
    }

    /**
     * @param qualifier The qualifier to use when writing data. Set this to null if writes are to
     * occur in multiple columns.
     * @return This object to allow chaining of setter methods.
     */
    public Options withQualifier(String qualifier) {
      mQualifier = qualifier;
      return this;
    }

    /** @return A factory to use when creating entity ids used for writing. */
    public EntityIdFactory getEntityIdFactory() {
      return mEntityIdFactory;
    }

    /** @return Family specified for this writer. */
    public String getFamily() {
      return mFamily;
    }

    /** @return Qualifier specified for this writer. */
    public String getQualifier() {
      return mQualifier;
    }
  }

  /**
   * Creates an instance.
   *
   * @param options The options to create the data writer with.
   */
  protected KijiDataWriter(Options options) {
    mEntityIdFactory = options.getEntityIdFactory();
    mFamily = options.getFamily();
    mQualifier = options.getQualifier();
  }

  /**
   * Creates a new instance by copying the options from another writer.
   *
   * @param original The data writer whose options should be copied.
   */
  protected KijiDataWriter(KijiDataWriter original) {
    mEntityIdFactory = original.mEntityIdFactory;
    mFamily = original.mFamily;
    mQualifier = original.mQualifier;
  }

  /**
   * Gets an instance of an entity id from a UTF8 encoded Kiji row key.
   *
   * @param kijiRowKey UTF8 encoded Kiji row key.
   * @return An entity id.
   */
  public EntityId getEntityId(String kijiRowKey) {
    return mEntityIdFactory.fromKijiRowKey(kijiRowKey);
  }

  /**
   * @return The column qualifier used by this data writer, or
   * <code>null</code> if it has not been set.
   */
  public String getQualifier() {
    return mQualifier;
  }

  /**
   * Sets the qualifier being written to.
   *
   * @param qualifier The qualifier to use in future writes.
   */
  public void setQualifier(String qualifier) {
    mQualifier = qualifier;
  }

  /**
   * @return The column qualifier used by this data writer.
   * @throws RuntimeException If the column qualifier has not been set.
   */
  private String getQualifierIfNotNull() {
    if (null == mQualifier) {
      throw new RuntimeException("No column qualifier has been specified for write.");
    }
    return mQualifier;
  }

  /**
   * Checks that an output qualifier specified for a write is valid under this writer's
   * configuration.
   *
   * @param qualifier The qualifier to test (possibly
   * <code>null</code>).
   * @throws IllegalArgumentException If the qualifier is invalid.
   */
  protected void checkQualifier(String qualifier) {
    if (null == getQualifier() && null == qualifier) {
      throw new IllegalArgumentException(
          "Output qualifier was not specified and the output column did not specify a qualifier."
          + " Did you forget the colon in your output column spec?");
    }

    if (null != getQualifier() && !getQualifier().equals(qualifier)) {
      throw new IllegalArgumentException(
          "Output key " + qualifier + " does not match the output column qualifier "
          + getQualifier());
    }
  }

  /**
   * @return The family used by this data writer, or
   * <code>null</code> if it has not been set.
   */
  public String getFamily() {
    return mFamily;
  }

  /**
   * Sets the family being used by this data writer.
   *
   * @param family The family to use in future writes.
   */
  public void setFamily(String family) {
    mFamily = family;
  }

  /**
   * @return The family used by this data writer.
   * @throws RuntimeException If the family has not been set.
   */
  private String getFamilyIfNotNull() {
    if (null == mFamily) {
      throw new RuntimeException("No family has been specified for write.");
    }
    return mFamily;
  }

  /**
   * Checks that an output family specified for a write is valid under this writer's configuration.
   *
   * @param family The family to test (possibly
   * <code>null</code>).
   * @throws IllegalArgumentException If the family is invalid.
   */
  protected void checkFamily(String family) {
    if (null == getFamily() && null == family) {
      throw new IllegalArgumentException(
          "Output family was not specified and the output did not specify a family");
    }

    if (null != getFamily() && !getFamily().equals(family)) {
      throw new IllegalArgumentException(
          "Output family " + family + " does not match the specified family " + getFamily());
    }
  }

  /**
   * Sets the writer to write to the specified column.
   *
   * @param family The family of the column.
   * @param qualifier The qualifier of the column.
   */
  public void setColumn(String family, String qualifier) {
    mFamily = family;
    mQualifier = qualifier;
  }

  /**
   * Checks that the specified family and qualifier are valid under this writer's configuration.
   *
   * @param family The family to test (possibly
   * <code>null</code>).
   * @param qualifier The qualifier to test (possibly
   * <code>null</code>).
   * @throws IllegalArgumentException If the family or qualifier are invalid.
   */
  protected void checkColumn(String family, String qualifier) {
    checkFamily(family);
    checkQualifier(qualifier);
  }

  /**
   * Writes a null value to the specified row.
   *
   * @param id The entity id of the row.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void writeNull(EntityId id)
      throws IOException, InterruptedException {
    writeNull(id, getFamilyIfNotNull(), getQualifierIfNotNull());
  }

  /**
   * Writes a null value to the specified row at the specified timestamp.
   *
   * @param id The entity id of the row.
   * @param timestamp The timestamp at which to write the value.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void writeNull(EntityId id, long timestamp)
      throws IOException, InterruptedException {
    writeNull(id, getFamilyIfNotNull(), getQualifierIfNotNull(), timestamp);
  }

  /**
   * Writes a null value to the specified row and key.
   *
   * @param id The entity id of the row.
   * @param qualifier The key to write to.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void writeNull(EntityId id, String qualifier)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), qualifier, Schema.create(Schema.Type.NULL), null);
  }

  /**
   * Writes a null value to the specified row and key.
   *
   * @param id The entity id of the row.
   * @param family The family to write to.
   * @param qualifier The key to write to.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void writeNull(EntityId id, String family, String qualifier)
      throws IOException, InterruptedException {
    write(id, family, qualifier, Schema.create(Schema.Type.NULL), null);
  }

  /**
   * Writes a null value to the specified row, key, and timestamp.
   *
   * @param id The entity id of the row.
   * @param qualifier The key to write to.
   * @param timestamp The timestamp at which to write the value.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   *
   */
  public void writeNull(EntityId id, String qualifier, long timestamp)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), qualifier, Schema.create(Schema.Type.NULL), timestamp, null);
  }

  /**
   * Writes a null value to the specified row, key, and timestamp.
   *
   * @param id The entity id of the row.
   * @param family The family to write to.
   * @param qualifier The key to write to.
   * @param timestamp The timestamp at which to write the value.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   *
   */
  public void writeNull(EntityId id, String family, String qualifier, long timestamp)
      throws IOException, InterruptedException {
    write(id, family, qualifier, Schema.create(Schema.Type.NULL), timestamp, null);
  }

  /**
   * Writes a boolean value to the specified row.
   *
   * @param id The entity id of the row.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, boolean value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), getQualifierIfNotNull(), value);
  }

  /**
   * Writes a boolean value to the specified row at the specified timestamp.
   *
   * @param id The entity id of the row.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, long timestamp, boolean value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), getQualifierIfNotNull(), timestamp, value);
  }

  /**
   * Writes a boolean value to the specified row and qualifier.
   *
   * @param id The entity id of the row.
   * @param qualifier The column qualifier at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String qualifier, boolean value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), qualifier, Schema.create(Schema.Type.BOOLEAN), value);
  }

  /**
   * Writes a boolean value to the specified row and qualifier.
   *
   * @param id The entity id of the row.
   * @param family The family to write to.
   * @param qualifier The column qualifier at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String family, String qualifier, boolean value)
      throws IOException, InterruptedException {
    write(id, family, qualifier, Schema.create(Schema.Type.BOOLEAN), value);
  }

  /**
   * Writes a boolean value to the specified row, qualifier, and timestamp.
   *
   * @param id The entity id of the row.
   * @param qualifier The column qualifier at which the value will be written.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String qualifier, long timestamp, boolean value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), qualifier, Schema.create(Schema.Type.BOOLEAN), timestamp,
        value);
  }

  /**
   * Writes a boolean value to the specified row, qualifier, and timestamp.
   *
   * @param id The entity id of the row.
   * @param family The family to write to.
   * @param qualifier The column qualifier at which the value will be written.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String family, String qualifier, long timestamp, boolean value)
      throws IOException, InterruptedException {
    write(id, family, qualifier, Schema.create(Schema.Type.BOOLEAN), timestamp, value);
  }

  /**
   * Writes an integer value to the specified row.
   *
   * @param id The entity id of the row.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, int value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), getQualifierIfNotNull(), value);
  }

  /**
   * Writes an integer value to the specified row at the specified timestamp.
   *
   * @param id The entity id of the row.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, long timestamp, int value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), getQualifierIfNotNull(), timestamp, value);
  }

  /**
   * Writes an integer value to the specified row and qualifier.
   *
   * @param id The entity id of the row.
   * @param qualifier The column qualifier at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String qualifier, int value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), qualifier, Schema.create(Schema.Type.INT), value);
  }

  /**
   * Writes an integer value to the specified row and qualifier.
   *
   * @param id The entity id of the row.
   * @param family The family to write to.
   * @param qualifier The column qualifier at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String family, String qualifier, int value)
      throws IOException, InterruptedException {
    write(id, family, qualifier, Schema.create(Schema.Type.INT), value);
  }

  /**
   * Writes an integer value to the specified row, qualifier, and timestamp.
   *
   * @param id The entity id of the row.
   * @param qualifier The column qualifier at which the value will be written.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String qualifier, long timestamp, int value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), qualifier, Schema.create(Schema.Type.INT), timestamp, value);
  }

  /**
   * Writes an integer value to the specified row, qualifier, and timestamp.
   *
   * @param id The entity id of the row.
   * @param family The family to write to.
   * @param qualifier The column qualifier at which the value will be written.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String family, String qualifier, long timestamp, int value)
      throws IOException, InterruptedException {
    write(id, family, qualifier, Schema.create(Schema.Type.INT), timestamp, value);
  }

  /**
   * Writes a long value to the specified row.
   *
   * @param id The entity id of the row.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, long value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), getQualifierIfNotNull(), value);
  }

  /**
   * Writes a long value to the specified row at the specified timestamp.
   *
   * @param id The entity id of the row.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, long timestamp, long value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), getQualifierIfNotNull(), timestamp, value);
  }

  /**
   * Writes a long value to the specified row and qualifier.
   *
   * @param id The entity id of the row.
   * @param qualifier The column qualifier at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String qualifier, long value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), qualifier, Schema.create(Schema.Type.LONG), value);
  }

  /**
   * Writes a long value to the specified row and qualifier.
   *
   * @param id The entity id of the row.
   * @param family The family to write to.
   * @param qualifier The column qualifier at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String family, String qualifier, long value)
      throws IOException, InterruptedException {
    write(id, family, qualifier, Schema.create(Schema.Type.LONG), value);
  }

  /**
   * Writes a long value to the specified row, qualifier, and timestamp.
   *
   * @param id The entity id of the row.
   * @param qualifier The column qualifier at which the value will be written.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String qualifier, long timestamp, long value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), qualifier, Schema.create(Schema.Type.LONG), timestamp, value);
  }

  /**
   * Writes a long value to the specified row, qualifier, and timestamp.
   *
   * @param id The entity id of the row.
   * @param family The family to write to.
   * @param qualifier The column qualifier at which the value will be written.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String family, String qualifier, long timestamp, long value)
      throws IOException, InterruptedException {
    write(id, family, qualifier, Schema.create(Schema.Type.LONG), timestamp, value);
  }

  /**
   * Writes a float value to the specified row.
   *
   * @param id The entity id of the row.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, float value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), getQualifierIfNotNull(), value);
  }

  /**
   * Writes a float value to the specified row at the specified timestamp.
   *
   * @param id The entity id of the row.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, long timestamp, float value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), getQualifierIfNotNull(), timestamp, value);
  }

  /**
   * Writes a float value to the specified row and qualifier.
   *
   * @param id The entity id of the row.
   * @param qualifier The column qualifier at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String qualifier, float value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), qualifier, Schema.create(Schema.Type.FLOAT), value);
  }

  /**
   * Writes a float value to the specified row and qualifier.
   *
   * @param id The entity id of the row.
   * @param family The family to write to.
   * @param qualifier The column qualifier at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String family, String qualifier, float value)
      throws IOException, InterruptedException {
    write(id, family, qualifier, Schema.create(Schema.Type.FLOAT), value);
  }

  /**
   * Writes a float value to the specified row, qualifier, and timestamp.
   *
   * @param id The entity id of the row.
   * @param qualifier The column qualifier at which the value will be written.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String qualifier, long timestamp, float value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), qualifier, Schema.create(Schema.Type.FLOAT), timestamp, value);
  }

  /**
   * Writes a float value to the specified row, qualifier, and timestamp.
   *
   * @param id The entity id of the row.
   * @param family The family to write to.
   * @param qualifier The column qualifier at which the value will be written.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String family, String qualifier, long timestamp, float value)
      throws IOException, InterruptedException {
    write(id, family, qualifier, Schema.create(Schema.Type.FLOAT), timestamp, value);
  }

  /**
   * Writes a double value to the specified row.
   *
   * @param id The entity id of the row.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, double value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), getQualifierIfNotNull(), value);
  }

  /**
   * Writes a double value to the specified row at the specified timestamp.
   *
   * @param id The entity id of the row.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, long timestamp, double value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), getQualifierIfNotNull(), timestamp, value);
  }

  /**
   * Writes a double value to the specified row and qualifier.
   *
   * @param id The entity id of the row.
   * @param qualifier The column qualifier at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String qualifier, double value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), qualifier, Schema.create(Schema.Type.DOUBLE), value);
  }

  /**
   * Writes a double value to the specified row and qualifier.
   *
   * @param id The entity id of the row.
   * @param family The family to write to.
   * @param qualifier The column qualifier at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String family, String qualifier, double value)
      throws IOException, InterruptedException {
    write(id, family, qualifier, Schema.create(Schema.Type.DOUBLE), value);
  }

  /**
   * Writes a double value to the specified row, qualifier, and timestamp.
   *
   * @param id The entity id of the row.
   * @param qualifier The column qualifier at which the value will be written.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String qualifier, long timestamp, double value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), qualifier, Schema.create(Schema.Type.DOUBLE), timestamp, value);
  }

  /**
   * Writes a double value to the specified row, qualifier, and timestamp.
   *
   * @param id The entity id of the row.
   * @param family The family to write to.
   * @param qualifier The column qualifier at which the value will be written.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String family, String qualifier, long timestamp, double value)
      throws IOException, InterruptedException {
    write(id, family, qualifier, Schema.create(Schema.Type.DOUBLE), timestamp, value);
  }

  /**
   * Writes a byte array value to the specified row.
   *
   * @param id The entity id of the row.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, byte[] value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), getQualifierIfNotNull(), value);
  }

  /**
   * Writes a byte array value to the specified row at the specified timestamp.
   *
   * @param id The entity id of the row.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, long timestamp, byte[] value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), getQualifierIfNotNull(), timestamp, value);
  }

  /**
   * Writes a byte array value to the specified row and qualifier.
   *
   * @param id The entity id of the row.
   * @param qualifier The column qualifier at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String qualifier, byte[] value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), qualifier, Schema.create(Schema.Type.BYTES), ByteBuffer.wrap(
        value));
  }

  /**
   * Writes a byte array value to the specified row and qualifier.
   *
   * @param id The entity id of the row.
   * @param family The family to write to.
   * @param qualifier The column qualifier at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String family, String qualifier, byte[] value)
      throws IOException, InterruptedException {
    write(id, family, qualifier, Schema.create(Schema.Type.BYTES), ByteBuffer.wrap(value));
  }

  /**
   * Writes a byte array value to the specified row, qualifier, and timestamp.
   *
   * @param id The entity id of the row.
   * @param qualifier The column qualifier at which the value will be written.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String qualifier, long timestamp, byte[] value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), qualifier, Schema.create(Schema.Type.BYTES), timestamp,
        ByteBuffer.wrap(value));
  }

  /**
   * Writes a byte array value to the specified row, qualifier, and timestamp.
   *
   * @param id The entity id of the row.
   * @param family The family to write to.
   * @param qualifier The column qualifier at which the value will be written.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String family, String qualifier, long timestamp, byte[] value)
      throws IOException, InterruptedException {
    write(id, family, qualifier, Schema.create(Schema.Type.BYTES), timestamp,
        ByteBuffer.wrap(value));
  }

  /**
   * Writes a character sequence value to the specified row.
   *
   * @param id The entity id of the row.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, CharSequence value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), getQualifierIfNotNull(), value);
  }

  /**
   * Writes a character sequence value to the specified row at the specified timestamp.
   *
   * @param id The entity id of the row.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, long timestamp, CharSequence value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), getQualifierIfNotNull(), timestamp, value);
  }

  /**
   * Writes a character sequence value to the specified row and qualifier.
   *
   * @param id The entity id of the row.
   * @param qualifier The column qualifier at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String qualifier, CharSequence value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), qualifier, Schema.create(Schema.Type.STRING), value);
  }

  /**
   * Writes a character sequence value to the specified row and qualifier.
   *
   * @param id The entity id of the row.
   * @param family The family to write to.
   * @param qualifier The column qualifier at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String family, String qualifier, CharSequence value)
      throws IOException, InterruptedException {
    write(id, family, qualifier, Schema.create(Schema.Type.STRING), value);
  }

  /**
   * Writes a char sequence value to the specified row, qualifier, and timestamp.
   *
   * @param id The entity id of the row.
   * @param qualifier The column qualifier at which the value will be written.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String qualifier, long timestamp, CharSequence value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), qualifier, Schema.create(Schema.Type.STRING), timestamp, value);
  }

  /**
   * Writes a char sequence value to the specified row, qualifier, and timestamp.
   *
   * @param id The entity id of the row.
   * @param family The family to write to.
   * @param qualifier The column qualifier at which the value will be written.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String family, String qualifier, long timestamp,
      CharSequence value) throws IOException, InterruptedException {
    write(id, family, qualifier, Schema.create(Schema.Type.STRING), timestamp, value);
  }

  /**
   * Writes an Avro value to the specified row.
   *
   * @param id The entity id of the row.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, GenericContainer value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), getQualifierIfNotNull(), value);
  }

  /**
   * Writes an Avro value to the specified row at the specified timestamp.
   *
   * @param id The entity id of the row.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, long timestamp, GenericContainer value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), getQualifierIfNotNull(), timestamp, value);
  }

  /**
   * Writes an Avro value to the specified row and qualifier.
   *
   * @param id The entity id of the row.
   * @param qualifier The column qualifier at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String qualifier, GenericContainer value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), qualifier, value.getSchema(), value);
  }

  /**
   * Writes an Avro value to the specified row and qualifier.
   *
   * @param id The entity id of the row.
   * @param family The family to write to.
   * @param qualifier The column qualifier at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String family, String qualifier, GenericContainer value)
      throws IOException, InterruptedException {
    write(id, family, qualifier, value.getSchema(), value);
  }

  /**
   * Writes an Avro value to the specified row, qualifier, and timestamp.
   *
   * @param id The entity id of the row.
   * @param qualifier The column qualifier at which the value will be written.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String qualifier, long timestamp, GenericContainer value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), qualifier, value.getSchema(), timestamp, value);
  }

  /**
   * Writes an Avro value to the specified row, qualifier, and timestamp.
   *
   * @param id The entity id of the row.
   * @param family The family to write to.
   * @param qualifier The column qualifier at which the value will be written.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The value to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String family, String qualifier, long timestamp,
      GenericContainer value)
      throws IOException, InterruptedException {
    write(id, family, qualifier, value.getSchema(), timestamp, value);
  }

  /**
   * Writes a Kiji cell to the specified row.
   *
   * @param id The entity id of the row.
   * @param value The cell to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, KijiCell<? extends Object> value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), getQualifierIfNotNull(), value);
  }

  /**
   * Writes a Kiji cell to the specified row at the specified timestamp.
   *
   * @param id The entity id of the row.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The cell to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, long timestamp, KijiCell<? extends Object> value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), getQualifierIfNotNull(), timestamp, value);
  }

  /**
   * Writes a Kiji cell to the specified row and qualifier.
   *
   * @param id The entity id of the row.
   * @param qualifier The column qualifier at which the value will be written.
   * @param value The cell to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String qualifier, KijiCell<? extends Object> value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), qualifier, value.getWriterSchema(), value.getData());
  }

  /**
   * Writes a Kiji cell to the specified row and qualifier.
   *
   * @param id The entity id of the row.
   * @param family The family to write to.
   * @param qualifier The column qualifier at which the value will be written.
   * @param value The cell to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String family, String qualifier, KijiCell<? extends Object> value)
      throws IOException, InterruptedException {
    write(id, family, qualifier, value.getWriterSchema(), value.getData());
  }

  /**
   * Writes a Kiji cell to the specified row, qualifier, and timestamp.
   *
   * @param id The entity id of the row.
   * @param qualifier The column qualifier at which the value will be written.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The cell to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String qualifier, long timestamp, KijiCell<? extends Object> value)
      throws IOException, InterruptedException {
    write(id, getFamilyIfNotNull(), qualifier, value.getWriterSchema(), timestamp, value.getData());
  }

  /**
   * Writes a Kiji cell to the specified row, qualifier, and timestamp.
   *
   * @param id The entity id of the row.
   * @param family The family to write to.
   * @param qualifier The column qualifier at which the value will be written.
   * @param timestamp The timestamp at which the value will be written.
   * @param value The cell to write.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public void write(EntityId id, String family, String qualifier, long timestamp,
      KijiCell<? extends Object> value)
      throws IOException, InterruptedException {
    write(id, family, qualifier, value.getWriterSchema(), timestamp, value.getData());
  }

  /**
   * Writes a value at the current timestamp and reports progress.
   *
   * @param id The entity id of the row to write to.
   * @param family The family to write to.
   * @param qualifier The key (column qualifier) to write the data to.
   * @param schema The writer schema for the value.
   * @param value The data payload.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  private void write(EntityId id, String family, String qualifier, Schema schema, Object value)
      throws IOException, InterruptedException {
    write(id, family, qualifier, schema, HConstants.LATEST_TIMESTAMP, value);
  }

  /**
   * Writes a value at the current timestamp and reports progress.
   *
   * <p>All other write methods of Kiji data writers delegate to this method. Subclasses should
   * override this method to implement desired write functionality.</p>
   *
   * @param id The entity id of the row to write to.
   * @param family The family to write to.
   * @param qualifier The key (column qualifier) to write the data to.
   * @param schema The writer schema for the value.
   * @param timestamp The timestamp at which the data will be written.
   * @param value The data payload.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  protected abstract void write(EntityId id, String family, String qualifier, Schema schema,
      long timestamp, Object value) throws IOException, InterruptedException;

  /**
   * Performs any writes that have been buffered.
   *
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  public abstract void flush() throws InterruptedException, IOException;

  /**
   * Flushes any writes that have been buffered and releases any system resources used by this
   * writer.
   *
   * @throws IOException If there is a problem writing or closing resources.
   */
  @Override
  public abstract void close() throws IOException;
}
