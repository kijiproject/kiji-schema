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

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;

/**
 * Base class for writers of Kiji data writers that use a wrapper to aid with encapsulating and
 * buffering data.
 *
 * @param <WRAPPER> The {@link KijiDataWrapper} class used to wrap data elements.
 * @param <DATUM> The type of the wrapped data elements.
 */
public abstract class WrappedDataWriter<WRAPPER extends KijiDataWrapper<DATUM>, DATUM>
    extends KijiDataWriter {
  /**
   * The maximum number of writes buffered by this data writer.
   *
   * <p>Set this to 0 to disable buffering.</p>
   */
  private final int mMaxBufferedWrites;
  /** Wrapper used to wrap datum before writing it to a table. */
  private final WRAPPER mWrapper;

  /**
   * Describes the options that can be configured on the {@link WrappedDataWriter}.
   *
   * @param <WRAPPER> The {@link KijiDataWrapper} class used to wrap data elements.
   */
  public static class Options<WRAPPER extends KijiDataWrapper<?>> extends KijiDataWriter.Options {
    private int mMaxBufferedWrites;
    private WRAPPER mWrapper;

    /**
     * @param maxBufferedWrites The maximum number of writes buffered by this data writer.
     * <p>Set this to 0 to disable buffering.</p>
     * @return This object to allow chaining of setter methods.
     */
    public Options<WRAPPER> withMaxBufferedWrites(int maxBufferedWrites) {
      mMaxBufferedWrites = maxBufferedWrites;
      return this;
    }

    /**
     * @param wrapper Wrapper used to wrap datum before writing it to a table.
     * @return This object to allow chaining of setter methods.
     */
    public Options<WRAPPER> withWrapper(WRAPPER wrapper) {
      mWrapper = wrapper;
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public Options<WRAPPER> withEntityIdFactory(EntityIdFactory entityIdFactory) {
      super.withEntityIdFactory(entityIdFactory);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public Options<WRAPPER> withFamily(String family) {
      super.withFamily(family);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public Options<WRAPPER> withQualifier(String qualifier) {
      super.withQualifier(qualifier);
      return this;
    }

    /** @return The maximum number of writes buffered by this data writer. */
    public int getMaxBufferedWrites() {
      return mMaxBufferedWrites;
    }

    /** @return The wrapper used to wrap datum before writing it to a table. */
    public WRAPPER getWrapper() {
      return mWrapper;
    }
  }

  /**
   * Creates an instance.
   *
   * @param options The options to create the data writer with.
   */
  protected WrappedDataWriter(Options<WRAPPER> options) {
    super(options);
    mMaxBufferedWrites = options.getMaxBufferedWrites();
    mWrapper = options.getWrapper();
  }

  /**
   * Creates a new instance by copying the options from another writer.
   *
   * @param original The data writer whose options should be copied.
   */
  protected WrappedDataWriter(WrappedDataWriter<WRAPPER, DATUM> original) {
    super(original);
    mMaxBufferedWrites = original.mMaxBufferedWrites;
    mWrapper = original.mWrapper;
  }

  /**
   * The maximum number of writes buffered by this data writer. <p>Set this to 0 to disable
   * buffering.</p>
   *
   * @return the mMaxBufferedWrites
   */
  protected int getMaxBufferedWrites() {
    return mMaxBufferedWrites;
  }

  /**
   * @return The wrapper used to wrap datum before writing it to a table.
   */
  protected WRAPPER getWrapper() {
    return mWrapper;
  }

  /**
   * @return The number of elements in the this writer's buffer.
   */
  public int getNumBufferedElements() {
    return mWrapper.size();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void write(EntityId id, String family, String qualifier, Schema schema, long timestamp,
      Object value) throws IOException, InterruptedException {
    checkColumn(family, qualifier);

    if (0 == getMaxBufferedWrites()) {
      // No buffering.
      doWrite(mWrapper.wrap(id, family, qualifier, schema, timestamp, value));
    } else {
      // Add to buffer.
      mWrapper.add(id, family, qualifier, schema, timestamp, value);

      // Flush buffer if reached buffer size.
      if (mWrapper.size() >= getMaxBufferedWrites()) {
        flush();
      }
    }
  }

  /**
   * Writes a value at the current timestamp and reports progress.
   *
   * <p>All other write methods of Kiji data writers delegate to this method. Subclasses should
   * override this method to implement desired write functionality.</p>
   *
   * @param wrapped The wrapped data to write out.
   * @throws IOException If there is a problem writing.
   * @throws InterruptedException If the thread is interrupted while writing.
   */
  protected abstract void doWrite(DATUM wrapped) throws IOException, InterruptedException;

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush() throws InterruptedException, IOException {
    List<DATUM> wrapped = mWrapper.getWrapped();
    for (DATUM data : wrapped) {
      doWrite(data);
    }
    mWrapper.clear();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    // Overriding {@link Closeable#close()} only allows for throwing an IOException, but we could
    // throw an InterruptedException while flushing to the underlying context.
    try {
      flush();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}
