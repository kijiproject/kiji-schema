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

package org.kiji.schema.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataWrapper;
import org.kiji.schema.KijiTable;
import org.kiji.schema.WrappedDataWriter;

/**
 * Base class for writers that use the local java api to write.
 *
 * @param <WRAPPER> The {@link KijiDataWrapper} class used to wrap data elements.
 * @param <DATUM> The type of the wrapped data elements.
 */
public abstract class LocalApiWriter<WRAPPER extends KijiDataWrapper<DATUM>, DATUM>
    extends WrappedDataWriter<WRAPPER, DATUM> {
  private static final Logger LOG = LoggerFactory.getLogger(LocalApiWriter.class);

  /** The kiji instance the table is in. */
  private final Kiji mKiji;
  /** The kiji table instance. */
  private final HBaseKijiTable mTable;

  /**
   * Class containing the options for a {@link WrappedDataWriter}.
   *
   * @param <WRAPPER> The {@link KijiDataWrapper} class used to wrap data elements.
   */
  public static class Options<WRAPPER extends KijiDataWrapper<?>>
      extends WrappedDataWriter.Options<WRAPPER> {
    private KijiTable mKijiTable;

    /**
     * @param kijiTable The kiji table instance.<br/>
     *     NOTE: When passing this writer a KijiTable object, this class does not close the table
     *     object for you.
     * @return This object to allow chaining of setter methods.
     */
    public Options<WRAPPER> withKijiTable(KijiTable kijiTable) {
      mKijiTable = kijiTable;
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public Options<WRAPPER> withMaxBufferedWrites(int maxBufferedWrites) {
      super.withMaxBufferedWrites(maxBufferedWrites);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public Options<WRAPPER> withWrapper(WRAPPER wrapper) {
      super.withWrapper(wrapper);
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

    /** @return The kiji table instance. */
    public KijiTable getKijiTable() {
      return mKijiTable;
    }
  }

  /**
   * Creates an instance.
   *
   * @param options The options to create the data writer with.
   */
  protected LocalApiWriter(Options<WRAPPER> options) {
    super(options);
    KijiTable table = options.getKijiTable();
    mKiji = table.getKiji();
    mTable = HBaseKijiTable.downcast(table);
  }

  /**
   * Creates a new instance by copying the options from another writer.
   *
   * @param original The data writer whose options should be copied.
   */
  protected LocalApiWriter(LocalApiWriter<WRAPPER, DATUM> original) {
    super(original);
    mKiji = original.getKiji();
    mTable = original.getTable();
  }

  /** @return The kiji instance the table is in. */
  protected Kiji getKiji() {
    return mKiji;
  }

  /** @return The kiji table instance. */
  protected HBaseKijiTable getTable() {
    return mTable;
  }
}
