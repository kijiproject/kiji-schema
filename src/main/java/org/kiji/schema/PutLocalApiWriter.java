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

import org.apache.hadoop.hbase.client.Put;

import org.kiji.schema.impl.LocalApiWriter;
import org.kiji.schema.layout.ColumnNameTranslator;

/**
 * A Kiji data writer that outputs writes to the local Kiji table using the Kiji java API.
 */
public class PutLocalApiWriter
    extends LocalApiWriter<PutWrapper, Put> {
  /**
   * Describes the options that can be configured on the {@link PutLocalApiWriter}.
   */
  public static class Options extends LocalApiWriter.Options<PutWrapper> {
    private KijiCellEncoder mCellEncoder;
    private ColumnNameTranslator mColumnNameTranslator;

    /**
     * @param cellEncoder The cell encoder to use while wrapping data.
     * @return This object to allow chaining of setter methods.
     */
    public Options withCellEncoder(KijiCellEncoder cellEncoder) {
      mCellEncoder = cellEncoder;
      return this;
    }

    /**
     * @param columnNameTranslator The translator used to convert between HTable column names and
     *     Kiji column names.
     * @return This object to allow chaining of setter methods.
     */
    public Options withColumnNameTranslator(ColumnNameTranslator columnNameTranslator) {
      mColumnNameTranslator = columnNameTranslator;
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public Options withKijiTable(KijiTable kijiTable) {
      super.withKijiTable(kijiTable);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public Options withMaxBufferedWrites(int maxBufferedWrites) {
      super.withMaxBufferedWrites(maxBufferedWrites);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public Options withWrapper(PutWrapper wrapper) {
      super.withWrapper(wrapper);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public Options withEntityIdFactory(EntityIdFactory entityIdFactory) {
      super.withEntityIdFactory(entityIdFactory);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public Options withFamily(String family) {
      super.withFamily(family);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public Options withQualifier(String qualifier) {
      super.withQualifier(qualifier);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public PutWrapper getWrapper() {
      if (null != super.getWrapper()) {
        return super.getWrapper();
      } else {
        return new PutWrapper(mCellEncoder, mColumnNameTranslator);
      }
    }
  }

  /**
   * Creates an instance.
   *
   * @param options The options to create the data writer with.
   */
  public PutLocalApiWriter(LocalApiWriter.Options<PutWrapper> options) {
    super(options);
  }

  /**
   * Creates a new instance of PutLocalApiWriter by copying the options from another writer.
   *
   * @param original The data writer whose options should be copied.
   */
  public PutLocalApiWriter(PutLocalApiWriter original) {
    super(original);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void doWrite(Put wrapped) throws IOException {
    getTable().getHTable().put(wrapped);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush() throws InterruptedException, IOException {
    super.flush();
    getTable().getHTable().flushCommits();
    getKiji().getSchemaTable().flush();
  }
}
