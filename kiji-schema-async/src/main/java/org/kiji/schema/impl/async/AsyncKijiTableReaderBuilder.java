/**
 * (c) Copyright 2013 WibiData, Inc.
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
package org.kiji.schema.impl.hbase;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTableReaderBuilder;
import org.kiji.schema.layout.ColumnReaderSpec;

/** HBase implementation of KijiTableReaderBuilder. */
@ApiAudience.Private
public final class HBaseKijiTableReaderBuilder implements KijiTableReaderBuilder {

  /**
   * Create a new HBaseKijiTableReaderBuilder for the given HBaseKijiTable.
   *
   * @param table HBaseKijiTable for which to build a reader.
   * @return a new HBaseKijiTableReaderBuilder.
   */
  public static HBaseKijiTableReaderBuilder create(
      final HBaseKijiTable table
  ) {
    return new HBaseKijiTableReaderBuilder(table);
  }

  private final HBaseKijiTable mTable;
  private OnDecoderCacheMiss mOnDecoderCacheMiss = null;
  private Map<KijiColumnName, ColumnReaderSpec> mOverrides = null;
  private Multimap<KijiColumnName, ColumnReaderSpec> mAlternatives = null;

  /**
   * Initialize a new HBaseKijiTableReaderBuilder for the given HBaseKijiTable.
   *
   * @param table HBaseKijiTable for which to build a reader.
   */
  private HBaseKijiTableReaderBuilder(
      final HBaseKijiTable table
  ) {
    mTable = table;
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableReaderBuilder withOnDecoderCacheMiss(
      final OnDecoderCacheMiss behavior
  ) {
    Preconditions.checkNotNull(behavior, "OnDecoderCacheMiss behavior may not be null.");
    Preconditions.checkState(null == mOnDecoderCacheMiss,
        "OnDecoderCacheMiss behavior already set to: %s", mOnDecoderCacheMiss);
    mOnDecoderCacheMiss = behavior;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public OnDecoderCacheMiss getOnDecoderCacheMiss() {
    return mOnDecoderCacheMiss;
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableReaderBuilder withColumnReaderSpecOverrides(
      final Map<KijiColumnName, ColumnReaderSpec> overrides
  ) {
    Preconditions.checkNotNull(overrides, "ColumnReaderSpec overrides may not be null.");
    Preconditions.checkState(null == mOverrides,
        "ColumnReaderSpec overrides already set to: %s", mOverrides);
    mOverrides = ImmutableMap.copyOf(overrides);
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public Map<KijiColumnName, ColumnReaderSpec> getColumnReaderSpecOverrides() {
    return mOverrides;
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableReaderBuilder withColumnReaderSpecAlternatives(
      final Multimap<KijiColumnName, ColumnReaderSpec> alternatives
  ) {
    Preconditions.checkNotNull(alternatives, "ColumnReaderSpec alternatives may not be null.");
    Preconditions.checkState(null == mAlternatives,
        "ColumnReaderSpec alternatives already set to: %s", mAlternatives);
    mAlternatives = ImmutableMultimap.copyOf(alternatives);
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public Multimap<KijiColumnName, ColumnReaderSpec> getColumnReaderSpecAlternatives() {
    return mAlternatives;
  }

  /** {@inheritDoc} */
  @Override
  public HBaseKijiTableReader buildAndOpen() throws IOException {
    if (null == mOnDecoderCacheMiss) {
      mOnDecoderCacheMiss = DEFAULT_CACHE_MISS;
    }
    if (null == mOverrides) {
      mOverrides = DEFAULT_READER_SPEC_OVERRIDES;
    }
    if (null == mAlternatives) {
      mAlternatives = DEFAULT_READER_SPEC_ALTERNATIVES;
    }

    return HBaseKijiTableReader.createWithOptions(
        mTable, mOnDecoderCacheMiss, mOverrides, mAlternatives);
  }
}
