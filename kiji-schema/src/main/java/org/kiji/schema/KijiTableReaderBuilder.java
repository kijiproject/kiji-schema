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
package org.kiji.schema;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.layout.ColumnReaderSpec;

/**
 * Builder for {@link org.kiji.schema.KijiTableReader}s with options.
 *
 * <p>Options include:</p>
 * <ul>
 *   <li>
 *     Setting the behavior of a reader when a cell decoder cannot be found in the decoder cache.
 *   </li>
 *   <li>Setting a map of overriding default read behavior per column.</li>
 *   <li>
 *     Setting a map of alternative read behaviors which will be included in the decoder cache, but
 *     which will not change default read behavior.
 *   </li>
 * </ul>
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
public interface KijiTableReaderBuilder {

  /**
   * By default, build and cache new cell decoders for unknown
   * {@link org.kiji.schema.layout.ColumnReaderSpec} overrides.
   */
  OnDecoderCacheMiss DEFAULT_CACHE_MISS = OnDecoderCacheMiss.BUILD_AND_CACHE;

  /** By default, do not override any column read behaviors. */
  Map<KijiColumnName, ColumnReaderSpec> DEFAULT_READER_SPEC_OVERRIDES = ImmutableMap.of();

  /** By default, do not include any alternate column reader specs. */
  Multimap<KijiColumnName, ColumnReaderSpec> DEFAULT_READER_SPEC_ALTERNATIVES =
      ImmutableSetMultimap.of();

  /**
   * Optional behavior when a {@link ColumnReaderSpec} override specified in a
   * {@link KijiDataRequest} used with this reader is not found in the prebuilt cache of cell
   * decoders. Default is BUILD_AND_CACHE.
   */
  enum OnDecoderCacheMiss {
    /** Throw an exception to indicate that the override is not supported. */
    FAIL,
    /** Build a new cell decoder based on the override and store it to the cache. */
    BUILD_AND_CACHE,
    /** Build a new cell decoder based on the override, but do not store it to the cache. */
    BUILD_DO_NOT_CACHE
  }

  /**
   * Configure the KijiTableReaderOptions to include the given OnDecoderCacheMiss behavior. If unset
   * this option defaults to
   * {@link org.kiji.schema.KijiTableReaderBuilder.OnDecoderCacheMiss#BUILD_AND_CACHE}.
   *
   * @param behavior OnDecoderCacheMiss behavior to use when a {@link ColumnReaderSpec} override
   *     specified in a {@link KijiDataRequest} cannot be found in the prebuilt cache of cell
   *     decoders.
   * @return this builder.
   */
  KijiTableReaderBuilder withOnDecoderCacheMiss(OnDecoderCacheMiss behavior);

  /**
   * Get the configured OnDecoderCacheMiss behavior or null if none has been set.
   *
   * @return the configured OnDecoderCacheMiss behavior or null if none has been set.
   */
  OnDecoderCacheMiss getOnDecoderCacheMiss();

  /**
   * Configure the KijiTableReaderOptions to include the given ColumnReaderSpec overrides. These
   * ColumnReaderSpecs will be used to determine read behavior for associated columns. These
   * overrides will change the default behavior of the associated column when read by this
   * reader, even when no ColumnReaderSpec is specified in a KijiDataRequest. If unset this option
   * defaults to not override any columns.
   *
   * @param overrides mapping from columns to overriding read behavior for those columns.
   * @return this builder.
   */
  KijiTableReaderBuilder withColumnReaderSpecOverrides(
      Map<KijiColumnName, ColumnReaderSpec> overrides);

  /**
   * Get the configured ColumnReaderSpec overrides or null if none have been set.
   *
   * @return the configured ColumnReaderSpec overrides or null if none have been set.
   */
  Map<KijiColumnName, ColumnReaderSpec> getColumnReaderSpecOverrides();

  /**
   * Configure the KijiTableReaderOptions to include the given ColumnReaderSpecs as alternate
   * reader schema options for the associated columns. Setting these alternatives does not
   * change the behavior of associated columns when ColumnReaderSpecs are not included in
   * KijiDataRequests. ColumnReaderSpecs included here can be used as reader spec overrides in
   * KijiDataRequests without triggering {@link OnDecoderCacheMiss#FAIL} and without the cost
   * associated with constructing a new cell decoder. If unset this option defaults to not create
   * any alternative cell decoders.
   *
   * <p>
   *   Note: ColumnReaderSpec overrides provided to
   *   {@link #withColumnReaderSpecOverrides(java.util.Map)} should not be duplicated here.
   * </p>
   *
   * @param alternatives mapping from columns to reader spec alternatives which the
   *     KijiTableReader will accept as overrides in data requests.
   * @return this builder.
   */
  KijiTableReaderBuilder withColumnReaderSpecAlternatives(
      Multimap<KijiColumnName, ColumnReaderSpec> alternatives);

  /**
   * Get the configured ColumnReaderSpec alternatives or null if none have been set.
   *
   * @return the configured ColumnReaderSpec alternatives or null if none have been set.
   */
  Multimap<KijiColumnName, ColumnReaderSpec> getColumnReaderSpecAlternatives();

  /**
   * Build a new KijiTableReaderOptions from the values set in this builder. The user is responsible
   * for closing this reader when it is no longer needed.
   *
   * @return a new KijiTableReaderOptions from the values set in this builder.
   * @throws IOException in case of an error building the reader.
   */
  KijiTableReader buildAndOpen() throws IOException;
}
