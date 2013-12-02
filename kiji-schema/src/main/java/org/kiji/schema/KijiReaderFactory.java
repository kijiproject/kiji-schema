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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.ColumnReaderSpec;

/**
 * Interface for table reader factories.
 *
 * <p> Use <code>KijiTable.getReaderFactory()</code> to get a reader.
 */
@ApiAudience.Public
@ApiStability.Experimental
public interface KijiReaderFactory {

  /** Options governing behavior of KijiTableReader instances. */
  public static final class KijiTableReaderOptions {

    /** A KijiTableReaderOptions instance with all values set to default. */
    public static final KijiTableReaderOptions ALL_DEFAULTS = Builder.create().build();

    /** Builder for KijiTableReaderOptions. */
    public static final class Builder {

      /**
       * By default, build and cache new cell decoders for unknown {@link ColumnReaderSpec}
       * overrides.
       */
      public static final OnDecoderCacheMiss DEFAULT_CACHE_MISS =
          OnDecoderCacheMiss.BUILD_AND_CACHE;

      /** By default, do not override any column read behaviors. */
      public static final Map<KijiColumnName, ColumnReaderSpec> DEFAULT_READER_SPEC_OVERRIDES =
          Maps.newHashMap();

      /** By default, do not include any alternate column reader specs. */
      public static final Multimap<KijiColumnName, ColumnReaderSpec>
          DEFAULT_READER_SPEC_ALTERNATIVES = ImmutableSetMultimap.of();

      /**
       * Create a new Builder for KijiTableReaderOptions.
       *
       * @return a new Builder.
       */
      public static Builder create() {
        return new Builder();
      }

      private OnDecoderCacheMiss mOnDecoderCacheMiss = null;
      private Map<KijiColumnName, ColumnReaderSpec> mOverrides = null;
      private Multimap<KijiColumnName, ColumnReaderSpec> mAlternatives = null;

      /**
       * Configure the KijiTableReaderOptions to include the given OnDecoderCacheMiss behavior.
       *
       * @param behavior OnDecoderCacheMiss behavior to use when a {@link ColumnReaderSpec} override
       *     specified in a {@link KijiDataRequest} cannot be found in the prebuilt cache of cell
       *     decoders.
       * @return this builder.
       */
      public Builder withOnDecoderCacheMiss(
          final OnDecoderCacheMiss behavior
      ) {
        Preconditions.checkNotNull(behavior, "OnDecoderCacheMiss behavior may not be null.");
        Preconditions.checkState(null == mOnDecoderCacheMiss,
            "OnDecoderCacheMiss is already set to: " + mOnDecoderCacheMiss);
        mOnDecoderCacheMiss = behavior;
        return this;
      }

      /**
       * Get the configured OnDecoderCacheMiss behavior or null if none has been set.
       *
       * @return the configured OnDecoderCacheMiss behavior or null if none has been set.
       */
      public OnDecoderCacheMiss getOnDecoderCacheMiss() {
        return mOnDecoderCacheMiss;
      }

      /**
       * Configure the KijiTableReaderOptions to include the given ColumnReaderSpec overrides. These
       * ColumnReaderSpecs will be used to determine read behavior for associated columns. These
       * overrides will change the default behavior of the associated column when read by this
       * reader, even when no ColumnReaderSpec is specified in a KijiDataRequest.
       *
       * @param overrides mapping from columns to overriding read behavior for those columns.
       * @return this builder.
       */
      public Builder withColumnReaderSpecOverrides(
          final Map<KijiColumnName, ColumnReaderSpec> overrides
      ) {
        Preconditions.checkNotNull(overrides, "ColumnReaderSpec overrides may not be null.");
        Preconditions.checkState(
            null == mOverrides, "ColumnReaderSpec overrides are already set to: " + mOverrides);
        mOverrides = overrides;
        return this;
      }

      /**
       * Get the configured ColumnReaderSpec overrides or null if none have been set.
       *
       * @return the configured ColumnReaderSpec overrides or null if none have been set.
       */
      public Map<KijiColumnName, ColumnReaderSpec> getColumnReaderSpecOverrides() {
        return mOverrides;
      }

      /**
       * Configure the KijiTableReaderOptions to include the given ColumnReaderSpecs as alternate
       * reader schema options for the associated columns. Setting these alternatives does not
       * change the behavior of associated columns when ColumnReaderSpecs are not included in
       * KijiDataRequests. ColumnReaderSpecs included here can be used as reader spec overrides in
       * KijiDataRequests without triggering {@link OnDecoderCacheMiss#FAIL} and without the cost
       * associated with constructing a new cell decoder.
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
      public Builder withColumnReaderSpecAlternatives(
          final Multimap<KijiColumnName, ColumnReaderSpec> alternatives
      ) {
        Preconditions.checkNotNull(alternatives, "ColumnReaderSpec alternatives may not be null.");
        Preconditions.checkState(null == mAlternatives,
            "ColumnReaderSpec alternatives already set to: " + mAlternatives);
        mAlternatives = alternatives;
        return this;
      }

      /**
       * Get the configured ColumnReaderSpec alternatives or null if none have been set.
       *
       * @return the configured ColumnReaderSpec alternatives or null if none have been set.
       */
      public Multimap<KijiColumnName, ColumnReaderSpec> getColumnReaderSpecAlternatives() {
        return mAlternatives;
      }

      /**
       * Build a new KijiTableReaderOptions from the values set in this builder.
       *
       * @return a new KijiTableReaderOptions from the values set in this builder.
       */
      public KijiTableReaderOptions build() {
        if (null == mOnDecoderCacheMiss) {
          mOnDecoderCacheMiss = DEFAULT_CACHE_MISS;
        }
        if (null == mOverrides) {
          mOverrides = DEFAULT_READER_SPEC_OVERRIDES;
        }
        if (null == mAlternatives) {
          mAlternatives = DEFAULT_READER_SPEC_ALTERNATIVES;
        }

        return new KijiTableReaderOptions(mOnDecoderCacheMiss, mOverrides, mAlternatives);
      }
    }

    /**
     * Optional behavior when a {@link ColumnReaderSpec} override specified in a
     * {@link KijiDataRequest} used with this reader is not found in the prebuilt cache of cell
     * decoders. Default is BUILD_AND_CACHE.
     */
    public static enum OnDecoderCacheMiss {
      /** Throw an exception to indicate that the override is not supported. */
      FAIL,
      /** Build a new cell decoder based on the override and store it to the cache. */
      BUILD_AND_CACHE,
      /** Build a new cell decoder based on the override, but do not store it to the cache. */
      BUILD_DO_NOT_CACHE
    }

    /**
     * Convenience method for creating a KijiTableReaderOptions with the given OnDecoderCacheMiss
     * behavior and defaults for all other options.
     *
     * @param behavior Behavior of the reader when a cell decoder cannot be found for a
     *     {@link ColumnReaderSpec} override specified in a {@link KijiDataRequest}.
     * @return a new KijiTableReaderOptions with the given OnDecoderCacheMiss behavior and defaults
     *     for all other options.
     */
    public static KijiTableReaderOptions createWithOnDecoderCacheMiss(
        final OnDecoderCacheMiss behavior
    ) {
      return Builder.create().withOnDecoderCacheMiss(behavior).build();
    }

    /**
     * Convenience method for creating a KijiTableReaderOptions with the given
     * {@link ColumnReaderSpec} overrides and defaults for all other options.
     *
     * @param overrides mapping from columns to overriding read behavior for those columns. These
     *     overrides will change the default behavior of the associated column when read by this
     *     reader, even when no ColumnReaderSpec is specified in a KijiDataRequest.
     * @return a new KijiTableReaderOptions with the given read behavior overrides and defaults for
     *     all other options.
     */
    public static KijiTableReaderOptions createWithColumnReaderSpecOverrides(
        final Map<KijiColumnName, ColumnReaderSpec> overrides
    ) {
      return Builder.create().withColumnReaderSpecOverrides(overrides).build();
    }

    /**
     * Convenience method for creating a KijiTableReaderOptions with the given
     * {@link ColumnReaderSpec} alternatives and defaults for all other options.
     *
     * @param alternatives mapping from columns to alternative read behaviors for those columns.
     *     These alternatives will not change the default read behavior of the associated column.
     * @return a new KijiTableReaderOptions with the given read behavior alternatives and defaults
     *     for all other options.
     */
    public static KijiTableReaderOptions createWithColumnReaderSpecAlternatives(
        final Multimap<KijiColumnName, ColumnReaderSpec> alternatives
    ) {
      return Builder.create().withColumnReaderSpecAlternatives(alternatives).build();
    }

    private final OnDecoderCacheMiss mOnDecoderCacheMiss;
    private final Map<KijiColumnName, ColumnReaderSpec> mOverrides;
    private final Multimap<KijiColumnName, ColumnReaderSpec> mAlternates;

    /**
     * Initializes a new KijiTableReaderOptions.
     *
     * @param onDecoderCacheMiss behavior of the reader when a cell decoder cannot be found for a
     *     {@link ColumnReaderSpec} override specified in a {@link KijiDataRequest}.
     * @param overrides mapping from columns to overriding read behavior for those columns.
     * @param alternates mapping from columns to alternative read behaviors for those columns.
     */
    private KijiTableReaderOptions(
        final OnDecoderCacheMiss onDecoderCacheMiss,
        final Map<KijiColumnName, ColumnReaderSpec> overrides,
        final Multimap<KijiColumnName, ColumnReaderSpec> alternates
    ) {
      mOnDecoderCacheMiss = onDecoderCacheMiss;
      mOverrides = overrides;
      mAlternates = alternates;
    }

    /**
     * Get the {@link OnDecoderCacheMiss} behavior from these options.
     *
     * @return the {@link OnDecoderCacheMiss} behavior from these options.
     */
    public OnDecoderCacheMiss getOnDecoderCacheMiss() {
      return mOnDecoderCacheMiss;
    }

    /**
     * Get the column reader behavior overrides from these options.
     *
     * @return the column reader behavior overrides from these options.
     */
    public Map<KijiColumnName, ColumnReaderSpec> getColumnReaderSpecOverrides() {
      return mOverrides;
    }

    /**
     * Get the column reader spec alternatives from these options.
     *
     * @return the column reader spec alternatives from these options.
     */
    public Multimap<KijiColumnName, ColumnReaderSpec> getColumnReaderSpecAlternatives() {
      return mAlternates;
    }
  }

  /**
   * Get the table from which readers built by this factory will read.
   *
   * @return the table from which readers built by this factory will read.
   */
  KijiTable getTable();

  /**
   * Opens a new reader for the KijiTable associated with this reader factory.
   *
   * <p> The caller of this method is responsible for closing the reader. </p>
   * <p>
   *   The reader returned by this method does not provide any isolation guarantee.
   *   In particular, you should assume that the underlying resources (connections, buffers, etc)
   *   are used concurrently for other purposes.
   * </p>
   *
   * @return a new KijiTableReader.
   * @throws IOException on I/O error.
   */
  KijiTableReader openTableReader() throws IOException;

  /**
   * Opens a new reader for the KijiTable associated with this reader factory.
   *
   * <p>
   *   This factory method lets the user customize the layout of the table.
   *   In particular, for each column, the user may:
   *   <ul>
   *     <li> choose between generic and specific Avro records. </li>
   *     <li> specify different Avro reader schemas. </li>
   *     <li> request the Avro writer schemas (this forces using generic records). </li>
   *   </ul>
   * </p>
   *
   * <p>
   *   By default, the reader attempts to use Avro specific records if they are available
   *   on the classpath, and falls back to using generic records if a specific record
   *   is not available on the classpath.
   * </p>
   *
   * <p>
   *   Note: layout customizations are overlaid on top of the table layout without modifying
   *   the actual layout of the table.
   * </p>
   *
   * <h1> Examples </h1>
   *
   * <h2> Overriding an Avro reader schema </h2>
   *
   * You may override the Avro reader schema used to decode a column with
   * {@link CellSpec#setReaderSchema(org.apache.avro.Schema)}:
   *
   * <pre><tt>{@code
   *   final KijiTable table = ...
   *   final KijiColumnName column = new KijiColumName("family", "qualifier");
   *   // Force the Avro reader schema for family:qualifier to be this schema:
   *   final Schema myReaderSchema = ...
   *
   *   final Map<KijiColumnName, CellSpec> overrides = ImmutableMap.builder()
   *       .put(column, table.getLayout().getCellSpec(column).setReaderSchema(myReaderSchema))
   *       .build();
   *   final KijiTableReader reader = table.getReaderFactory().openTableReader(overrides);
   *   try {
   *      ...
   *   } finally {
   *     reader.close();
   *   }
   * }</tt></pre>
   *
   * <h2> Decoding cells using the Avro writer schemas </h2>
   *
   * You may configured cells of a column to be decoded using the writer schemas using
   * {@link CellSpec#setUseWriterSchema()}:
   *
   * <pre><tt>{@code
   *   final KijiTable table = ...
   *   final KijiColumnName column = new KijiColumName("family", "qualifier");
   *
   *   final Map<KijiColumnName, CellSpec> overrides = ImmutableMap.builder()
   *       .put(column, table.getLayout().getCellSpec(column).setUseWriterSchema())
   *       .build();
   *   final KijiTableReader reader = table.getReaderFactory().openTableReader(overrides);
   *   try {
   *      ...
   *   } finally {
   *     reader.close();
   *   }
   * }</tt></pre>
   *
   * <p> Note:
   *     when a reader is configured to decode a column using the Avro writer schemas of each cell,
   *     each decoded cell may have a different schema. For this reason, we enforce the use of
   *     generic records in this case.
   * </p>
   *
   * <h2> Decoding cells as Avro generic records </h2>
   *
   * You may configure cells from a column to be decoded as Avro generic records with
   * {@link CellSpec#setDecoderFactory(KijiCellDecoderFactory)}:
   *
   * <pre><tt>{@code
   *   final KijiTable table = ...
   *   final KijiColumnName column = new KijiColumName("family", "qualifier");
   *
   *   final Map<KijiColumnName, CellSpec> overrides = ImmutableMap.builder()
   *       .put(column, table.getLayout().getCellSpec(column)
   *           .setDecoderFactory(GenericCellDecoderFactor.get()))
   *       .build();
   *   final KijiTableReader reader = table.getReaderFactory().openTableReader(overrides);
   *   try {
   *     final KijiDataRequest dataRequest = KijiDataRequest.builder()
   *         .addColumns(ColumnsDef.create().add("family", "qualifier"))
   *         .build();
   *     final EntityId entityId = table.getEntityId(...);
   *     final KijiRowData row = reader.get(entityId, dataRequest);
   *     final GenericRecord record = row.getMostRecentValue("family", "qualifier");
   *     ...
   *   } finally {
   *     reader.close();
   *   }
   * }</tt></pre>
   *
   * <p>
   *   This customization will force the use Avro generic records even if appropriate Avro specific
   *   records are available on the classpath.
   * </p>
   *
   * @param overrides Map of column specifications overriding the actual table layout.
   * @return a new KijiTableReader.
   * @throws IOException on I/O error.
   */
  KijiTableReader openTableReader(Map<KijiColumnName, CellSpec> overrides) throws IOException;

  /**
   * Opens a new reader for the KijiTable associated with this reader factory.
   *
   * @param options configuration for the KijiTableReader.
   * @return a new KijiTableReader.
   * @throws IOException in case of an error opening the reader.
   */
  KijiTableReader openTableReader(KijiTableReaderOptions options) throws IOException;
}
