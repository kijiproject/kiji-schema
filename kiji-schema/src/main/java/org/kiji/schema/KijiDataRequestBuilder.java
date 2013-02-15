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

import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.filter.KijiColumnFilter;

/**
 * <p>Builds a request for columns of data to read from a Kiji table.</p>
 *
 * <p>You can instantiate a KijiDataRequestBuilder using the {@link
 * KijiDataRequest#builder()} method.</p>
 *
 * <p>{@link KijiDataRequest} objects are immutable; this object helps you construct
 * them. With a KijiDataRequest builder, you can set various properties that affect
 * the data request as a whole (for example, the timestamp interval to retrieve).
 * You can also use the {@link #newColumnsDef()} method of this object to get instances of
 * {@link KijiDataRequestBuilder.ColumnsDef}, which allow you to define a set of columns that
 * are part of a data request, and their retrieval properties.</p>
 *
 * <p>A KijiDataRequestBuilder.ColumnsDef object has two types of methods: methods
 * starting with <code>with...</code> define properties associated with some columns. The
 * <code>add()</code> methods then attach specific columns to the data request being built,
 * using the properties previously specified. You may call <code>add()</code> or
 * <code>addFamily()</code> multiple times.
 * You may not overwrite the value of a property like maxVersions once it's already been set.</p>
 *
 * <p>It is an error to change properties within a column request (e.g., call {@link
 * KijiDataRequestBuilder.ColumnsDef#withMaxVersions(int)}) after using the
 * <code>add()</code> or <code>addFamily()</code> methods to add columns to the request.</p>
 *
 * <p>The following behaviors are errors and are not allowed:</p>
 * <ul>
 *   <li>Adding the same column multiple times. This includes definitions like
 *       <tt>info:name</tt> that conflict with requests for the entire
 *       <tt>info:*</tt> family.</li>
 *   <li>Calling a property-setting method (<tt>withTimeRange()</tt>,
 *       <tt>withMaxVersions()</tt>, etc) more than once on a given
 *       <tt>KijiDataRequestBuilder</tt> or {@link KijiDataRequestBuilder.ColumnsDef}.
 *       These methods will throw IllegalStateException.</li>
 *   <li>Changing any properties after calling build() to construct the KijiDataRequest.</li>
 *   <li>Calling build() more than once.</li>
 * </ul>
 *
 * <h2>Usage example</h2>
 *
 * <p>For the common case of reading <tt>info:foo</tt>, <tt>info:bar</tt>,
 * <tt>info:baz</tt>, and <tt>products:*</tt>:</p>
 * <pre>
 * KijiDataRequestBuilder builder = KijiDataRequest.builder().withTimeRange(t1, t2);
 * builder.newColumnsDef().withMaxVersions(42)
 *     .add("info", "foo")
 *     .add("info", "bar")
 *     .add("info", "baz")
 *     .addFamily("products");
 * KijiDataRequest request = builder.build();
 * </pre>
 *
 * This can also be written as:
 * <pre>
 * final KijiDataRequest request = KijiDataRequest.build()
 *     .addColumns(ColumnsDef.create()
 *         .withMaxVerions(42)
 *         .add("info", "foo")
 *         .add("info", "bar")
 *         .add("info", "baz")
 *         .addFamily("products"))
 *     .build();
 * </pre>
 *
 * <p>To add <tt>fam1:col1</tt>, <tt>fam1:col2</tt>, and <tt>fam2:*</tt>, each with
 * different retrieval properties to the same request, do the following:</p>
 * <pre>
 * KijiDataRequestBuilder builder = KijiDataRequest.builder().withTimeRange(t1, t2);
 * builder.newColumnsDef().withMaxVersions(10).withPageSize(p).add("fam1", "col1");
 * builder.newColumnsDef().add("fam1", "col2");
 * builder.newColumnsDef().withMaxVersions(42).addFamily("fam2");
 * KijiDataRequest request = builder.build();
 * </pre>
 */
@ApiAudience.Public
public final class KijiDataRequestBuilder {

  /** Column builders associated with this data request builder. */
  private Set<ColumnsDef> mColumnsDefs = Sets.newHashSet();

  /** The minimum timestamp of cells to be read (inclusive). */
  private long mMinTimestamp = KConstants.BEGINNING_OF_TIME;

  /** The maximum timestamp of cells to be read (exclusive). */
  private long mMaxTimestamp = KConstants.END_OF_TIME;

  /** True if the user already set timestamp range. */
  private boolean mIsTimeRangeSet;

  /** True if we already built an object. */
  private boolean mIsBuilt = false;

  /**
   * Defines properties associated with one or more columns in a request for Kiji table columns.
   *
   * <p>See {@link KijiDataRequestBuilder} for a larger specification of how
   * {@link KijiDataRequest} objects are constructed.</p>
   *
   * <p>Use the {@link KijiDataRequestBuilder#newColumnsDef()} method to get an instance of
   * a column set definition. This object has no "build()" method; use the {@link
   * KijiDataRequestBuilder#build()} method to build the entire {@link
   * KijiDataRequest} at once.</p>
   *
   * <p>It is an error to call one of the <tt>with...()</tt> methods after calling
   * <tt>add()</tt> to add a specific column definition. You must define all
   * properties of the columns before specifying particular columns to attach to
   * the KijiDataRequest.</p>
   *
   * <p>It is an error to request a column more than once in the same
   * KijiDataRequest.</p>
   */
  @ApiAudience.Public
  public static final class ColumnsDef {
    /** Becomes true when the columns definition is sealed. */
    private boolean mSealed = false;

    /** The maximum number of versions from the column to read (of the most recent). */
    private Integer mMaxVersions = null;

    /** A column filter (may be null). */
    private KijiColumnFilter mFilter = null;

    /** Becomes true once the filter is set. */
    private boolean mFilterInitialized = false;

    /** The number of cells per page (zero means no paging). */
    private Integer mPageSize;

    /** Columns in this definition. */
    private List<KijiColumnName> mColumns = Lists.newArrayList();

    /** Creates a new requested <code>ColumnsDef</code> builder. */
    private ColumnsDef() {
    }

    /** @return a new builder for column definitions. */
    public static ColumnsDef create() {
      return new ColumnsDef();
    }

    /** @return true if the user has already started assigning columns to this builder. */
    private boolean assignedColumns() {
      return !mColumns.isEmpty();
    }

    /** If the user has assigned columns to this builder, throw IllegalStateException. */
    private void checkNoCols() {
      Preconditions.checkState(!mSealed,
          "ColumnsDef cannot be used once KijiDataRequestBuilder.build() had been called.");
      Preconditions.checkState(!assignedColumns(),
          "Properties of the columns builder cannot be changed once columns are assigned to it.");
    }

    /**
     * Sets the maximum number of the most recent versions to return.
     *
     * @param maxVersions The maximum number of versions of the cell to read.
     * @return This column request builder instance.
     */
    public ColumnsDef withMaxVersions(int maxVersions) {
      checkNoCols();
      Preconditions.checkState(mMaxVersions == null,
          "Cannot set max versions to %d, max versions already set to %d.",
          maxVersions, mMaxVersions);
      Preconditions.checkArgument(maxVersions > 0,
          "Maximum number of versions must be strictly positive, but got: %d",
          maxVersions);

      mMaxVersions = maxVersions;
      return this;
    }

    /**
     * Sets a filter to attach to each column specified by this column request builder.
     *
     * @param filter The column filter;
     * @return This column request builder instance.
     */
    public ColumnsDef withFilter(KijiColumnFilter filter) {
      checkNoCols();
      Preconditions.checkState(!mFilterInitialized, "Cannot set filter multiple times");

      mFilter = filter;
      mFilterInitialized = true;
      return this;
    }

    /**
     * Sets the page size (i.e. the maximum number of cells per page).
     *
     * Defaults to zero, which means paging is disabled.
     *
     * @param pageSize The maximum number of cells to return in each page of results.
     *     Use 0 to disable paging and return all results at once.
     * @return This column request instance.
     */
    public ColumnsDef withPageSize(int pageSize) {
      checkNoCols();
      Preconditions.checkState(mPageSize == null,
          "Cannot set page size to %d, page size already set to %d.", pageSize, mPageSize);
      Preconditions.checkArgument(pageSize >= 0,
          "Page size must be 0 (disabled) or positive, but got: %d", mPageSize);

      mPageSize = pageSize;
      return this;
    }

    /**
     * Adds a column to the data request, using the properties associated with this
     * KijiDataRequestBuilder.ColumnsDef object. Once you call this method, you may not
     * call property-setting methods like withPageSize() on this same object.
     *
     * @param family the column family to retrieve as a map.
     * @return this column request builder instance.
     */
    public ColumnsDef addFamily(String family) {
      Preconditions.checkNotNull(family);
      Preconditions.checkArgument(!family.contains(":"),
          "Family name cannot contain ':', but got '%s'.", family);
      return add(new KijiColumnName(family, null));
    }

    /**
     * Adds a column to the data request, using the properties associated with this
     * KijiDataRequestBuilder.ColumnsDef object. Once you call this method, you may not
     * call property-setting methods like withPageSize() on this same object.
     *
     * @param family the column family of the column to retrieve.
     * @param qualifier the qualifier of the column to retrieve.
     * @return this column request builder instance.
     */
    public ColumnsDef add(String family, String qualifier) {
      return add(new KijiColumnName(family, qualifier));
    }

    /**
     * Adds a column to the data request, using the properties associated with this
     * KijiDataRequestBuilder.ColumnsDef object. Once you call this method, you may not
     * call property-setting methods like withPageSize() on this same object.
     *
     * @param column the column name to retrieve.
     * @return this column request builder instance.
     */
    public ColumnsDef add(KijiColumnName column) {
      Preconditions.checkState(!mSealed,
          "Cannot add more columns to this ColumnsDef after build() has been called.");
      mColumns.add(column);
      return this;
    }

    /**
     * Seals this columns definition.
     *
     * The definition becomes immutable, and the only method call allowed from there on is build().
     */
    private void seal() {
      Preconditions.checkState(!mSealed);
      mSealed = true;
    }

    /**
     * Builds the columns associated with this column builder.
     *
     * @return a list of constructed KijiDataRequest.Column objects associated with the
     *     output KijiDataRequest instance.
     */
    private List<KijiDataRequest.Column> buildColumns() {
      if (!mSealed) {
        mSealed = true;
      }

      // Values not previously initialized are now set to default values.
      // This builder is immutable after this method is called, so this is ok.
      if (mPageSize == null) {
        mPageSize = 0; // disable paging.
      }

      if (mMaxVersions == null) {
        mMaxVersions = 1;
      }

      final List<KijiDataRequest.Column> columns = Lists.newArrayListWithCapacity(mColumns.size());
      for (KijiColumnName column: mColumns) {
        columns.add(new KijiDataRequest.Column(
            column.getFamily(), column.getQualifier(), mMaxVersions, mFilter, mPageSize));
      }
      return columns;
    }
  }

  /**
   * Constructor. Package-private; use {@link KijiDataRequest#builder()} to get an
   * instance of this.
   */
  KijiDataRequestBuilder() {
  }

  /**
   * Sets the time range of cells to return: [<code>minTimestamp</code>,
   * <code>maxTimestamp</code>).
   *
   * @param minTimestamp Request cells with a timestamp at least minTimestamp.
   * @param maxTimestamp Request cells with a timestamp less than maxTimestamp.
   * @return This data request builder instance.
   */
  public KijiDataRequestBuilder withTimeRange(long minTimestamp, long maxTimestamp) {
    checkNotBuilt();
    Preconditions.checkArgument(minTimestamp >= 0,
        "minTimestamp must be positive or zero, but got: %d", minTimestamp);
    Preconditions.checkArgument(maxTimestamp > minTimestamp,
        "Invalid time range [%d--%d]", minTimestamp, maxTimestamp);
    Preconditions.checkState(!mIsTimeRangeSet,
        "Cannot set time range more than once.");

    mIsTimeRangeSet = true;
    mMinTimestamp = minTimestamp;
    mMaxTimestamp = maxTimestamp;
    return this;
  }

  /**
   * Return a builder for columns associated with this KijiDataRequestBuilder.
   *
   * <p>Creates an object that allows you to specify a set of related columns attached
   * to the same KijiDataRequest that all share the same retrieval properties, like
   * the number of max versions.</p>
   *
   * @return a new KijiDataRequestBuilder.ColumnsDef builder object associated with this
   *     data request builder.
   */
  public ColumnsDef newColumnsDef() {
    checkNotBuilt();
    final ColumnsDef c = new ColumnsDef();
    mColumnsDefs.add(c);
    return c;
  }

  /**
   * Return a builder for columns, initialized from an existing
   * {@link KijiDataRequest.Column}.
   *
   * <p>Creates an object that allows you to specify a set of related columns attached
   * to the same KijiDataRequest that all share the same retrieval properties, like
   * the number of max versions.</p>
   *
   * <p>This builder will have all properties fully initialized, and it will already
   * include a request for the column named as an argument. Only additional calls to
   * <code>KijiDataRequestBuilder.ColumnsDef.add(...)</code> are permitted.</p>
   *
   * @param existingColumn is a Column from an existing KijiDataRequest object that should
   *     be included in this new KijiDataRequest.
   * @return a new KijiDataRequestBuilder.ColumnsDef builder object associated with this
   *     data request builder.
   */
  public ColumnsDef newColumnsDef(KijiDataRequest.Column existingColumn) {
    return newColumnsDef()
        .withFilter(existingColumn.getFilter())
        .withPageSize(existingColumn.getPageSize())
        .withMaxVersions(existingColumn.getMaxVersions())
        .add(existingColumn.getFamily(), existingColumn.getQualifier());
  }

  /**
   * Adds another set of column definitions to this KijiDataRequest builder.
   *
   * <p>Columns added in this manner must not redefine any column definitions already included in
   * the KijiDataRequestBuilder. It is an error to add a ColumnsDef instance to multiple
   * KijiDataRequestBuilders.
   *
   * @param def A set of column definitions contained in a {@link KijiDataRequest.ColumnsDef}
   *     instance.
   * @return this KijiDataRequest builder.
   */
  public KijiDataRequestBuilder addColumns(ColumnsDef def) {
    def.seal();
    mColumnsDefs.add(def);
    return this;
  }

  /**
   * Construct a new KijiDataRequest based on the configuration specified in this builder
   * and its associated column builders.
   *
   * <p>After calling build(), you may not use the builder anymore.</p>
   *
   * @return a new KijiDataRequest object containing the column requests associated
   *     with this KijiDataRequestBuilder.
   */
  public KijiDataRequest build() {
    checkNotBuilt();
    mIsBuilt = true;

    // Entire families for which a definition has been recorded:
    final Set<String> families = Sets.newHashSet();

    // Fully-qualified columns for which a definition has been recorded:
    final Set<KijiColumnName> columns = Sets.newHashSet();

    // Families of fully-qualified columns for which definitions have been recorded:
    final Set<String> familiesOfColumns = Sets.newHashSet();

    final List<KijiDataRequest.Column> requestedColumns = Lists.newArrayList();
    for (ColumnsDef columnsDef : mColumnsDefs) {
      for (KijiDataRequest.Column column : columnsDef.buildColumns()) {
        if (column.getQualifier() == null) {
          final boolean isNotDuplicate = families.add(column.getFamily());
          Preconditions.checkState(isNotDuplicate,
              "Duplicate definition for family '%s'.", column.getFamily());

          Preconditions.checkState(!familiesOfColumns.contains(column.getFamily()),
              "KijiDataRequest may not simultaneously contain definitions for family '%s' "
              + "and definitions for fully qualified columns in family '%s'.",
              column.getFamily(), column.getFamily());

        } else {
          final boolean isNotDuplicate = columns.add(column.getColumnName());
          Preconditions.checkState(isNotDuplicate, "Duplicate definition for column '%s'.", column);

          Preconditions.checkState(!families.contains(column.getFamily()),
              "KijiDataRequest may not simultaneously contain definitions for family '%s' "
              + "and definitions for fully qualified columns '%s'.",
              column.getFamily(), column.getColumnName());
          familiesOfColumns.add(column.getFamily());
        }
        requestedColumns.add(column);
      }
    }
    return new KijiDataRequest(requestedColumns, mMinTimestamp, mMaxTimestamp);
  }

  /**
   * @throws IllegalStateException after the KijiDataRequest has been built with {@link #build()}.
   *     Prevents reusing this builder.
   */
  private void checkNotBuilt() {
    Preconditions.checkState(!mIsBuilt,
        "KijiDataRequest builder cannot be used after build() is invoked.");
  }
}
