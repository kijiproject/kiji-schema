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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
 * You can also use the {@link #column()} method of this object to get instances of
 * {@link KijiDataRequestBuilder.Column}, which allow you to define the columns that
 * are part of a data request, and their retrieval properties.</p>
 *
 * <p>A KijiDataRequestBuilder.Column object has two types of methods: methods
 * starting with <code>with...</code> define properties associated with a column. The
 * <code>add()</code> methods then attach columns to the data request being built,
 * using the properties previously specified. You may call <code>add()</code>
 * multiple times. You may not overwrite the value of a property like maxVersions
 * once it's already been set.</p>
 *
 * <p>It is an error to change properties within a column request (e.g., call {@link
 * KijiDataRequestBuilder.Column#withMaxVersions(int)}) after using the
 * <code>add()</code> method to add columns to the request.</p>
 *
 * <p>The following behaviors are errors and are not allowed:</p>
 * <ul>
 *   <li>Adding the same column multiple times. This includes definitions like
 *       <tt>info:name</tt> that conflict with requests for the entire
 *       <tt>info:*</tt> family.</li>
 *   <li>Calling a property-setting method (<tt>withTimeRange()</tt>,
 *       <tt>withMaxVersions()</tt>, etc) more than once on a given
 *       <tt>KijiDataRequestBuilder</tt> or {@link KijiDataRequestBuilder.Column}.  These methods
 *       will throw IllegalStateException.</li>
 *   <li>Changing any properties after calling build() to construct the KijiDataRequest.</li>
 * </ul>
 *
 * <h2>Usage example</h2>
 *
 * <p>For the common case of reading <tt>info:foo</tt>, <tt>info:bar</tt>,
 * <tt>info:baz</tt>, and <tt>products:*</tt>:</p>
 * <pre>
 * KijiDataRequestBuilder builder = KijiDataRequest.builder().withTimeRange(t1, t2);
 * builder.column().withMaxVersions(42)
 *     .add("info", "foo")
 *     .add("info", "bar")
 *     .add("info", "baz")
 *     .add("products");
 * KijiDataRequest req = builder.build();
 * </pre>
 *
 * <p>To add <tt>fam1:col1</tt>, <tt>fam1:col2</tt>, and <tt>fam2:*</tt>, each with
 * different retrieval properties to the same request, do the following:</p>
 * <pre>
 * KijiDataRequestBuilder builder = KijiDataRequest.builder().withTimeRange(t1, t2);
 * builder.column().withMaxVersions(10).withPageSize(p).add("fam1", "col1");
 * builder.column().add("fam1", "col2");
 * builder.column().withMaxVersions(42).add("fam2");
 * KijiDataRequest req = builder.build();
 * </pre>
 */
@ApiAudience.Public
public final class KijiDataRequestBuilder {

  /** Column builders associated with this data request builder. */
  private Set<Column> mColBuilders;

  /** Set of fully-qualified column names attached to the data request builder. */
  private Set<KijiColumnName> mQualifiers;

  /** Set of map-type families explicitly attached to the data request builder. */
  private Set<KijiColumnName> mFamilies;

  /** Set of families represented by columns in mQualifiers. */
  private Set<KijiColumnName> mFamiliesFromQualifiers;

  /** Set of pre-built columns to include in the KDR. */
  private Set<KijiDataRequest.Column> mExistingColumns;

  /** The minimum timestamp of cells to be read (inclusive). */
  private long mMinTimestamp;

  /** The maximum timestamp of cells to be read (exclusive). */
  private long mMaxTimestamp;

  /** True if the user already set timestamp range. */
  private boolean mIsTimeRangeSet;

  /** True if we already built an object. */
  private boolean mIsBuilt;

  /**
   * Builds a request for Kiji table columns.
   *
   * <p>See {@link KijiDataRequestBuilder} for a larger specification of how
   * {@link KijiDataRequest} objects work.</p>
   *
   * <p>Use the {@link KijiDataRequestBuilder#column()} method to get an instance of
   * a column builder. This object has no "build()" method; use the {@link
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
  public final class Column {

    /** The maximum number of versions from the column to read (of the most recent). */
    private Integer mMaxVersions;

    /** A column filter (may be null). */
    private KijiColumnFilter mFilter;
    private boolean mFilterInitialized;

    /** The number of cells per page (zero means no paging). */
    private Integer mPageSize;

    /** Columns in the KijiDataRequest to create from this column builder. */
    private Set<KijiColumnName> mMyColumns;

    /**
     * Creates a new requested <code>Column</code> builder.
     */
    private Column() {
      mMyColumns = new HashSet<KijiColumnName>();
    }

    /** @return true if the user has already started assigning columns to this builder. */
    private boolean assignedColumns() {
      return mMyColumns.size() > 0;
    }

    /** If the user has assigned columns to this builder, throw IllegalStateException. */
    private void checkNoCols() {
      if (assignedColumns()) {
        throw new IllegalStateException("You have already assigned columns to this builder "
            + "with the add() method and cannot change properties now.");
      } else if (mIsBuilt) {
        throw new IllegalStateException("You have already built a KijiDataRequest and "
            + "cannot make further changes.");
      }
    }

    /**
     * Sets the maximum number of the most recent versions to return.
     *
     * @param num The maximum number of versions of the cell to read.
     * @return This column request builder instance.
     */
    public Column withMaxVersions(int num) {
      checkNoCols();
      if (num <= 0) {
        throw new IllegalArgumentException("Number of versions must be positive.");
      } else if (mMaxVersions != null) {
        throw new IllegalStateException("Cannot set max versions multiple times");
      }

      mMaxVersions = num;
      return this;
    }

    /**
     * Sets a filter to attach to each column specified by this column request builder.
     *
     * @param filter The column filter;
     * @return This column request builder instance.
     */
    public Column withFilter(KijiColumnFilter filter) {
      checkNoCols();
      if (mFilterInitialized) {
        throw new IllegalStateException("Cannot set filter multiple times");
      }

      mFilter = filter;
      mFilterInitialized = true;
      return this;
    }

    /**
     * Sets the number of cells per page (defaults to zero, which means paging is disabled).
     *
     * @param cellsPerPage The number of cells to return in each page of results. Use 0 to
     *     disable paging and return all results at once.
     * @return This column request instance.
     */
    public Column withPageSize(int cellsPerPage) {
      checkNoCols();
      if (cellsPerPage < 0) {
        throw new IllegalArgumentException("Page size must be 0 (disabled) or positive.");
      } else if (mPageSize != null) {
        throw new IllegalStateException("Cannot set page size multiple times");
      }

      mPageSize = cellsPerPage;
      return this;
    }

    /**
     * Adds a column to the data request, using the properties associated with this
     * KijiDataRequestBuilder.Column object. Once you call this method, you may not
     * call property-setting methods like withPageSize() on this same object.
     *
     * @param family the column family to retrieve as a map.
     * @return this column request builder instance.
     */
    public Column add(String family) {
      if (null == family) {
        throw new IllegalArgumentException("Cannot request a null family");
      } else if (family.contains(":")) {
        // This will be parsed by the KijiColumnName c'tor as family:qualifier.
        throw new IllegalArgumentException("Cannot request a family name with a ':' char.");
      }

      return add(new KijiColumnName(family));
    }

    /**
     * Adds a column to the data request, using the properties associated with this
     * KijiDataRequestBuilder.Column object. Once you call this method, you may not
     * call property-setting methods like withPageSize() on this same object.
     *
     * @param family the column family of the column to retrieve.
     * @param qualifier the qualifier of the column to retrieve.
     * @return this column request builder instance.
     */
    public Column add(String family, String qualifier) {
      return add(new KijiColumnName(family, qualifier));
    }

    /**
     * Adds a column to the data request, using the properties associated with this
     * KijiDataRequestBuilder.Column object. Once you call this method, you may not
     * call property-setting methods like withPageSize() on this same object.
     *
     * @param colName the column name to retrieve.
     * @return this column request builder instance.
     */
    public Column add(KijiColumnName colName) {
      if (mIsBuilt) {
        throw new IllegalStateException("You have already built a KijiDataRequest and "
            + "cannot make further changes.");
      }

      if (colName.isFullyQualified()) {
        // Handle the info:foo case.
        KijiColumnName associatedFamily = new KijiColumnName(colName.getFamily());
        if (mQualifiers.contains(colName)) {
          throw new IllegalArgumentException("You cannot specify the same column multiple "
              + "times in the same KijiDataRequest.");
        } else if (mFamilies.contains(associatedFamily)) {
          throw new IllegalArgumentException("You cannot specify a column individually "
              + "after requesting the entire column family in the same KijiDataRequest");
        }

        mQualifiers.add(colName);
        mFamiliesFromQualifiers.add(associatedFamily);
      } else {
        // Handle the info:* case.
        if (mFamiliesFromQualifiers.contains(colName)) {
          throw new IllegalArgumentException("You cannot specify a map-type column family "
              + "after requesting individual columns from that family in the same KijiDataRequest");
        } else if (mFamilies.contains(colName)) {
          throw new IllegalArgumentException("You cannot specify the same map-type column family "
              + "more than once in the same KijiDataRequest");
        }

        mFamilies.add(colName);
      }

      mMyColumns.add(colName);
      return this;
    }

    /**
     * Builds the columns associated with this column builder.
     *
     * @return a list of constructed KijiDataRequest.Column objects associated with the
     *     output KijiDataRequest instance.
     */
    private List<KijiDataRequest.Column> buildColumns() {

      // Values not previously initialized are now set to default values.
      // This builder is immutable after this method is called, so this is ok.
      if (mPageSize == null) {
        mPageSize = 0; // disable paging.
      }

      if (mMaxVersions == null) {
        mMaxVersions = 1;
      }

      List<KijiDataRequest.Column> cols = new ArrayList<KijiDataRequest.Column>();
      for (KijiColumnName name : mMyColumns) {
        cols.add(new KijiDataRequest.Column(name.getFamily(),
            name.getQualifier(), mMaxVersions, mFilter, mPageSize));
      }

      return cols;
    }
  }

  /**
   * Constructor. Package-private; use {@link KijiDataRequest#builder()} to get an
   * instance of this.
   */
  KijiDataRequestBuilder() {
    mColBuilders = new HashSet<Column>();

    mQualifiers = new HashSet<KijiColumnName>();
    mFamilies = new HashSet<KijiColumnName>();
    mFamiliesFromQualifiers = new HashSet<KijiColumnName>();

    mMinTimestamp = 0;
    mMaxTimestamp = Long.MAX_VALUE;
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
    if (minTimestamp < 0) {
      throw new IllegalArgumentException("minTimestamp may not be negative: " + minTimestamp);
    }
    if (maxTimestamp <= minTimestamp) {
      throw new IllegalArgumentException(
          "Invalid time range [" + minTimestamp + "," + maxTimestamp + ")");
    }
    if (mIsTimeRangeSet) {
      throw new IllegalStateException("Cannot set time range more than once.");
    }
    if (mIsBuilt) {
      throw new IllegalStateException(
          "Cannot modify this state after the data request is constructed.");
    }

    mIsTimeRangeSet = true;
    mMinTimestamp = minTimestamp;
    mMaxTimestamp = maxTimestamp;
    return this;
  }

  /**
   * Return a builder for columns.
   *
   * <p>Creates an object that allows you to specify a set of related columns attached
   * to the same KijiDataRequest that all share the same retrieval properties, like
   * the number of max versions.</p>
   *
   * @return a new KijiDataRequestBuilder.Column builder object associated with this
   *     data request builder.
   */
  public Column column() {
    if (mIsBuilt) {
      throw new IllegalStateException(
          "Cannot modify this state after the data request is constructed.");
    }

    Column c = new Column();
    mColBuilders.add(c);
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
   * <code>KijiDataRequestBuilder.Column.add(...)</code> are permitted.</p>
   *
   * @param existingColumn is a Column from an existing KijiDataRequest object that should
   *     be included in this new KijiDataRequest.
   * @return a new KijiDataRequestBuilder.Column builder object associated with this
   *     data request builder.
   */
  public Column column(KijiDataRequest.Column existingColumn) {
    return column()
        .withFilter(existingColumn.getFilter())
        .withPageSize(existingColumn.getPageSize())
        .withMaxVersions(existingColumn.getMaxVersions())
        .add(existingColumn.getFamily(), existingColumn.getQualifier());
  }

  /**
   * Construct a new KijiDataRequest based on the configuration specified in this builder
   * and its associated column builders.
   *
   * <p>After calling build(), you may not modify any aspects of the builder.</p>
   *
   * @return a new KijiDataRequest object containing the column requests associated
   *     with this KijiDataRequestBuilder.
   */
  public KijiDataRequest build() {
    mIsBuilt = true;
    List<KijiDataRequest.Column> outColumns = new ArrayList<KijiDataRequest.Column>();
    for (Column colBuilder : mColBuilders) {
      outColumns.addAll(colBuilder.buildColumns());
    }

    return new KijiDataRequest(outColumns, mMinTimestamp, mMaxTimestamp);
  }

}
