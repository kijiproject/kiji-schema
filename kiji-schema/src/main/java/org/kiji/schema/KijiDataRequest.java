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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hbase.HConstants;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.filter.KijiColumnFilter;
import org.kiji.schema.layout.ColumnReaderSpec;

/**
 * <p>Describes a request for columns of data to read from a Kiji table.</p>
 *
 * <p>KijiDataRequest objects are immutable. To create a KijiDataRequest, use
 * the {@link #builder()} method to get a new {@link KijiDataRequestBuilder} object.
 * Populate the object's fields, then call {@link KijiDataRequestBuilder#build()}.</p>
 *
 * <p>For example, to request the 3 most recent versions of cell data from a column
 * <code>bar</code> from
 * the family <code>foo</code> within the time range [123, 456):
 * <pre>
 * KijiDataRequestBuilder builder = KijiDataRequest.builder()
 *     .withTimeRange(123L, 456L);
 * builder.newColumnsDef().withMaxVersions(3).add("foo", "bar");
 * KijiDataRequest request = builder.build();
 * </pre>
 * Or:
 * <pre>
 * KijiDataRequest dataRequest = KijiDataRequest.builder()
 *     .withTimeRange(123L, 456L)
 *     .addColumns(KijiDataRequestBuilder.ColumnsDef.create().withMaxVersions(3).add("foo", "bar"))
 *     .build();
 * </pre>
 * </p>
 *
 * <p>For convenience, you can also build KijiDataRequests for a single cell
 * using the <code>KijiDataRequest.create()</code> method:
 *
 * <pre>
 * KijiDataRequest dataRequest = KijiDataRequest.create("info", "foo");
 * </pre>
 * </p>
 *
 * <p>You cannot set any properties of the requested column using this
 * syntax; for further customization, see {@link KijiDataRequestBuilder}.</p>
 */
@ApiAudience.Public
@ApiStability.Stable
public final class KijiDataRequest implements Serializable {
  /** Serialization version. */
  public static final long serialVersionUID = 1L;

  /** Page size used to indicate that pagination is not requested. */
  public static final int PAGING_DISABLED = 0;

  /** Empty data request which will return an empty KijiRowData when used with KijiTableReader. */
  private static final KijiDataRequest EMPTY_REQUEST = KijiDataRequest.builder().build();

  /** Unmodifiable map from full column name to Column describing the request. */
  private final Map<String, Column> mColumns;

  /** The minimum timestamp of cells to be read (inclusive). */
  private final long mMinTimestamp;

  /** The maximum timestamp of cells to be read (exclusive). */
  private final long mMaxTimestamp;

  // -----------------------------------------------------------------------------------------------

  /**
   * Describes a data request for a column in a Kiji table.
   */
  @ApiAudience.Public
  public static final class Column implements Serializable {
    /** Serialization version. */
    public static final long serialVersionUID = 1L;

    /** Column family requested. Never null. */
    private final String mFamily;

    /** Column qualifier requested. Null means all qualifiers in the family. */
    private final String mQualifier;

    /** Maximum number of the most recent versions to read from the column. */
    private final int mMaxVersions;

    /** Column filter. Null means none. */
    private final KijiColumnFilter mFilter;

    /** When using pagination, number of cells per page. Zero means no paging. */
    private final int mPageSize;

    /**
     * Read properties used to decode cells from this column.
     * Null means use the default properties from the table layout.
     */
    private final ColumnReaderSpec mReaderSpec;

    /**
     * Creates a new request for the latest version of the cell in <code>family:qualifier</code>.
     *
     * @param family The name of the column family to request.
     * @param qualifier The name of the column qualifier to request. Null means all qualifiers.
     * @param maxVersions the max versions of the column to request.
     * @param filter a column filter to attach to the results of this column request.
     * @param pageSize the default number of cells per page to retrieve at a time.
     * @param columnReaderSpec read properties used when decoding cells from this column.
     *     Null means use the default properties from the table layout.
     */
    Column(
        final String family,
        final String qualifier,
        final int maxVersions,
        final KijiColumnFilter filter,
        final int pageSize,
        final ColumnReaderSpec columnReaderSpec
    ) {
      mFamily = family;
      mQualifier = qualifier;
      mMaxVersions = maxVersions;
      mFilter = filter;
      mPageSize = pageSize;
      mReaderSpec = columnReaderSpec;
    }

    /**
     * Gets the name of the requested column family.
     *
     * @return A column family name.
     */
    public String getFamily() { return mFamily; }

    /**
     * Gets the name of the requested column qualifier, which may be null if any qualifier
     * should be included.
     *
     * @return A column qualifier name (may be null or empty).
     */
    public String getQualifier() { return mQualifier; }

    /**
     * <p>Gets the full name of the requested column.</p>
     *
     * <p>Note: A request for column "foo" means that all columns within the family "foo"
     * should be requested, whereas a request for column "foo:" means that the single
     * column with qualifier "" (empty) should be returned from the family "foo".
     *
     * @return A column name.
     */
    public String getName() {
      if (mQualifier == null) {
        return mFamily;
      }
      return mFamily + ":" + mQualifier;
    }

    /**
     * Gets the column name.
     *
     * @return The column name.
     */
    public KijiColumnName getColumnName() {
      return KijiColumnName.create(getFamily(), getQualifier());
    }

    /**
     * Gets the max number of most recent versions in this column.
     *
     * @return The maximum number of most recent versions in this column.
     */
    public int getMaxVersions() { return mMaxVersions; }

    /**
     * Gets the column filter, or null if no filter was specified.
     *
     * @return The column filter, or null.
     */
    public KijiColumnFilter getFilter() {
      return mFilter;
    }

    /**
     * Gets the number of cells to return per page of results.
     *
     * @return The page size (or 0 if paging is disabled).
     */
    public int getPageSize() {
      return mPageSize;
    }

    /**
     * Determines whether paging is enabled for this column.
     *
     * @return Whether paging is enabled.
     */
    public boolean isPagingEnabled() {
      return mPageSize != PAGING_DISABLED;
    }

    /**
     * Returns the read properties requested when decode this column.
     * @return the read properties requested when decode this column.
     *     Null means use the default read properties from the table layout.
     */
    public ColumnReaderSpec getReaderSpec() {
      return mReaderSpec;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hashCode(mFamily, mQualifier, mMaxVersions, mFilter, mPageSize, mReaderSpec);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final Column other = (Column) obj;
      return Objects.equal(this.mFamily, other.mFamily)
          && Objects.equal(this.mQualifier, other.mQualifier)
          && Objects.equal(this.mMaxVersions, other.mMaxVersions)
          && Objects.equal(this.mFilter, other.mFilter)
          && Objects.equal(this.mPageSize, other.mPageSize)
          && Objects.equal(this.mReaderSpec, other.mReaderSpec);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      final ToStringHelper helper = Objects.toStringHelper(this).add("name", getColumnName());
      if (mMaxVersions != 0) {
        helper.add("max_versions", mMaxVersions);
      }
      if (mFilter != null) {
        helper.add("filter", mFilter);
      }
      if (mPageSize != PAGING_DISABLED) {
        helper.add("page_size", mPageSize);
      }
      if (mReaderSpec != null) {
        helper.add("reader_spec", mReaderSpec);
      }
      return helper.toString();
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Constructor. Package-private; invoked by {@link KijiDataRequestBuilder#build()}
   * and create().
   *
   * @param columnRequests the columns to include in this data request.
   * @param minTs the inclusive lower-bound on timestamps to request.
   * @param maxTs the exclusive upper-bound on timestamps to request.
   */
  KijiDataRequest(Collection<Column> columnRequests, long minTs, long maxTs) {
    mMinTimestamp = minTs;
    mMaxTimestamp = maxTs;

    final ImmutableMap.Builder<String, Column> builder = ImmutableMap.builder();
    for (Column col : columnRequests) {
      builder.put(col.getName(), col);
    }
    mColumns = builder.build();
  }

  /**
   * Empty data request which will return an empty row data from a {@link KijiTableReader} without a
   * trip to the underlying storage.
   *
   * @return an empty data request.
   */
  public static KijiDataRequest empty() {
    return EMPTY_REQUEST;
  }

  /**
   * Factory method for a simple KijiDataRequest for the most recent version
   * of each qualifier in one column family.
   *
   * <p>This method does not facilitate additional customization of the
   * data request, such as requesting multiple column families, or setting
   * the timestamp range. For that, get a {@link KijiDataRequestBuilder} by
   * calling {@link #builder()}.</p>
   *
   * @param family the column family to request
   * @return a new KijiDataRequest that retrieves the selected column family.
   */
  public static KijiDataRequest create(String family) {
    KijiDataRequestBuilder builder = builder();
    builder.newColumnsDef().addFamily(family);
    return builder.build();
  }

  /**
   * Factory method for a simple KijiDataRequest for the most recent
   * version of a specific family:qualifier.
   *
   * <p>This method does not facilitate additional customization of the
   * data request, such as requesting multiple columns, or setting
   * the timestamp range. For that, get a {@link KijiDataRequestBuilder} by
   * calling {@link #builder()}.</p>
   *
   * @param family the column family to request
   * @param qualifier the column qualifier to request
   * @return a new KijiDataRequest that retrieves the selected column.
   */
  public static KijiDataRequest create(String family, String qualifier) {
    KijiDataRequestBuilder builder = builder();
    builder.newColumnsDef().add(family, qualifier);
    return builder.build();
  }

  /**
   * Merges the properties of the two column requests and returns a new column request
   * with the specified family and qualifier, and the merged properties.
   *
   * <p>This merges the specified columns according to the logic of
   * {@link #merge(KijiDataRequest)}. The output family and qualifier are specified
   * explicitly.</p>
   *
   * @param family the output column request's column family.
   * @param qualifier the output column request's qualifier. May be null.
   * @param col1 one of two column definitions to merge properties from.
   * @param col2 the other column definition to use as input.
   * @return a new Column request for family:qualifier, with properties merged from
   *     col1 and col2.
   * @throws IllegalStateException if the column requests cannot be merged.
   */
  private static Column mergeColumn(String family, String qualifier, Column col1, Column col2) {
    assert null != col1;
    assert null != col2;

    int pageSize = Math.min(col1.getPageSize(), col2.getPageSize());
    if (0 == pageSize && Math.max(col1.getPageSize(), col2.getPageSize()) > 0) {
      // One column had a page size of zero (i.e., infinity / paging disabled)
      // and one had a non-zero page size. Go with that one.
      pageSize = Math.max(col1.getPageSize(), col2.getPageSize());
    }

    int maxVersions = Math.max(col1.getMaxVersions(), col2.getMaxVersions());

    // Merge column read properties. We don't actually merge but enforce consistency:
    final ColumnReaderSpec readerSpec;
    if (col1.getReaderSpec() == null) {
      readerSpec = col2.getReaderSpec();
    } else if (col2.getReaderSpec() == null) {
      readerSpec = col1.getReaderSpec();
    } else if (Objects.equal(col1.getReaderSpec(), col2.getReaderSpec())) {
      readerSpec = col1.getReaderSpec();
    } else {
      throw new IllegalStateException(String.format(
          "Cannot merge reader specifications %s with %s for column '%s:%s'",
          col1.getReaderSpec(), col2.getReaderSpec(), family, qualifier));
    }

    return new Column(family, qualifier, maxVersions, null, pageSize, readerSpec);
  }

  /**
   * Creates a new data request representing the union of this data request and the
   * data request specified as an argument.
   *
   * <p>This method merges data requests using the widest-possible treatment of
   * parameters. This may result in cells being included in the result set that
   * were not specified by either data set. For example, if request A includes <tt>info:foo</tt>
   * from time range [400, 700), and request B includes <tt>info:bar</tt> from time range
   * [500, 900), then A.merge(B) will yield a data request for both columns, with time
   * range [400, 900).</p>
   *
   * <p>More precisely, merges are handled in the following way:</p>
   * <ul>
   *   <li>The output time interval encompasses both data requests' time intervals.</li>
   *   <li>All columns associated with both data requests will be included.</li>
   *   <li>When maxVersions differs for the same column in both requests, the greater
   *       value is chosen.</li>
   *   <li>When pageSize differs for the same column in both requests, the lesser value
   *       is chosen.</li>
   *   <li>If either request contains KijiColumnFilter definitions attached to a column,
   *      this is considered an error, and a RuntimeException is thrown. Data requests with
   *      filters cannot be merged.</li>
   *   <li>If one data request includes an entire column family (<tt>foo:*</tt>) and
   *       the other data request includes a column within that family (<tt>foo:bar</tt>),
   *       the entire family will be requested, and properties such as max versions, etc.
   *       for the family-wide request will be merged with the column to ensure the request
   *       is as wide as possible.</li>
   * </ul>
   *
   * @param other another data request to include as a part of this one.
   * @return A new KijiDataRequest instance, including the union of this data request
   *     and the argument request.
   */
  public KijiDataRequest merge(KijiDataRequest other) {
    if (null == other) {
      throw new IllegalArgumentException("Input data request cannot be null.");
    }

    List<Column> outCols = new ArrayList<Column>();
    Set<String> families = new HashSet<String>(); // map-type families requested.

    // First, include any requests for column families.
    for (Column otherCol : other.getColumns()) {
      if (otherCol.getFilter() != null) {
        // And while we're at it, check for filters. We don't know how to merge these.
        throw new IllegalStateException("Invalid merge request: "
            + otherCol.getName() + " has a filter.");
      }

      if (otherCol.getQualifier() == null) {
        Column outFamily = otherCol;
        // Loop through any requests on our end that have the same family.
        for (Column myCol : getColumns()) {
          if (myCol.getFamily().equals(otherCol.getFamily())) {
            outFamily = mergeColumn(myCol.getFamily(), null, myCol, outFamily);
          }
        }

        outCols.add(outFamily);
        families.add(outFamily.getFamily());
      }
    }

    // Include requests for column families on our side that aren't present on their's.
    for (Column myCol : getColumns()) {
      if (myCol.getFilter() != null) {
        // And while we're at it, check for filters. We don't know how to merge these.
        throw new IllegalStateException("Invalid merge request: "
            + myCol.getName() + " has a filter.");
      }

      if (myCol.getQualifier() == null && !families.contains(myCol.getFamily())) {
        Column outFamily = myCol;
        // Loop through requests on their end that have the same family.
        for (Column otherCol : other.getColumns()) {
          if (otherCol.getFamily().equals(myCol.getFamily())) {
            outFamily = mergeColumn(myCol.getFamily(), null, outFamily, otherCol);
          }
        }

        outCols.add(outFamily);
        families.add(outFamily.getFamily());
      }
    }


    // Now include individual columns from their side. If we have corresponding definitions
    // for the same columns, merge them. If the column is already covered by a request
    // for a family, ignore the individual column (it's already been merged).
    for (Column otherCol : other.getColumns()) {
      if (otherCol.getQualifier() != null && !families.contains(otherCol.getFamily())) {
        Column myCol = getColumn(otherCol.getFamily(), otherCol.getQualifier());
        if (null == myCol) {
          // We don't have a request for otherColumn, so add it.
          outCols.add(otherCol);
        } else {
          outCols.add(mergeColumn(myCol.getFamily(), myCol.getQualifier(), myCol, otherCol));
        }
      }
    }

    // Now grab any columns that were present in our data request, but missed entirely
    // in the other side's data request.
    for (Column myCol : getColumns()) {
      if (myCol.getQualifier() != null && !families.contains(myCol.getFamily())) {
        Column otherCol = other.getColumn(myCol.getFamily(), myCol.getQualifier());
        if (null == otherCol) {
          // Column in our list but not the other side's list.
          outCols.add(myCol);
        }
      }
    }

    long outMinTs = Math.min(getMinTimestamp(), other.getMinTimestamp());
    long outMaxTs = Math.max(getMaxTimestamp(), other.getMaxTimestamp());

    return new KijiDataRequest(outCols, outMinTs, outMaxTs);
  }

  /**
   * Reports the request for the specified column or family, or null if no such request exists.
   *
   * @param family The requested column family name.
   * @param qualifier The requested column qualifier name.
   *     Null means no qualifier, ie. reports the request for the entire family.
   * @return the request for the specified column or family, or null if no such request exists.
   */
  public Column getColumn(String family, String qualifier) {
    return mColumns.get(qualifier != null ? family + ":" + qualifier : family);
  }

  /**
   * Reports the request for the specified column or family, or null if no such request exists.
   *
   * @param column Requested column or family.
   * @return the request for the specified column or family, or null if no such request exists.
   */
  public Column getColumn(final KijiColumnName column) {
    return mColumns.get(column.toString());
  }

  /**
   * Reports the request that applies for the specified column, or null if no such request exists.
   *
   * <p>
   *   If the column belongs to a map-type family and there is a request for the entire family,
   *   the request for the entire family is reported.
   * </p>
   *
   * @param family The requested column family name.
   * @param qualifier The requested column qualifier name.
   *     Null means no qualifier, ie. reports the request for the entire family.
   * @return the request that applies for the specified column or family,
   *     or null if no such request exists.
   */
  public Column getRequestForColumn(String family, String qualifier) {
    final Column column = getColumn(family, qualifier);
    if (column != null) {
      return column;
    }
    if (qualifier != null) {
      return getColumn(family, null);
    }
    return null;
  }

  /**
   * Reports the request the applies for the specified column, or null if no such request exists.
   *
   * <p>
   *   If the column belongs to a map-type family and there is a request for the entire family,
   *   the request for the entire family is reported.
   * </p>
   *
   * @param columnName Requested column or family.
   * @return the request that applies for the specified column or family,
   *     or null if no such request exists.
   */
  public Column getRequestForColumn(final KijiColumnName columnName) {
    final Column column = getColumn(columnName);
    if (column != null) {
      return column;
    }
    if (columnName.isFullyQualified()) {
      return getColumn(columnName.getFamily(), null);
    }
    return null;
  }

  /**
   * Determines whether this data request has any columns.
   *
   * @return Whether any data has been requested.
   */
  public boolean isEmpty() {
    return mColumns.isEmpty();
  }

  /**
   * Gets the collection of requested columns.
   *
   * @return All the requested column specs as an immutable collection.
   */
  public Collection<Column> getColumns() {
    return mColumns.values();
  }

  /**
   * Gets the minimum timestamp for versions in this request (inclusive).
   *
   * @return A minimum timestamp (milliseconds since the epoch).
   */
  public long getMinTimestamp() { return mMinTimestamp; }

  /**
   * Gets the maximum timestamp for versions in this request (exclusive).
   *
   * @return A maximum timestamp (milliseconds since the epoch).
   */
  public long getMaxTimestamp() { return mMaxTimestamp; }

  /**
   * Determines whether a timestamp <code>ts</code> is within the time range for this
   * request.
   *
   * @param ts The timestamp to check.
   * @return Whether the timestamp is within the range of this request.
   */
  public boolean isTimestampInRange(long ts) {
    if (HConstants.LATEST_TIMESTAMP == ts && HConstants.LATEST_TIMESTAMP == getMaxTimestamp()) {
      // Special case for "most recent timestamp."
      return true;
    }
    return ts >= getMinTimestamp() && ts < getMaxTimestamp();
  }

  /**
   * Determines whether paging is enabled on any of the columns in this request.
   *
   * @return Whether paging is enabled.
   */
  public boolean isPagingEnabled() {
    for (Column column : getColumns()) {
      if (column.isPagingEnabled()) {
        return true;
      }
    }
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mColumns, mMinTimestamp, mMaxTimestamp);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final KijiDataRequest other = (KijiDataRequest) obj;
    return Objects.equal(this.mColumns, other.mColumns)
        && Objects.equal(this.mMinTimestamp, other.mMinTimestamp)
        && Objects.equal(this.mMaxTimestamp, other.mMaxTimestamp);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("columns", mColumns)
        .add("min_timestamp", mMinTimestamp)
        .add("max_timestamp", mMaxTimestamp)
        .toString();
  }

  /**
   * Creates a new {@link KijiDataRequestBuilder}. Use this to configure a KijiDataRequest,
   * then create one with the {@link KijiDataRequestBuilder#build()} method.
   *
   * @return a new KijiDataRequestBuilder.
   */
  public static KijiDataRequestBuilder builder() {
    return new KijiDataRequestBuilder();
  }
}
