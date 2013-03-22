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
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hadoop.hbase.HConstants;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.filter.KijiColumnFilter;

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

  /** Unmodifiable map from full column name to Column describing the request. */
  private final Map<String, Column> mColumns;

  /** The minimum timestamp of cells to be read (inclusive). */
  private final long mMinTimestamp;

  /** The maximum timestamp of cells to be read (exclusive). */
  private final long mMaxTimestamp;

  /**
   * Describes a request for a Kiji Table column.
   */
  @ApiAudience.Public
  public static final class Column implements Serializable {
    /** Serialization version. */
    public static final long serialVersionUID = 1L;

    /** The column family requested. */
    private final String mFamily;
    /** The column qualifier requested (may be null, which means any qualifier). */
    private final String mQualifier;
    /** The maximum number of versions from the column to read (of the most recent). */
    private final int mMaxVersions;
    /** A column filter (may be null). */
    private final KijiColumnFilter mFilter;
    /** The number of cells per page (zero means no paging). */
    private final int mPageSize;

    /**
     * Creates a new request for the latest version of the cell in <code>family:qualifier</code>.
     *
     * @param family The name of the column family to request.
     * @param qualifier The name of the column qualifier to request.
     * @param maxVersions the max versions of the column to request.
     * @param filter a column filter to attach to the results of this column request.
     * @param pageSize the default number of cells per page to retrieve at a time.
     */
    Column(String family, String qualifier, int maxVersions, KijiColumnFilter filter,
        int pageSize) {
      mFamily = family;
      mQualifier = qualifier;
      mMaxVersions = maxVersions;
      mFilter = filter;
      mPageSize = pageSize;
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
      return new KijiColumnName(getFamily(), getQualifier());
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
      return 0 != mPageSize;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Column)) {
        return false;
      }
      final Column otherCol = (Column) other;
      return new EqualsBuilder()
          .append(getName(), otherCol.getName())
          .append(mMaxVersions, otherCol.mMaxVersions)
          .append(mPageSize, otherCol.mPageSize)
          .isEquals();
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return toString().hashCode();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return Objects.toStringHelper(Column.class)
          .add("name", getName())
          .add("maxVersions", getMaxVersions())
          .add("filter", getFilter())
          .add("pageSize", getPageSize())
          .toString();
    }
  }

  /**
   * Constructor. Package-private; invoked by {@link KijiDataRequestBuilder#build()}
   * and create().
   *
   * @param columns the columns to include in this data request.
   * @param minTs the inclusive lower-bound on timestamps to request.
   * @param maxTs the exclusive upper-bound on timestamps to request.
   */
  KijiDataRequest(Collection<Column> columns, long minTs, long maxTs) {
    mMinTimestamp = minTs;
    mMaxTimestamp = maxTs;

    final ImmutableMap.Builder<String, Column> builder = ImmutableMap.builder();
    for (Column col : columns) {
      builder.put(col.getName(), col);
    }
    mColumns = builder.build();
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

    return new Column(family, qualifier, maxVersions, null, pageSize);
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
          continue;
        } else {
          outCols.add(mergeColumn(myCol.getFamily(), myCol.getQualifier(),
              myCol, otherCol));
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
   * Gets a <code>Column</code> requested named "<code>family</code>:<code>key</code>", or
   * null if none exists.
   *
   * @param family The requested column family name.
   * @param qualifier The requested column qualifier name.
   * @return The requested column.
   */
  public Column getColumn(String family, String qualifier) {
    return mColumns.get(qualifier != null ? family + ":" + qualifier : family);
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
  public boolean equals(Object other) {
    if (!(other instanceof KijiDataRequest)) {
      return false;
    }

    final KijiDataRequest otherReq = (KijiDataRequest) other;
    if (mColumns.size() != otherReq.mColumns.size()) {
      return false;
    }
    for (String columnName : mColumns.keySet()) {
      if (!otherReq.mColumns.containsKey(columnName)) {
        return false;
      }
      if (!mColumns.get(columnName).equals(otherReq.mColumns.get(columnName))) {
        return false;
      }
    }

    return otherReq.mMinTimestamp == mMinTimestamp
        && otherReq.mMaxTimestamp == mMaxTimestamp;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    final ToStringHelper helper = Objects.toStringHelper(KijiDataRequest.class);
    // TODO: For style points, sort the columns by name before we emit this list.
    for (Map.Entry<String, Column> entry : mColumns.entrySet()) {
      helper.add(String.format("column[%s]", entry.getKey()), entry.getValue());
    }
    return helper
        .add("timeRange", String.format("%s,%s", getMinTimestamp(), getMaxTimestamp()))
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
