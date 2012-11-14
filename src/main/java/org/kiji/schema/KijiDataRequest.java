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
import java.util.Collection;
import java.util.HashMap;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.schema.filter.KijiColumnFilter;

/**
 * <p>Describes a request for columns of data to read from a Kiji table.</p>
 *
 * <p>To request the 3 most recent versions of cell data from a column <code>bar</code> from
 * the family <code>foo</code> within the time range (123, 456]:
 *
 * <pre>
 * KijiDataRequest dataRequest = new KijiDataRequest()
 *     .withTimeRange(123L, 456L)
 *     .addColumn(new KijiDataRequest.Column("foo", "bar").withMaxVersions(3));
 * </pre>
 *
 * </p>
 */
public class KijiDataRequest implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * Map from full column name to Column describing the request. This is not a java.util.Map
   * because java.util.map isn't serializable.
   */
  private HashMap<String, Column> mColumns;

  /** The minimum timestamp of cells to be read (inclusive). */
  private long mMinTimestamp;

  /** The maximum timestamp of cells to be read (exclusive). */
  private long mMaxTimestamp;

  /**
   * Describes a request for a Kiji Table column.
   */
  public static class Column implements Serializable {
    private static final long serialVersionUID = 1L;

    /** The column family requested. */
    private String mFamily;
    /** The column qualifier requested (may be null, which means any qualifier). */
    private String mKey;
    /** The maximum number of versions from the column to read (of the most recent). */
    private int mMaxVersions;
    /** A column filter (may be null). */
    private KijiColumnFilter mFilter;
    /** The number of cells per page (zero means no paging). */
    private int mPageSize;

    /**
     * Creates a new requested <code>Column</code>.
     *
     * @param name The name of the column to request.
     */
    public Column(KijiColumnName name) {
      this(name.getFamily(), name.getQualifier());
    }

    /**
     * Creates a new request for the latest version of the cells in <code>family</code>.
     *
     * @param family The name of the column family to request.
     */
    public Column(String family) {
      this(family, (String) null);
      // This is an invalid use of this class.
      if (family.contains(":")) {
        KijiColumnName maybeColName = new KijiColumnName(family);

        throw new IllegalArgumentException("Cannot name column '" + family + "'. Did you mean "
            + "new KijiDataRequest.Column(\"" + maybeColName.getFamily() + "\", \""
            + maybeColName.getQualifier() + "\")?");
      }
    }

    /**
     * Creates a new request for the latest version of the cell in <code>family:key</code>.
     *
     * @param family The name of the column family to request.
     * @param key The name of the column qualifier to request.
     */
    public Column(String family, String key) {
      mFamily = family;
      mKey = key;
      mMaxVersions = 1;
    }

    /**
     * Creates a new request for the latest version of the cell in <code>family:key</code>.
     *
     * @param family The name of the column family to request.
     * @param key The name of the column qualifier to request.
     */
    public Column(String family, byte[] key) {
      this(family, Bytes.toString(key));
    }

    /**
     * Sets the maximum number of the most recent versions to return.
     *
     * @param num The maximum number of versions of the cell to read.
     * @return This column request instance.
     */
    public Column withMaxVersions(int num) {
      if (num <= 0) {
        throw new IllegalArgumentException("Number of versions must be positive.");
      }
      mMaxVersions = num;
      return this;
    }

    /**
     * Sets a filter to use on the column.
     *
     * @param filter The column filter;
     * @return This column request instance.
     */
    public Column withFilter(KijiColumnFilter filter) {
      mFilter = filter;
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
      mPageSize = cellsPerPage;
      return this;
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
    public String getKey() { return mKey; }

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
      if (mKey == null) {
        return mFamily;
      }
      return mFamily + ":" + mKey;
    }

    /**
     * Gets the column name.
     *
     * @return The column name.
     */
    public KijiColumnName getColumnName() {
      return new KijiColumnName(getFamily(), getKey());
    }

    /**
     * Gets the max number of most recent versions in this request.
     *
     * @return A number of versions.
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
      Column spec = (Column) other;
      return getName().equals(spec.getName())
          && mMaxVersions == spec.mMaxVersions
          && mPageSize == spec.mPageSize;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return toString().hashCode();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("name=").append(getName()).append("/")
          .append("maxVersions=").append(getMaxVersions()).append("/")
          .append("filter=").append(getFilter()).append("/")
          .append("pageSize=").append(getPageSize());
      return sb.toString();
    }
  }

  /** Constructor. */
  public KijiDataRequest() {
    mColumns = new HashMap<String, Column>();
    mMinTimestamp = 0;
    mMaxTimestamp = Long.MAX_VALUE;
  }

  /**
   * Creates a new KijiDataRequest from a copy.
   *
   * @param copy The KijiDataRequest to copy.
   */
  public KijiDataRequest(KijiDataRequest copy) {
    mColumns = new HashMap<String, Column>(copy.mColumns);
    mMinTimestamp = copy.mMinTimestamp;
    mMaxTimestamp = copy.mMaxTimestamp;
  }

  /**
   * Add a request for a column to this data request.  Only the most recent request for a
   * particular column is kept if multiple requests for the same column name are added.
   *
   * @param column The column request.
   * @return This KijiDataRequest instance.
   */
  public KijiDataRequest addColumn(Column column) {
    mColumns.put(column.getName(), column);
    return this;
  }

  /**
   * Sets the time range of cells to return: [<code>minTimestamp</code>,
   * <code>maxTimestamp</code>).
   *
   * @param minTimestamp Request cells with a timestamp at least minTimestamp.
   * @param maxTimestamp Request cells with a timestamp less than maxTimestamp.
   * @return This data request instance.
   */
  public KijiDataRequest withTimeRange(long minTimestamp, long maxTimestamp) {
    if (minTimestamp < 0) {
      throw new IllegalArgumentException("minTimestamp may not be negative: " + minTimestamp);
    }
    if (maxTimestamp <= minTimestamp) {
      throw new IllegalArgumentException(
          "Invalid time range [" + minTimestamp + "," + maxTimestamp + ")");
    }
    mMinTimestamp = minTimestamp;
    mMaxTimestamp = maxTimestamp;
    return this;
  }

  /**
   * Merges the requested columns in <code>other</code> into this data request.
   *
   * @param other Another data request to include as a part of this one.
   * @return This KijiDataRequest instance, mutated to include the other requested columns.
   */
  public KijiDataRequest merge(KijiDataRequest other) {
    for (Column otherColumn : other.getColumns()) {
      Column myColumn = getColumn(otherColumn.getFamily(), otherColumn.getKey());
      if (null == myColumn) {
        // We don't have a request for otherColumn, so add it.
        addColumn(otherColumn);
        continue;
      }

      // Increase the max versions if the other column request requires more.
      if (otherColumn.getMaxVersions() > myColumn.getMaxVersions()) {
        myColumn.withMaxVersions(otherColumn.getMaxVersions());
      }
    }

    // Expand the time range if the other data request requires more.
    long myMinTimestamp = getMinTimestamp();
    long myMaxTimestamp = getMaxTimestamp();
    if (other.getMinTimestamp() < myMinTimestamp) {
      myMinTimestamp = other.getMinTimestamp();
    }
    if (other.getMaxTimestamp() > myMaxTimestamp) {
      myMaxTimestamp = other.getMaxTimestamp();
    }
    withTimeRange(myMinTimestamp, myMaxTimestamp);
    return this;
  }

  /**
   * Gets a <code>Column</code> requested named "<code>family</code>:<code>key</code>", or
   * null if none exists.
   *
   * @param family The requested column family name.
   * @param key The requested column qualifier name.
   * @return The requested column.
   */
  public Column getColumn(String family, String key) {
    return mColumns.get(key != null ? family + ":" + key : family);
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
   * @return All the requested column specs.
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

    KijiDataRequest request = (KijiDataRequest) other;
    if (mColumns.size() != request.mColumns.size()) {
      return false;
    }
    for (String columnName : mColumns.keySet()) {
      if (!request.mColumns.containsKey(columnName)) {
        return false;
      }
      if (!mColumns.get(columnName).equals(request.mColumns.get(columnName))) {
        return false;
      }
    }

    return request.mMinTimestamp == mMinTimestamp
        && request.mMaxTimestamp == mMaxTimestamp;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (String columnName : mColumns.keySet()) {
      sb.append("Column:").append(mColumns.get(columnName).toString()).append("\n");
    }
    sb.append("timeRange=").append(getMinTimestamp()).append(",").append(getMaxTimestamp());
    return sb.toString();
  }
}
