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

package org.kiji.schema.filter;

import java.io.IOException;

import com.google.common.base.Objects;

import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiColumnName;

/**
 * A KijiColumnFilter that allows for pagination of results given an offset, limit, and
 * other filters that are and-ed together.
 */
@ApiAudience.Private
public class KijiPaginationFilter extends KijiColumnFilter {
  private static final long serialVersionUID = 1L;

  /** The max number of versions to return. */
  private final int mLimit;

  /** How many versions back in history to start looking. */
  private final int mOffset;

  /** Other filters to be checked before the pagination filter. */
  private final KijiColumnFilter mInputFilter;

  /**
   * Initialize pagination filter with limit, offset, and other filters to fold in.
   *
   * @param limit The max number of versions to return.
   * @param offset How many versions back in history to begin looking.
   * Write unit tests that verify these. HBase's docs are not specific
   */
  public KijiPaginationFilter(int limit, int offset) {
    mLimit = limit;
    mOffset = offset;
    mInputFilter = null;
  }

  /**
   * Initialize pagination filter with limit, offset, and other filters to fold in.
   *
   * @param limit The max number of versions to return.
   * @param offset How many versions back in history to begin looking.
   * @param filter Other filter that will precede
   */
  public KijiPaginationFilter(int limit, int offset, KijiColumnFilter filter) {
    mLimit = limit;
    mOffset = offset;
    mInputFilter = filter;
  }

  /** {@inheritDoc} */
  @Override
  public Filter toHBaseFilter(KijiColumnName kijiColumnName, Context context) throws IOException {
    FilterList requestFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    // Order that filters get added matters. Earlier in the list, the higher priority.
    if (mInputFilter != null) {
      requestFilter.addFilter(mInputFilter.toHBaseFilter(kijiColumnName, context));
    }
    Filter paginationFilter = new ColumnPaginationFilter(mLimit, mOffset);
    requestFilter.addFilter(paginationFilter);
    return requestFilter;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof KijiPaginationFilter)) {
      return false;
    } else {
      final KijiPaginationFilter otherFilter = (KijiPaginationFilter) other;
      return Objects.equal(otherFilter.mLimit, mLimit)
          && Objects.equal(otherFilter.mOffset, mOffset)
          && Objects.equal(otherFilter.mInputFilter, mInputFilter);
    }
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mLimit, mOffset, mInputFilter);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(KijiPaginationFilter.class)
        .add("limit", mLimit)
        .add("offset", mOffset)
        .add("filter", mInputFilter)
        .toString();
  }
}
