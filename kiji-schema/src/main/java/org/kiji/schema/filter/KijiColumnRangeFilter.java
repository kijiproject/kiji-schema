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

package org.kiji.schema.filter;

import java.io.IOException;

import com.google.common.base.Objects;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;

/** Column filter that bounds the range of returned qualifiers within a map-type family. */
@ApiAudience.Public
@ApiStability.Experimental
public class KijiColumnRangeFilter extends KijiColumnFilter {
  private static final long serialVersionUID = 1L;

  /** Qualifier lower bound, or null. */
  private final String mMinQualifier;

  /** Qualifier upper bound, or null. */
  private final String mMaxQualifier;

  /** Whether to include the lower bound qualifier. */
  private final boolean mIncludeMin;

  /** Whether to include the upper bound qualifier. */
  private final boolean mIncludeMax;

  /**
   * Initialize pagination filter with limit, offset, and other filters to fold in.
   *
   * @param min Qualifier lower bound. Null means none.
   * @param includeMin Whether to include the lower bound.
   * @param max Qualifier upper bound. Null means none.
   * @param includeMax Whether to include the upper bound.
   */
  public KijiColumnRangeFilter(String min, boolean includeMin, String max, boolean includeMax) {
    mMinQualifier = min;
    mIncludeMin = includeMin;
    mMaxQualifier = max;
    mIncludeMax = includeMax;
  }

  /**
   * Creates a column filter accepting qualifiers strictly greater than a given lower bound.
   *
   * @param qualifier Strict lower bound on accepted qualifiers.
   * @return a column filter accepting qualifiers strictly greater than a given lower bound.
   */
  public static KijiColumnRangeFilter greaterThan(String qualifier) {
    return new KijiColumnRangeFilter(qualifier, false, null, false);
  }

  /**
   * Creates a column filter accepting qualifiers greater than or equal to a given lower bound.
   *
   * @param qualifier Inclusive lower bound on accepted qualifiers.
   * @return a column filter accepting qualifiers greater than or equal to a given lower bound.
   */
  public static KijiColumnRangeFilter greaterThanOrEqualsTo(String qualifier) {
    return new KijiColumnRangeFilter(qualifier, true, null, false);
  }

  /**
   * Creates a column filter accepting qualifiers strictly smaller than a given upper bound.
   *
   * @param qualifier Strict upper bound on accepted qualifiers.
   * @return a column filter accepting qualifiers strictly smaller than a given upper bound.
   */
  public static KijiColumnRangeFilter lessThan(String qualifier) {
    return new KijiColumnRangeFilter(null, false, qualifier, false);
  }

  /**
   * Creates a column filter accepting qualifiers smaller than or equal to a given upper bound.
   *
   * @param qualifier Inclusive upper bound on accepted qualifiers.
   * @return a column filter accepting qualifiers smaller than or equal to a given upper bound.
   */
  public static KijiColumnRangeFilter lessThanOrEqualsTo(String qualifier) {
    return new KijiColumnRangeFilter(null, false, qualifier, true);
  }

  /**
   * Gets the UTF-8 encoded byte representation of a Kiji column qualifier, potentially null.
   *
   * @param context Column filter context.
   * @param family Kiji column family.
   * @param qualifier Kiji column qualifier, potentially null.
   * @return the HBase qualifier for the specified Kiji column, or null.
   * @throws NoSuchColumnException if there is no such column.
   */
  private static byte[] toHBaseQualifierBytesOrNull(
      Context context, String family, String qualifier) throws NoSuchColumnException {
    if (qualifier == null) {
      return null;
    }
    return context.getHBaseColumnName(KijiColumnName.create(family, qualifier)).getQualifier();
  }

  /** {@inheritDoc} */
  @Override
  public Filter toHBaseFilter(KijiColumnName kijiColumnName, Context context) throws IOException {
    final String family = kijiColumnName.getFamily();
    return new ColumnRangeFilter(
        toHBaseQualifierBytesOrNull(context, family, mMinQualifier), mIncludeMin,
        toHBaseQualifierBytesOrNull(context, family, mMaxQualifier), mIncludeMax);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object object) {
    if (!(object instanceof KijiColumnRangeFilter)) {
      return false;
    }
    final KijiColumnRangeFilter that = (KijiColumnRangeFilter) object;
    return Objects.equal(this.mMinQualifier, that.mMinQualifier)
        && Objects.equal(this.mMaxQualifier, that.mMaxQualifier)
        && Objects.equal(this.mIncludeMin, that.mIncludeMin)
        && Objects.equal(this.mIncludeMax, that.mIncludeMax);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mMinQualifier, mMaxQualifier, mIncludeMin, mIncludeMax);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(KijiColumnRangeFilter.class)
        .add("min", mMinQualifier)
        .add("include-min", mIncludeMin)
        .add("max", mMaxQualifier)
        .add("include-max", mIncludeMax)
        .toString();
  }

  /**
   * Get the minimum qualifier for this column range filter.
   *
   * @return The minimum qualifier for the filter.
   */
  public String getMinQualifier() {
    return mMinQualifier;
  }

  /**
   * Get the maximum qualifier for this column range filter.
   *
   * @return The maximum qualifier for the filter.
   */
  public String getMaxQualifier() {
    return mMaxQualifier;
  }

  /**
   * Get whether to include the minimum qualifier.
   *
   * @return whether to include the minimum qualifier.
   */
  public boolean isIncludeMin() {
    return mIncludeMin;
  }

  /**
   * Get whether to include the maximum qualifier.
   *
   * @return whether to include the maximum qualifier.
   */
  public boolean isIncludeMax() {
    return mIncludeMax;
  }
}
