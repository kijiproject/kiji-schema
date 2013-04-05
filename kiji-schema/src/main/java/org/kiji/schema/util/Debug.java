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

package org.kiji.schema.util;

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.util.StringUtils;

import org.kiji.annotations.ApiAudience;

/** Debugging utilities. */
@ApiAudience.Private
public final class Debug {
  /** @return a string representation of the current stack trace. */
  public static String getStackTrace() {
    try {
      throw new Exception();
    } catch (Exception exn) {
      return StringUtils.stringifyException(exn);
    }
  }

  /**
   * Pretty-prints an HBase filter, for debugging purposes.
   *
   * @param filter HBase filter to pretty-print.
   * @return a debugging representation of the HBase filter.
   */
  public static String toDebugString(Filter filter) {
    if (filter == null) { return "<no filter>"; }
    if (filter instanceof FilterList) {
      final FilterList flist = (FilterList) filter;
      String operator = null;
      switch (flist.getOperator()) {
      case MUST_PASS_ALL: operator = " and "; break;
      case MUST_PASS_ONE: operator = " or "; break;
      default: throw new RuntimeException();
      }
      final List<String> filters = Lists.newArrayList();
      for (Filter fchild : flist.getFilters()) {
        filters.add(toDebugString(fchild));
      }
      return String.format("(%s)", Joiner.on(operator).join(filters));
    } else if (filter instanceof FamilyFilter) {
      final FamilyFilter ffilter = (FamilyFilter) filter;
      return String.format("(HFamily %s %s)",
          ffilter.getOperator(), Bytes.toStringBinary(ffilter.getComparator().getValue()));
    } else if (filter instanceof ColumnPrefixFilter) {
      final ColumnPrefixFilter cpfilter = (ColumnPrefixFilter) filter;
      return String.format("(HColumn starts with %s)", Bytes.toStringBinary(cpfilter.getPrefix()));
    } else if (filter instanceof ColumnPaginationFilter) {
      final ColumnPaginationFilter cpfilter = (ColumnPaginationFilter) filter;
      return String.format("(%d cells from offset %d)", cpfilter.getLimit(), cpfilter.getOffset());
    } else {
      return filter.toString();
    }
  }

  /** Utility class cannot be instantiated. */
  private Debug() {
  }
}
