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
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.platform.SchemaPlatformBridge;

/** Debugging utilities. */
@ApiAudience.Private
public final class Debug {
  private static final Logger LOG = LoggerFactory.getLogger(Debug.class);

  /** Ruler to help formatting log statements. */
  private static final String LINE = Strings.repeat("-", 80);

  /**
   * Returns a string representation of the current stack trace.
   * @return a string representation of the current stack trace.
   */
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
          SchemaPlatformBridge.get().debugStringsForCompareFilter(ffilter));
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

  /**
   * Formats a collection of key-value pairs, for logging purposes.
   *
   * @param entries Key-value pairs to format.
   * @return the formatted list of key-value pairs.
   * @param <K> Type of the keys.
   * @param <V> Type of the values.
   */
  public static <K, V> String toLogString(Iterable<Map.Entry<K, V>> entries) {
    final Map<String, V> map = Maps.newTreeMap();
    for (Map.Entry<K, V> entry : entries) {
      map.put(entry.getKey().toString(), entry.getValue());
    }
    final StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, V> entry : map.entrySet()) {
      sb.append(String.format("%s: \"%s\"%n",
          entry.getKey(), StringEscapeUtils.escapeJava(entry.getValue().toString())));
    }
    return sb.toString();
  }

  /**
   * Logs the process configuration, for debugging purposes.
   *
   * Note: this is very verbose.
   *
   * @param conf Configuration to log.
   */
  public static void logConfiguration(final Configuration conf) {
    LOG.info(LINE);
    LOG.info("Using Job tracker: {}", conf.get("mapred.job.tracker"));
    LOG.info("Using default HDFS: {}", conf.get("fs.defaultFS"));
    LOG.info("Using HBase: quorum: {} - client port: {}",
        conf.get("hbase.zookeeper.quorum"), conf.get("hbase.zookeeper.property.clientPort"));

    LOG.info(LINE);
    LOG.info("Using Avro package: {}", Package.getPackage("org.apache.avro"));

    LOG.info(LINE);

    LOG.info("Environment variables:\n{}\n{}\n{}",
        LINE, toLogString(System.getenv().entrySet()), LINE);
    LOG.info("System properties:\n{}\n{}\n{}",
        LINE, toLogString(System.getProperties().entrySet()), LINE);
    LOG.info("Classpath:\n{}\n{}\n{}",
        LINE, Joiner.on("\n").join(System.getProperty("java.class.path").split(":")), LINE);
    LOG.info("Hadoop configuration:\n{}\n{}\n{}",
        LINE, toLogString(conf), LINE);
  }

  /** Utility class cannot be instantiated. */
  private Debug() {
  }
}
