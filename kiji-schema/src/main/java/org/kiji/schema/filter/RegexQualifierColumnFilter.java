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
import java.util.regex.Pattern;

import com.google.common.base.Objects;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.platform.SchemaPlatformBridge;

/**
 * A KijiColumnFilter that only allows qualifiers that match a given regular expression.
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class RegexQualifierColumnFilter extends KijiColumnFilter {
  private static final long serialVersionUID = 1L;

  private final String mRegularExpression;

  /**
   * Constructor.
   *
   * @param regularExpression The regular expression for qualifiers that should be
   * accepted by this filter. The expression is matched against the full qualifier (as
   * if it implicitly starts with '^' and ends with '$'.
   */
  public RegexQualifierColumnFilter(String regularExpression) {
    // Try to compile the regular expression, so clients get an early error message.
    Pattern.compile(regularExpression);  // <-- throws a PatternSyntaxException if invalid.

    mRegularExpression = regularExpression;
  }

  /** {@inheritDoc} */
  @Override
  public Filter toHBaseFilter(KijiColumnName kijiColumnName, Context context) throws IOException {
    HBaseColumnName columnName = context.getHBaseColumnName(kijiColumnName);
    return SchemaPlatformBridge.get().createQualifierFilterFromRegex(CompareFilter.CompareOp.EQUAL,
        columnName.getQualifierAsString() + mRegularExpression);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof RegexQualifierColumnFilter)) {
      return false;
    } else {
      final RegexQualifierColumnFilter otherFilter = (RegexQualifierColumnFilter) other;
      return Objects.equal(otherFilter.mRegularExpression, mRegularExpression);
    }
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return mRegularExpression.hashCode();
  }
}
