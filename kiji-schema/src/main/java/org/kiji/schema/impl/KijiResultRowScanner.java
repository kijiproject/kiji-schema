/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.schema.impl;

import java.io.IOException;
import java.util.Iterator;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Iterators;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiResultScanner;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * A {@code KijiRowScanner} implementation backed by a {@code KijiResultScanner}.
 */
@ApiAudience.Private
public class KijiResultRowScanner implements KijiRowScanner {
  private final Iterator<KijiRowData> mResultRowScanner;
  private final KijiResultScanner<Object> mResultScanner;
  private final KijiTableLayout mLayout;

  /**
   * Constructor for {@code KijiResultRowScanner}.
   *
   * @param layout The {@code KijiTableLayout} for the table.
   * @param resultScanner The {@code KijiResultScanner} backing this {@code KijiRowScanner}. This
   * {@code KijiResultScanner} is owned by the newly created {@code KijiResultRowScanner} and
   * calling close on this {@code KijiResultRowScanner} will close the underlying
   * {@code KijiResultScanner}.
   */
  public KijiResultRowScanner(
      final KijiTableLayout layout,
      final KijiResultScanner<Object> resultScanner) {
    mLayout = layout;
    mResultScanner = resultScanner;
    mResultRowScanner = Iterators.transform(
        resultScanner,
        new Function<KijiResult<Object>, KijiRowData>() {
          @Override
          public KijiRowData apply(final KijiResult<Object> result) {
            return new KijiResultRowData(mLayout, result);
          }
        });
  }

  /** {@inheritDoc} */
  @Override
  public Iterator<KijiRowData> iterator() {
    return mResultRowScanner;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    mResultScanner.close();
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mLayout, mResultRowScanner);
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
    final KijiResultRowScanner other = (KijiResultRowScanner) obj;
    return Objects.equal(this.mLayout, other.mLayout)
        && Objects.equal(this.mResultRowScanner, other.mResultRowScanner);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("mLayout", mLayout)
        .add("mResultRowScanner", mResultRowScanner)
        .toString();
  }
}
