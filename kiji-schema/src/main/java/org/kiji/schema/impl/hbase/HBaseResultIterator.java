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
package org.kiji.schema.impl.hbase;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

/** Iterator over the KeyValues in a {@link org.apache.hadoop.hbase.client.Result}. */
public class HBaseResultIterator implements Iterator<KeyValue> {

  private final Result mResult;
  private final int mEndIndex;
  private int mCursor = 0;

  /**
   * Initialize a new HBaseResultIterator.
   *
   * @param result HBase Result to iterate across.
   */
  public HBaseResultIterator(
      final Result result
  ) {
    mResult = result;
    mCursor = 0;
    mEndIndex = result.size();
  }

  /**
   * Initialize a new HBaseResultIterator.
   *
   * @param result HBase Result to iterate across.
   * @param startIndex index in the Result at which to begin iteration. This index is inclusive.
   * @param endIndex index in the Result at which to end iteration. This index is exclusive.
   */
  public HBaseResultIterator(
      final Result result,
      final int startIndex,
      final int endIndex
  ) {
    Preconditions.checkArgument(startIndex >= 0 && startIndex <= result.size(),
        "start index must be greater than or equal to 0 and less than or equal to the size of the "
        + "result. Found start index: %s and result size: %s.", startIndex, result.size());
    Preconditions.checkArgument(endIndex >= 0 && endIndex <= result.size(),
        "end index  must be greater than or equal to 0 and less than or equal to the size of the "
        + "result. Found end index: %s and result size: %s.", endIndex, result.size());
    Preconditions.checkArgument(startIndex <= endIndex, "start index must be less than or equal to "
        + "end index. Found start index: %s and end index %s.", startIndex, endIndex);
    mResult = result;
    mCursor = startIndex;
    mEndIndex = endIndex;
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    return mEndIndex > mCursor;
  }

  /** {@inheritDoc} */
  @Override
  public KeyValue next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final KeyValue nextKV = mResult.raw()[mCursor];
    mCursor++;
    return nextKV;
  }

  /** {@inheritDoc} */
  @Override
  public void remove() {
    throw new UnsupportedOperationException("HBaseResultIterator does not support remove().");
  }
}
