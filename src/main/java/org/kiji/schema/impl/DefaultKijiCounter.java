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

package org.kiji.schema.impl;

import org.kiji.schema.KijiCounter;

/**
 * The default implementation of a Kiji counter, which contains the data in a counter cell
 * of a column.
 */
public class DefaultKijiCounter implements KijiCounter {
  private final long mTimestamp;
  private final long mValue;

  /**
   * Creates a new <code>DefaultKijiCounter</code> instance.
   *
   * @param timestamp The timestamp at which the counter was incremented.
   * @param value The counter value.
   */
  public DefaultKijiCounter(long timestamp, long value) {
    mTimestamp = timestamp;
    mValue = value;
  }

  /** {@inheritDoc} */
  @Override
  public long getTimestamp() {
    return mTimestamp;
  }

  /** {@inheritDoc} */
  @Override
  public long getValue() {
    return mValue;
  }
}
