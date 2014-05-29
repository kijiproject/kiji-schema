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

/**
 * A clock implementation that increments the time each time you query for it.
 */
public class IncrementingClock extends Clock {
  /** The current clock time. */
  private long mCurrentTime;

  /**
   * Creates a new <code>IncrementingClock</code> instance.
   *
   * @param initialTime The initial time for the clock.
   */
  public IncrementingClock(long initialTime) {
    mCurrentTime = initialTime;
  }

  /** {@inheritDoc} */
  @Override
  public long getTime() {
    return mCurrentTime++;
  }
}
