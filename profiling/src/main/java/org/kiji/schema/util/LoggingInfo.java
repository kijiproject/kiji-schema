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

package org.kiji.schema.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * The LoggingInfo class stores the information such as aggregate
 * time taken by all invocations of a function in nanoseconds and how
 * many times it was invoked.
 */
@ApiAudience.Framework
@ApiStability.Experimental
public final class LoggingInfo {
  private AtomicLong mAggregateTime;
  private AtomicInteger mNumInvoked;

  /**
   * Default constructor.
   */
  LoggingInfo() {
    mAggregateTime = new AtomicLong();
    mNumInvoked = new AtomicInteger();
  }

  /**
   * Constructor for LoggingInfo.
   *
   * @param aggregateTime nanoseconds spent so far in a function call.
   * @param timesInvoked number of times function was invoked so far.
   */
  public LoggingInfo(long aggregateTime, int timesInvoked) {
    mAggregateTime = new AtomicLong(aggregateTime);
    mNumInvoked = new AtomicInteger(timesInvoked);
  }

  /**
   * Decode a string into {@link LoggingInfo}.
   *
   * @param encoded the string to be decoded.
   */
  public LoggingInfo(String encoded) {
    String[] parts = encoded.split(", ");
    mAggregateTime = new AtomicLong(Long.decode(parts[0]));
    mNumInvoked = new AtomicInteger(Integer.decode(parts[1]));
  }

  /**
   * Encode the contents of this instance into a string.
   *
   * @return encoded string for the contents.
   */
  @Override
  public String toString() {
    return String.valueOf(mAggregateTime) + ", " + String.valueOf(mNumInvoked);
  }

  /**
   * Add to the aggregate time of a function call. By default this means
   * we are adding 1 to the number of method invocations.
   *
   * @param addToTime nanoseconds to add to the aggregate time spent in a function.
   * @return this LoggingInfo instance.
   */
  public LoggingInfo increment(long addToTime) {
    Preconditions.checkArgument(addToTime >= 0);
    mAggregateTime.addAndGet(addToTime);
    mNumInvoked.incrementAndGet();
    return this;
  }

  /**
   * Add to the aggregate time spent in a function and number of times it is invoked.
   *
   * @param addToTime nanoseconds to add to the total time spent in a function.
   * @param addToInvoked add to the number of times function was invoked.
   * @return this LoggingInfo instance.
   */
  public LoggingInfo increment(long addToTime, int addToInvoked) {
    Preconditions.checkArgument(addToTime >= 0 && addToInvoked >= 0);
    mAggregateTime.addAndGet(addToTime);
    mNumInvoked.addAndGet(addToInvoked);
    return this;
  }

  /**
   * Get time spent per function invocation.
   *
   * @return the average time taken per call in nanoseconds.
   */
  public Float perCallTime() {
    return mAggregateTime.longValue() / mNumInvoked.floatValue();
  }

  /**
   * Gets the aggregate time spent in this function.
   *
   * @return The aggregate time spent in the function as a long.
   */
  public long getAggregateTime() {
    return mAggregateTime.longValue();
  }

  /**
   * Gets the number of times this function was invoked.
   *
   * @return The number of times this function was invoked as an int.
   */
  public int getNumInvoked() {
    return mNumInvoked.intValue();
  }
}
