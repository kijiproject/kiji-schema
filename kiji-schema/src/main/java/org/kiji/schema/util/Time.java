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

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.RuntimeInterruptedException;

/**
 * Handier time interface.
 */
@ApiAudience.Private
public final class Time {

  /** Utility class may not be instantiated. */
  private Time() {
  }

  /**
   * Reports the current time, in seconds since the Epoch (with ms precision).
   *
   * <p> Provides millisecond precision. </p>
   *
   * @return the current time, in seconds since the Epoch (with ms precision).
   */
  public static double now() {
    final long nowMS = System.currentTimeMillis();
    return ((double) nowMS) / 1000.0;
  }

  /**
   * Puts this thread to sleep for the specified amount of time.
   *
   * @param seconds Amount of time to sleep for, in seconds.
   * @throws RuntimeInterruptedException if this thread is interrupted.
   */
  public static void sleep(double seconds) {
    try {
      final double millis = seconds * 1000;
      final long imillis = (long) millis;
      final double nanos = (millis - imillis) * 1000000;
      Thread.sleep(imillis, (int) nanos);
    } catch (InterruptedException ie) {
      throw new RuntimeInterruptedException(ie);
    }
  }
}
