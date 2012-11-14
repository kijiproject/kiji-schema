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

import java.util.Collections;
import java.util.Map;

import com.google.common.base.Function;

/**
 * A collection of useful functions.
 */
public final class Functions {
  /** Disable the constructor for this utility class. */
  private Functions() {}

  /**
   * A function that takes a map and returns an immutable version of it.
   *
   * @param <K> The map key type.
   * @param <V> The map value type.
   */
  private static class UnmodifiableMapFunction<K, V>
      implements Function<Map<? extends K, ? extends V>, Map<K, V>> {
    /** {@inheritDoc} */
    @Override
    public Map<K, V> apply(Map<? extends K, ? extends V> input) {
      if (null != input) {
        return Collections.unmodifiableMap(input);
      } else {
        return null;
      }
    }
  }

  /**
   * Returns a function that makes a map immutable.
   *
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A function returns an immutable version of a map.
   */
  public static <K, V> Function<Map<? extends K, ? extends V>, Map<K, V>> unmodifiableMap() {
    return new UnmodifiableMapFunction<K, V>();
  }
}
