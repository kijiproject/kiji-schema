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

package org.kiji.schema;

import java.util.Collection;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * A KijiRegion specifies a logical region in a Kiji Table, bounded by
 * a start key (inclusive) and an end key (exclusive).
 */
@ApiAudience.Public
@ApiStability.Evolving
public interface KijiRegion {
  /**
   * Gets the start key (inclusive) of this region.
   *
   * @return The start key (inclusive) of this region.
   */
  byte[] getStartKey();

  /**
   * Gets the end key (exclusive) of this region.
   *
   * @return The end key (exclusive) of this region.
   */
  byte[] getEndKey();

  /**
   * Gets the locations of this region in hostname:port form, if available.
   * The collection is empty if no location information is available.
   *
   * @return The locations of this region; empty if no location information is available.
   */
  Collection<String> getLocations();
}
