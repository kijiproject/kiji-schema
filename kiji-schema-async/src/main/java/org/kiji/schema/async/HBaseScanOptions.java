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

package org.kiji.schema.hbase;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Customizable parameters for KijiRowScanners backed by an HBase scan.
 * Any parameters that are not set will default to HBase values.
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class HBaseScanOptions {
  /** The number of rows to buffer on the client. */
  private Integer mClientBufferSize = null;
  /** The number of rows for each server to prefetch. */
  private Integer mServerPrefetchSize = null;
  /** Whether servers should cache rows as they're retrieved. */
  private Boolean mCacheBlocks = null;

  /**
   * Creates a new HBaseScanOptions where all parameters will default to HBase values.
   */
  public HBaseScanOptions() {}

  /**
   * Sets the number of rows that will be returned with each RPC when iterating
   * through a KijiRowScanner.  If null, uses HBase default.
   *
   * @param bufferSize The number of rows to request with each RPC.
   */
  public void setClientBufferSize(Integer bufferSize) {
    mClientBufferSize = bufferSize;
  }

  /**
   * Returns the number of rows to request with each RPC,
   * or null if using the HBase default.
   *
   * @return The number of rows to request with each RPC.
   */
  public Integer getClientBufferSize() {
    return mClientBufferSize;
  }

  /**
   * Sets the number of rows for servers to prefetch.  If null, use HBase default.
   * Increasing this number should improve amortized scan time, although individual requests
   * may take longer to complete while the server is prefetching the following rows.
   * <i>It is important that the scanner timeout be long enough to allow these longer fetches.</i>
   *
   * @param prefetchSize The number of rows to read into memory at a time.
   */
  public void setServerPrefetchSize(Integer prefetchSize) {
    mServerPrefetchSize = prefetchSize;
  }

  /**
   * Returns the number of rows for servers to prefetch,
   * or null if using the HBase default.
   *
   * @return The number of rows for servers to prefetch.
   */
  public Integer getServerPrefetchSize() {
    return mServerPrefetchSize;
  }

  /**
   * Sets whether rows requested are cached server-side for future use.  If null, use HBase default.
   *
   * @param cacheBlocks Whether rows are cached server-side.
   */
  public void setCacheBlocks(Boolean cacheBlocks) {
    mCacheBlocks = cacheBlocks;
  }

  /**
   * Returns whether requested rows are cached server-side,
   * or null if using the HBase default.
   *
   * @return Whether requested rows are cached server-side.
   */
  public Boolean getCacheBlocks() {
    return mCacheBlocks;
  }

  // TODO: Surface scanner timeout.
}
