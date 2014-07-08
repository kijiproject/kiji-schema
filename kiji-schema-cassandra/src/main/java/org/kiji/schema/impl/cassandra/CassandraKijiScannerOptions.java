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

package org.kiji.schema.impl.cassandra;

import com.google.common.base.Preconditions;

/**
 * Cassandra-specific options for row scanners.
 */
public final class CassandraKijiScannerOptions {
  /** Token value, inclusive, for partition key at start of scan (null if no start token). */
  private final Long mStartToken;

  /** Token value, inclusive, for partition key at end of scan (null if no stop token). */
  private final Long mStopToken;

  /**
   * @return the starting token (inclusive) for this scan.
   */
  public long getStartToken() {
    Preconditions.checkArgument(hasStartToken());
    return mStartToken;
  }

  /**
   * @return the ending token (inclusive) for this scan.
   */
  public long getStopToken() {
    Preconditions.checkArgument(hasStopToken());
    return mStopToken;
  }

  /**
   * @return whether this scan has a starting token specified.
   */
  public boolean hasStartToken() {
    return mStartToken != null;
  }

  /**
   * @return whether this scan has a stopping token specified.
   */
  public boolean hasStopToken() {
    return mStopToken != null;
  }

  /**
   * Creates a new CassandraKijiScannerOptions object with a start and stop token.
   *
   * @param startToken The starting token (inclusive) for the partition key.
   * @param stopToken The stopping token (inclusive) for the partition key.
   * @return A new CassandraKijiScannerOptions object.
   */
  public static CassandraKijiScannerOptions withTokens(long startToken, long stopToken) {
    return new CassandraKijiScannerOptions(startToken, stopToken);
  }

  /**
   * Creates a new CassandraKijiScannerOptions object with a start token.
   *
   * @param startToken The starting token (inclusive) for the partition key.
   * @return A new CassandraKijiScannerOptions object.
   */
  public static CassandraKijiScannerOptions withStartToken(long startToken) {
    return new CassandraKijiScannerOptions(startToken, null);
  }

  /**
   * Creates a new CassandraKijiScannerOptions object with a stop token.
   *
   * @param stopToken The stopping token (inclusive) for the partition key.
   * @return A new CassandraKijiScannerOptions object.
   */
  public static CassandraKijiScannerOptions withStopToken(long stopToken) {
    return new CassandraKijiScannerOptions(null, stopToken);
  }

  /**
   * Creates a new CassandraKijiScannerOptions object for a scan over all partition key tokens.
   *
   * @return A new CassandraKijiScannerOptions object.
   */
  public static CassandraKijiScannerOptions withoutBounds() {
    return new CassandraKijiScannerOptions(null, null);
  }

  /**
   * Private constructor for the scanner options.
   *
   * @param startToken Start token (inclusive) or null.
   * @param stopToken Stop token (inclusive) or null.
   */
  private CassandraKijiScannerOptions(Long startToken, Long stopToken) {
    this.mStartToken = startToken;
    this.mStopToken = stopToken;
  }
}
