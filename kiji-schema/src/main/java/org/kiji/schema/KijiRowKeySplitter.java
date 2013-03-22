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

package org.kiji.schema;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.util.ByteArrayFormatter;

/**
 * Utility class for splitting the Kiji row key space.
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class KijiRowKeySplitter {
  private static final Logger LOG = LoggerFactory.getLogger(KijiRowKeySplitter.class);

  /** No public constructor since this is a factory class. */
  private KijiRowKeySplitter() {}

  /**
   * Creates an instance of KijiRowKeySplitter for the default MD5 hash algorithm.
   *
   * @return a new splitter for Kiji row keys.
   */
  public static KijiRowKeySplitter get() {
    return new KijiRowKeySplitter();
  }

  /**
   * Resolution (in number of bytes) when generating evenly spaced HBase row keys.
   *
   * @return the number of bytes required by an md5 hash.
   */
  public int getRowKeyResolution() {
    return 16;
  }

  /**
   * Returns the split keys for the given number of regions.  This assumes that the keys are
   * byte strings of default size 16 for MD5.
   *
   * @param numRegions The number of desired regions.
   * @return The row keys that serve as the boundaries between the regions.
   */
  public byte[][] getSplitKeys(int numRegions) {
    if (numRegions < 2) {
      throw new IllegalArgumentException("numRegions must be at least 2, but was " + numRegions);
    }

    // Create a byte array of all zeros.
    byte[] startKey = new byte[getRowKeyResolution()];
    Arrays.fill(startKey, (byte) 0x00);

    // Create a byte array of all ones.
    byte[] limitKey = new byte[getRowKeyResolution()];
    Arrays.fill(limitKey, (byte) 0xFF);

    // This result includes numRegions + 1 keys in it (includes the startKey and limitKey).
    byte[][] ends = Bytes.split(startKey, limitKey, numRegions - 1);

    if (LOG.isDebugEnabled()) {
      for (int i = 0; i < ends.length; i++) {
        LOG.debug("Generated split key: " + ByteArrayFormatter.toHex(ends[i], ':'));
      }
    }

    // Remove the startKey from the beginning and the limitKey from the end.
    return Arrays.copyOfRange(ends, 1, ends.length - 1);
  }
}
