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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.avro.TableLayoutDesc;
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
  @Deprecated
  public int getRowKeyResolution() {
    return 16;
  }

  /**
   * Gets a table's row-key hash resolution (in number of bytes)
   * for use in evenly spacing HBase row keys.
   *
   * @param tableLayout the layout of the table
   * @return the table's hash resolution.
   */
  public static int getRowKeyResolution(TableLayoutDesc tableLayout) {
    // Get hashSize from layout.
    int hashSize = 16;
    // No assumptions make about the RKF.
    if (RowKeyFormat.class.equals(tableLayout.getKeysFormat().getClass())) {
      hashSize = ((RowKeyFormat) tableLayout.getKeysFormat()).getHashSize();
    } else if (RowKeyFormat2.class.equals(tableLayout.getKeysFormat().getClass())) {
      RowKeyFormat2 format = (RowKeyFormat2) tableLayout.getKeysFormat();
      if (null == format.getSalt()) {
        throw new IllegalArgumentException(
            "This table layout defines an entityId format without hashing enabled.");
      }
      hashSize = ((RowKeyFormat2) tableLayout.getKeysFormat()).getSalt().getHashSize();
    }
    return hashSize;
  }

  /**
   * Returns the split keys for the given number of regions.  This assumes that the keys are
   * byte strings of default size 16 for MD5.
   *
   * @param numRegions The number of desired regions.
   * @return The row keys that serve as the boundaries between the regions.
   */
  @Deprecated
  public byte[][] getSplitKeys(int numRegions) {
    return getSplitKeys(numRegions, getRowKeyResolution());
  }

  /**
   * Generates a list of region boundaries evenly spaced in the HBase row key space.
   *
   * <p>  </p>
   *
   * @param numRegions Number of desired regions. Must be greater than 2 and less than the maximum
   *     number of regions allowed given the requested key precision.
   * @param precision Precision of the boundary split keys, in number of bytes.
   * @return The row keys that serve as the boundaries between the regions.
   *     Each boundary split key has the requested precision, in number of bytes.
   *     The number of boundaries is numRegions - 1.
   */
  public byte[][] getSplitKeys(int numRegions, int precision) {
    Preconditions.checkArgument(precision >= 1, "Invalid precision: %s.", precision);
    Preconditions.checkArgument(numRegions >= 2,
        "Number of regions must be at least 2, got %s.", numRegions);
    if (precision < 4) {
      // 4 bytes give more precision than a Java integer achieves:
      final long maxRegions = 1L << (8 * precision);
      Preconditions.checkArgument(numRegions <= maxRegions,
          "Number of regions must be at most %s, got %s.", maxRegions, numRegions);
    }

    // Create a byte array of all zeros.
    final byte[] startKey = new byte[precision];
    Arrays.fill(startKey, (byte) 0x00);

    // Create a byte array of all ones.
    final byte[] limitKey = new byte[precision];
    Arrays.fill(limitKey, (byte) 0xFF);

    // This result includes numRegions + 1 keys in it (includes the startKey and limitKey).
    final byte[][] ends = Bytes.split(startKey, limitKey, true, numRegions - 1);

    if (LOG.isDebugEnabled()) {
      for (int i = 0; i < ends.length; i++) {
        LOG.debug("Generated split key: {}", ByteArrayFormatter.toHex(ends[i], ':'));
      }
    }

    // Remove the startKey from the beginning and the limitKey from the end.
    return Arrays.copyOfRange(ends, 1, ends.length - 1);
  }
}
