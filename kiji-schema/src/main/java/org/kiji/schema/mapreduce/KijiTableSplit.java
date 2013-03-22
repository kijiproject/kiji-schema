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

package org.kiji.schema.mapreduce;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * An KijiTableSplit stores exactly the same data as TableSplit, but
 * does a better job handling default split sizes.
 */
@ApiAudience.Public
@ApiStability.Evolving
@Deprecated
class KijiTableSplit extends TableSplit {
  private byte[] mRegionStartKey;
  private long mSplitSize; // lazily calculated and populated via getLength().

  /** The default constructor. */
  public KijiTableSplit() {
    super();
  }

  /**
   * Create a new KijiTableSplit instance from an HBase TableSplit.
   * @param tableSplit the HBase TableSplit to clone.
   * @param regionStartKey the starting key of the region associated with this split.
   */
  public KijiTableSplit(TableSplit tableSplit, byte[] regionStartKey) {
    super(tableSplit.getTableName(), tableSplit.getStartRow(), tableSplit.getEndRow(),
        tableSplit.getRegionLocation());

    checkNotNull(regionStartKey);
    mRegionStartKey = regionStartKey;
  }

  /**
   * Returns the start key of the region associated with this split.  If this split
   * is the first in the total scan, this may differ from the value returned by
   * getStartRow().
   *
   * @return the start key of the region associated with this split.
   */
  public byte[] getRegionStartKey() {
    return mRegionStartKey;
  }

  /**
   * Returns the length of the split.
   *
   * This method does not currently examine the data in the region
   * represented by the split. We assume that each split is 3/4 full (where
   * "full" is defined as hbase.hregion.max.filesize). If the region had
   * that many bytes in it, it would split in two, each containing 1/2 that
   * many bytes. So we expect, on average, regions to be halfway between
   * "newly split" and "just about to split."
   *
   * @return the length of the split.
   * @see org.apache.hadoop.mapreduce.InputSplit#getLength()
   */
  @Override
  public long getLength() {
    if (0 == mSplitSize) {
      // Calculate this value once and memoize its result.
      Configuration conf = new Configuration();
      conf = HBaseConfiguration.addHbaseResources(conf);
      mSplitSize = (conf.getLong("hbase.hregion.max.filesize", 0) * 4) / 3;
      if (0 == mSplitSize) {
        // Set this to some reasonable non-zero default if the HBase properties
        // weren't set correctly.
        mSplitSize = 64 * 1024 * 1024; // 64 MB
      }
    }

    return mSplitSize;
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in); // read all superclass fields.
    mRegionStartKey = Bytes.readByteArray(in);
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out); // write all superclass fields.
    Bytes.writeByteArray(out, mRegionStartKey);
  }
}
