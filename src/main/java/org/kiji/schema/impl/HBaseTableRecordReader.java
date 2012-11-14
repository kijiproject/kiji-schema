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

package org.kiji.schema.impl;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat;

/**
 * A record reader that reads from a HBase table that reports progress correctly.
 */
public class HBaseTableRecordReader extends TableRecordReader {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseTableRecordReader.class);

  private static final int ROW_KEY_RESOLUTION = 16;

  private RowKeyFormat mRowKeyFormat;
  private int mKeySize;
  private BigInteger mStartKey;
  private BigInteger mEndKey;
  private BigInteger mKeyRangeSize;

  /**
   * Constructs a new record reader.
   *
   * @param rowKeyFormat The row key encoding.
   */
  public HBaseTableRecordReader(RowKeyFormat rowKeyFormat) {
    mRowKeyFormat = rowKeyFormat;
  }

  @Override
  public void setScan(Scan scan) {
    super.setScan(scan);

    // Record the start key, end key, and range for computation of progress later.
    byte[] startRow = scan.getStartRow();
    byte[] stopRow = scan.getStopRow();

    final boolean hashingEnabled = (mRowKeyFormat.getEncoding() != RowKeyEncoding.RAW);

    if (!hashingEnabled) {
      if (null == startRow || 0 == startRow.length) {
        LOG.info("Won't be reporting progress for this input split, since row key hashing "
            + "is disabled and this is the first region in the table.");
        return;
      }
      if (null == stopRow || 0 == stopRow.length) {
        LOG.info("Won't be reporting progress for this input split, since row key hashing "
            + "is disabled and this is the last region in the table.");
        return;
      }
    }

    if (null == startRow || 0 == startRow.length) {
      // Use a bit string of all zeros.
      startRow = new byte[ROW_KEY_RESOLUTION];
      Arrays.fill(startRow, (byte) 0x00);
    }
    if (null == stopRow || 0 == stopRow.length) {
      // Use a bit string of all ones.
      stopRow = new byte[ROW_KEY_RESOLUTION];
      Arrays.fill(stopRow, (byte) 0xFF);
    }

    // Make the start/end keys the same length by padding with zeros on the right.
    if (startRow.length < stopRow.length) {
      startRow = Bytes.padTail(startRow, stopRow.length - startRow.length);
    } else if (stopRow.length < startRow.length) {
      stopRow = Bytes.padTail(stopRow, startRow.length - stopRow.length);
    }

    mKeySize = startRow.length;
    mStartKey = new BigInteger(1, startRow);
    mEndKey = new BigInteger(1, stopRow);
    mKeyRangeSize = mEndKey.subtract(mStartKey);

    LOG.info("Reporting progress based on the following stats...");
    LOG.info("StartKey: " + mStartKey.toString());
    LOG.info("LimitKey: " + mEndKey.toString());
    LOG.info("RangeSize: " + mKeyRangeSize.toString());
  }

  @Override
  public float getProgress() {
    if (null == mKeyRangeSize) {
      // Row key hashing is disabled and we're running on one of the regions
      // at the beginning or end of the key space.  Can't report sane progess.
      return 0.0f;
    }
    try {
      BigInteger currentKey
          = new BigInteger(1, forceToLength(getCurrentKey().get(), mKeySize));
      BigInteger progress = currentKey.subtract(mStartKey);
      return (float) (progress.doubleValue() / mKeyRangeSize.doubleValue());
    } catch (IOException e) {
      return 0.0f;
    } catch (InterruptedException e) {
      return 0.0f;
    } catch (IllegalStateException e) {
      return 0.0f;
    }
  }

  /**
   * Creates a new byte array of size <code>length</code>.  If the original array is too
   * short, it is padded with zeros to on the right.  If the original array is too long,
   * it is truncated on the right.  If is is the right size, the original is simply returned.
   *
   * @param original A byte array.
   * @param length   The length of the new array to return.
   * @return The original byte array but either truncated or padded to the right length.
   */
  private byte[] forceToLength(byte[] original, int length) {
    if (original.length < length) {
      return Bytes.padTail(original, length - original.length);
    }
    if (original.length > length) {
      return Bytes.head(original, length);
    }
    return original;
  }
}
