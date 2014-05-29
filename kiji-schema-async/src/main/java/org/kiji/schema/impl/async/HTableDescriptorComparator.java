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

package org.kiji.schema.impl.hbase;

import java.util.Comparator;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;

/**
 * Compares {@link org.apache.hadoop.hbase.HTableDescriptor}s.  They
 * are equal if they have the same HTable name, same max file size,
 * memstore flushsize with the same column families.
 */
@ApiAudience.Private
public final class HTableDescriptorComparator implements Comparator<HTableDescriptor> {
  @Override
  public int compare(HTableDescriptor o1, HTableDescriptor o2) {
    int nameResult = o1.getNameAsString().compareTo(o2.getNameAsString());
    if (nameResult != 0) {
      return nameResult;
    }
    int maxFileSizeResult = Long.valueOf(o1.getMaxFileSize()).compareTo(o2.getMaxFileSize());
    if (maxFileSizeResult != 0) {
          return maxFileSizeResult;
    }
    int memstoreFlushSizeResult =
            Long.valueOf(o1.getMemStoreFlushSize()).compareTo(o2.getMemStoreFlushSize());
    if (memstoreFlushSizeResult != 0) {
        return memstoreFlushSizeResult;
    }
    HColumnDescriptor[] families1 = o1.getColumnFamilies();
    HColumnDescriptor[] families2 = o2.getColumnFamilies();
    int familiesResult = Integer.valueOf(families1.length).compareTo(families2.length);
    if (familiesResult != 0) {
      return familiesResult;
    }

    HColumnDescriptorComparator columnComparator = new HColumnDescriptorComparator();
    for (int i = 0; i < families1.length; i++) {
      int result = columnComparator.compare(families1[i], families2[i]);
      if (result != 0) {
        return result;
      }
    }
    return 0;
  }


  /**
   * Creates an HTableDescriptor that looks "empty" from the perspective of
   * HTableDescriptorComparator and KijiAdmin.setTableLayout().
   *
   * @param hbaseTableName the HBase table name being represented by this "empty table."
   * @return an HTableDescriptor that represents a table with no column families.
   */
  public static HTableDescriptor makeEmptyTableDescriptor(
      final KijiManagedHBaseTableName hbaseTableName) {
    return new HTableDescriptor() {
      @Override
      public String getNameAsString() {
        return hbaseTableName.toString();
      }

      @Override
      public HColumnDescriptor[] getColumnFamilies() {
        return new HColumnDescriptor[0];
      }

      @Override
      public HColumnDescriptor getFamily(byte[] name) {
        return null;
      }
    };
  }

}
