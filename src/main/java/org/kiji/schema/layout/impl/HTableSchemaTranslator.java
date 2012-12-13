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

package org.kiji.schema.layout.impl;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiManagedHBaseTableName;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout;


/**
 * Translates between KijiTableLayouts and HTableDescriptors.
 *
 * <p>A Kiji table has a layout with locality groups, families, and columns.  An HTable
 * only has HColumns.  This classes maps between the Kiji layout components and the HTable
 * schema components, which ultimately determines how we map Kiji data onto an HTable.</p>
 */
@ApiAudience.Private
public final class HTableSchemaTranslator {
  /**
   * Creates a new <code>HTableSchemaTranslator</code> instance.
   */
  public HTableSchemaTranslator() {
  }

  /**
   * Translates a Kiji table layout into an HColumnDescriptor.
   *
   * @param kijiInstanceName The name of the Kiji instance the table lives in.
   * @param tableLayout The Kiji table layout.
   * @return The HTableDescriptor to use for storing the Kiji table data.
   */
  public HTableDescriptor toHTableDescriptor(String kijiInstanceName, KijiTableLayout tableLayout) {
    // Figure out the name of the table.
    final String tableName = tableLayout.getName();
    final KijiManagedHBaseTableName hbaseTableName =
        KijiManagedHBaseTableName.getKijiTableName(kijiInstanceName, tableName);
    final HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName.toString());

    // Add the columns.
    for (LocalityGroupLayout localityGroup : tableLayout.getLocalityGroupMap().values()) {
      tableDescriptor.addFamily(toHColumnDescriptor(localityGroup));
    }

    return tableDescriptor;
  }

  /**
   * Translates a Kiji locality group into an HColumnDescriptor.
   *
   * @param localityGroup A Kiji locality group.
   * @return The HColumnDescriptor to use for storing the data in the locality group.
   */
  private static HColumnDescriptor toHColumnDescriptor(LocalityGroupLayout localityGroup) {
    return new HColumnDescriptor(
        localityGroup.getId().toByteArray(),  // HBase family name
        localityGroup.getDesc().getMaxVersions(),
        localityGroup.getDesc().getCompressionType().toString(),
        localityGroup.getDesc().getInMemory(),
        true,  // block cache
        localityGroup.getDesc().getTtlSeconds(),
        HColumnDescriptor.DEFAULT_BLOOMFILTER);
  }
}
