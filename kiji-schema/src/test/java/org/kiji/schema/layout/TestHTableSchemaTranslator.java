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

package org.kiji.schema.layout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.impl.HTableSchemaTranslator;

public class TestHTableSchemaTranslator {
  @Test
  public void testTranslate() throws Exception {
    final HTableSchemaTranslator translator = new HTableSchemaTranslator();

    final KijiTableLayout tableLayout =
        KijiTableLayout.newLayout(KijiTableLayouts.getLayout(KijiTableLayouts.FULL_FEATURED));

    final KijiURI tableURI =
        KijiURI
            .newBuilder()
            .withInstanceName("myinstance")
            .withTableName(tableLayout.getName()).build();
    final HTableDescriptor tableDescriptor =
        translator.toHTableDescriptor(tableURI.getInstance(), tableLayout);

    assertEquals("kiji.myinstance.table.user", tableDescriptor.getNameAsString());
    assertEquals(2, tableDescriptor.getColumnFamilies().length);

    assertFalse(tableDescriptor.getFamily(Bytes.toBytes("B")).isInMemory());
    assertEquals(HConstants.ALL_VERSIONS,
        tableDescriptor.getFamily(Bytes.toBytes("B")).getMaxVersions());

    assertTrue(tableDescriptor.getFamily(Bytes.toBytes("C")).isInMemory());
    assertEquals(1, tableDescriptor.getFamily(Bytes.toBytes("C")).getMaxVersions());
  }
}
