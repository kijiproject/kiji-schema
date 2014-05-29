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

package org.kiji.schema.filter;

import static junit.framework.Assert.assertEquals;

import org.apache.hadoop.hbase.HConstants;
import org.junit.Test;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

/** Tests for {@link KijiFirstKeyOnlyColumnFilter}. */
public class TestKijiFirstKeyOnlyColumnFilter extends KijiClientTest {

  @Test
  public void testHashCodeAndEquals() {
    KijiFirstKeyOnlyColumnFilter filter1 = new KijiFirstKeyOnlyColumnFilter();
    KijiFirstKeyOnlyColumnFilter filter2 = new KijiFirstKeyOnlyColumnFilter();

    assertEquals(filter1.hashCode(), filter2.hashCode());
    assertEquals(filter1, filter2);
  }

  /**
   * Validate the behavior of the FirstKeyOnly column filter: in particular, applying
   * this filter to one column should not affect other columns.
   */
  @Test
  public void testFirstKeyOnlyColumnFilter() throws Exception {
    final Kiji kiji = new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(KijiTableLayouts.FULL_FEATURED))
            .withRow("row")
                .withFamily("info")
                    .withQualifier("name")
                        .withValue(1L, "name1")
                        .withValue(2L, "name2")
                    .withQualifier("email")
                        .withValue(1L, "email1")
                        .withValue(2L, "email2")
        .build();
    final KijiTable table = kiji.openTable("user");
    try {
      final KijiTableReader reader = table.openTableReader();
      try {
        final EntityId eid = table.getEntityId("row");

        // Make sure we have 2 versions in columns info:name and in info:email.
        {
          final KijiDataRequest dataRequest = KijiDataRequest.builder()
              .addColumns(ColumnsDef.create()
                  .withMaxVersions(HConstants.ALL_VERSIONS)
                  .addFamily("info"))
              .build();
          final KijiRowData row = reader.get(eid, dataRequest);
          assertEquals(2, row.getValues("info", "name").size());
          assertEquals(2, row.getValues("info", "email").size());
        }

        // Test FirstKeyOnly filter when applied on the group-type family.
        {
          final KijiDataRequest dataRequest = KijiDataRequest.builder()
              .addColumns(ColumnsDef.create()
                  .withMaxVersions(HConstants.ALL_VERSIONS)
                  .withFilter(new KijiFirstKeyOnlyColumnFilter())
                  .addFamily("info"))
              .build();
          final KijiRowData row = reader.get(eid, dataRequest);
          assertEquals(1, row.getValues("info", "name").size());
          assertEquals(1, row.getValues("info", "email").size());
        }

        // Test FirstKeyOnly filter when applied on a single column.
        // Make sure it doesn't affect other columns.
        {
          final KijiDataRequest dataRequest = KijiDataRequest.builder()
              .addColumns(ColumnsDef.create()
                  .withMaxVersions(HConstants.ALL_VERSIONS)
                  .withFilter(new KijiFirstKeyOnlyColumnFilter())
                  .add("info", "name"))
              .addColumns(ColumnsDef.create()
                  .withMaxVersions(HConstants.ALL_VERSIONS)
                  .add("info", "email"))
              .build();
          final KijiRowData row = reader.get(eid, dataRequest);
          assertEquals(1, row.getValues("info", "name").size());
          assertEquals(2, row.getValues("info", "email").size());
        }

        // Test FirstKeyOnly filter when applied on a single column.
        // Make sure it doesn't affect other columns.
        {
          final KijiDataRequest dataRequest = KijiDataRequest.builder()
              .addColumns(ColumnsDef.create()
                  .withMaxVersions(HConstants.ALL_VERSIONS)
                  .add("info", "name"))
              .addColumns(ColumnsDef.create()
                  .withMaxVersions(HConstants.ALL_VERSIONS)
                  .withFilter(new KijiFirstKeyOnlyColumnFilter())
                  .add("info", "email"))
              .build();
          final KijiRowData row = reader.get(eid, dataRequest);
          assertEquals(2, row.getValues("info", "name").size());
          assertEquals(1, row.getValues("info", "email").size());
        }

      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }
}
