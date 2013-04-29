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

package org.kiji.schema.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestInstanceBuilder extends KijiClientTest {
  @Test
  public void testBuilder() throws Exception {
    final KijiTableLayout layout =
        KijiTableLayout.newLayout(KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE));

    final Kiji kiji = new InstanceBuilder(getKiji())
        .withTable("table", layout)
            .withRow("row1")
                .withFamily("family")
                    .withQualifier("column").withValue(1, "foo1")
                                            .withValue(2, "foo2")
            .withRow("row2")
                .withFamily("family")
                    .withQualifier("column").withValue(100, "foo3")
        .build();

    final KijiTable table = kiji.openTable("table");
    final KijiTableReader reader = table.openTableReader();

    // Verify the first row.
    final KijiDataRequest req = KijiDataRequest.create("family", "column");
    final KijiRowData row1 = reader.get(table.getEntityId("row1"), req);
    assertEquals("foo2", row1.getValue("family", "column", 2).toString());

    // Verify the second row.
    final KijiRowData row2 = reader.get(table.getEntityId("row2"), req);
    assertEquals("foo3", row2.getValue("family", "column", 100).toString());

    ResourceUtils.closeOrLog(reader);
    ResourceUtils.releaseOrLog(table);
  }
}
