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

import java.util.Map;

import org.junit.Test;

import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestEnvironmentBuilder {
  @Test
  public void testBuilder() throws Exception {
    final TableLayoutDesc layoutDesc = KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE);
    final KijiTableLayout layout = new KijiTableLayout(layoutDesc, null);

    final EntityIdFactory eidFactory = EntityIdFactory.create(layoutDesc.getKeysFormat());
    final EntityId id1 = eidFactory.fromKijiRowKey("row1");
    final EntityId id2 = eidFactory.fromKijiRowKey("row2");

    final Map<String, Kiji> environment = new EnvironmentBuilder()
        .withInstance("inst1")
            .withTable("table", layout)
                .withRow(id1)
                    .withFamily("family")
                        .withQualifier("column").withValue(2, "foo1")
                        .withQualifier("column").withValue(1, "foo2")
                .withRow(id2)
                    .withFamily("family")
                        .withQualifier("column").withValue(100, "foo3")
        .build();

    final KijiTable table = environment.get("inst1").openTable("table");
    final KijiTableReader reader = table.openTableReader();

    // Verify the first row.
    final KijiRowData row1 = reader.get(id1, new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("family", "column")));
    assertEquals("foo2", row1.getValue("family", "column", 1).toString());

    // Verify the second row.
    final KijiRowData row2 = reader.get(id2, new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("family", "column")));
    assertEquals("foo3", row2.getValue("family", "column", 100).toString());
  }
}
