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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.junit.Test;

import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.impl.InMemoryMetaTable;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestInMemoryMetaTable {

  @Test(expected=KijiTableNotFoundException.class)
  public void testEmpty() throws IOException {
    InMemoryMetaTable metaTable = new InMemoryMetaTable();
    assertNull(metaTable.getTableLayout("table"));
  }

  @Test
  public void testLayouts() throws Exception {
    InMemoryMetaTable metaTable = new InMemoryMetaTable();

    final TableLayoutDesc desc = KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE);
    final KijiTableLayout layout = new KijiTableLayout(desc, null);
    metaTable.updateTableLayout("table", desc);

    assertEquals(layout, metaTable.getTableLayout("table"));

    metaTable.close();
  }
}
