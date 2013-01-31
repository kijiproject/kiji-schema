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
import static org.junit.Assert.fail;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;

/** Tests final columns. */
public class TestFinalColumns extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestFinalColumns.class);

  @Test
  public void testFakeKiji() throws Exception {
    final Kiji kiji = getKiji();

    final TableLayoutDesc layoutDesc = KijiTableLayouts.getLayout(KijiTableLayouts.FINAL_COLUMN);
    final KijiTableLayout tableLayout = KijiTableLayout.newLayout(layoutDesc);
    kiji.createTable("table", tableLayout);

    final KijiTable table = kiji.openTable("table");
    final KijiTableWriter writer = table.openTableWriter();

    final EntityId eid = table.getEntityId("row-key");
    writer.put(eid, "family", "column", "string value");

    try {
      writer.put(eid, "family", "column", 1L);
      fail("Final string column should not accept anything else but strings.");
    } catch (KijiEncodingException kee) {
      LOG.info("Expected error: " + kee);
    }

    final KijiTableReader reader = table.openTableReader();
    final KijiRowData row = reader.get(eid, KijiDataRequest.create("family", "column"));
    assertEquals("string value", row.getMostRecentValue("family", "column").toString());

    writer.close();
    table.close();
  }
}
