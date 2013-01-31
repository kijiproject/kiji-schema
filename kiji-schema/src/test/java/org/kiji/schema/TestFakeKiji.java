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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.VersionInfo;

/** Basic tests for a fake Kiji instance. */
public class TestFakeKiji {
  private static final Logger LOG = LoggerFactory.getLogger(TestFakeKiji.class);

  @Test
  public void testFakeKiji() throws Exception {
    LOG.info("Opening an in-memory kiji instance");
    final KijiURI uri = KijiURI.newBuilder("kiji://.fake.kiji/instance").build();
    final Configuration conf = HBaseConfiguration.create();
    KijiInstaller.get().install(uri, conf);

    final Kiji kiji = Kiji.Factory.open(uri, conf);

    LOG.info(String.format("Opened fake Kiji '%s'.", kiji.getURI()));

    final KijiSystemTable systemTable = kiji.getSystemTable();
    assertEquals("Client version should match installed version",
        VersionInfo.getClientDataVersion(), systemTable.getDataVersion());

    assertNotNull(kiji.getSchemaTable());
    assertNotNull(kiji.getMetaTable());

    {
      final TableLayoutDesc layoutDesc = KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE);
      final KijiTableLayout tableLayout = KijiTableLayout.newLayout(layoutDesc);
      kiji.createTable("table", tableLayout);
    }

    final KijiTable table = kiji.openTable("table");
    {
      final KijiDataRequestBuilder builder = KijiDataRequest.builder();
      builder.addColumns().addFamily("family");
      final KijiDataRequest dataRequest = builder.build();
      final KijiRowScanner scanner =
          table.openTableReader().getScanner(dataRequest);
      assertFalse(scanner.iterator().hasNext());
      scanner.close();
    }

    {
      final KijiTableWriter writer = table.openTableWriter();
      writer.put(table.getEntityId("row1"), "family", "column", "the string value");
      writer.close();
    }

    {
      final KijiTableReader reader = table.openTableReader();
      final KijiDataRequestBuilder builder = KijiDataRequest.builder();
      builder.addColumns().addFamily("family");
      final KijiDataRequest dataRequest = builder.build();
      final KijiRowScanner scanner =
          table.openTableReader().getScanner(dataRequest);
      final Iterator<KijiRowData> it = scanner.iterator();
      assertTrue(it.hasNext());
      KijiRowData row = it.next();
      assertEquals("the string value", row.getMostRecentValue("family", "column").toString());
      assertFalse(it.hasNext());
      scanner.close();
      reader.close();
    }

    table.close();
    kiji.release();
  }
}
