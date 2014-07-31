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

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.junit.Test;

import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.testing.fakehtable.FakeHTable;

public class TestHBaseSystemTable {
  @Test
  public void testSetDataVersion() throws IOException {
    final Configuration conf = HBaseConfiguration.create();
    final HTableDescriptor desc = new HTableDescriptor();
    final FakeHTable table = new FakeHTable("system", desc, conf, false, 0, true, true, null);

    final KijiURI uri = KijiURI.newBuilder("kiji://test/instance").build();
    final HBaseSystemTable systemTable = new HBaseSystemTable(uri, table);
    systemTable.setDataVersion(ProtocolVersion.parse("kiji-100"));
    assertEquals(ProtocolVersion.parse("kiji-100"), systemTable.getDataVersion());
    systemTable.close();
  }
}
