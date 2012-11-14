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

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.impl.ColumnId;

public class TestKijiAdmin extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestKijiAdmin.class);

  private KijiAdmin mKijiAdmin;
  private HBaseAdmin mHbaseAdmin;

  @Before
  public void setup() throws IOException {
    mHbaseAdmin = createStrictMock(HBaseAdmin.class);
    mKijiAdmin = new KijiAdmin(mHbaseAdmin, getKiji());
  }

  @Test
  public void testCreateTable() throws Exception {
    // Set mock expectations.
    HTableDescriptor tableDescriptor = new HTableDescriptor("kiji.default.table.table");
    HColumnDescriptor columnDescriptor = new HColumnDescriptor(
        new ColumnId(1).toByteArray(),  // family name
        3,  // max versions
        Compression.Algorithm.GZ.toString(),  // compression
        false,  // in memory
        true,  // block cache
        HConstants.FOREVER,  // TTL
        HColumnDescriptor.DEFAULT_BLOOMFILTER);
    tableDescriptor.addFamily(columnDescriptor);
    mHbaseAdmin.createTable(tableDescriptor);

    final TableLayoutDesc desc = KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE);
    final KijiTableLayout tableLayout = new KijiTableLayout(desc, null);

    replay(mHbaseAdmin);
    mKijiAdmin.createTable("table", tableLayout, false);
    verify(mHbaseAdmin);

    assertEquals(tableLayout.getName().toString(),
        getKiji().getMetaTable().getTableLayout("table").getName().toString());
  }

  @Test
  public void testSetTableLayoutAdd() throws Exception {
    final TableLayoutDesc originalLayoutDesc = KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE);
    getKiji().getMetaTable().updateTableLayout("table", originalLayoutDesc);

    HTableDescriptor tableDescriptor = new HTableDescriptor("kiji.default.table.table");
    tableDescriptor.addFamily(new HColumnDescriptor(
            new ColumnId(1).toByteArray(),  // family name
            3,  // max versions
            Compression.Algorithm.GZ.toString(),  // compression
            false,  // in memory
            true,  // block cche
            HConstants.FOREVER,  // TTL
            HColumnDescriptor.DEFAULT_BLOOMFILTER));
    expect(mHbaseAdmin.getTableDescriptor(
        EasyMock.aryEq(Bytes.toBytes("kiji.default.table.table"))))
        .andReturn(tableDescriptor);
    HColumnDescriptor columnDescriptor = new HColumnDescriptor(
        new ColumnId(2).toByteArray(),  // family name
        1,  // max versions
        Compression.Algorithm.GZ.toString(),  // compression
        false,  // in memory
        true,  // block cache
        HConstants.FOREVER,  // TTL
        HColumnDescriptor.DEFAULT_BLOOMFILTER);
    mHbaseAdmin.disableTable("kiji.default.table.table");
    mHbaseAdmin.addColumn("kiji.default.table.table", columnDescriptor);
    mHbaseAdmin.enableTable("kiji.default.table.table");

    final TableLayoutDesc newTableLayoutDesc =
        KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE_UPDATE_NEW_LOCALITY_GROUP);

    replay(mHbaseAdmin);
    LOG.debug("Existing layout: "
        + getKiji().getMetaTable().getTableLayout("table").toString());
    final KijiTableLayout tableLayout = mKijiAdmin.setTableLayout("table", newTableLayoutDesc);
    verify(mHbaseAdmin);

    assertEquals(tableLayout.getFamilies().size(),
        getKiji().getMetaTable().getTableLayout("table").getFamilies().size());
  }

  @Test
  public void testSetTableLayoutModify() throws Exception {
    final TableLayoutDesc initialLayoutDesc = KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE);
    getKiji().getMetaTable().updateTableLayout("table", initialLayoutDesc);

    HTableDescriptor tableDescriptor = new HTableDescriptor("kiji.default.table.table");
    HColumnDescriptor oldColumnDescriptor = new HColumnDescriptor(
        new ColumnId(1).toByteArray(),  // family name
        3,  // max versions
        Compression.Algorithm.GZ.toString(),  // compression
        false,  // in memory
        true,  // block cache
        HConstants.FOREVER,  // TTL
        HColumnDescriptor.DEFAULT_BLOOMFILTER);
    tableDescriptor.addFamily(oldColumnDescriptor);
    expect(mHbaseAdmin.getTableDescriptor(
        EasyMock.aryEq(Bytes.toBytes("kiji.default.table.table"))))
        .andReturn(tableDescriptor);
    HColumnDescriptor columnDescriptor = new HColumnDescriptor(
        new ColumnId(1).toByteArray(),  // family name
        1,  // max versions
        Compression.Algorithm.GZ.toString(),  // compression
        false,  // in memory
        true,  // block cache
        HConstants.FOREVER,  // TTL
        HColumnDescriptor.DEFAULT_BLOOMFILTER);
    mHbaseAdmin.disableTable("kiji.default.table.table");
    mHbaseAdmin.modifyColumn("kiji.default.table.table", columnDescriptor);
    mHbaseAdmin.enableTable("kiji.default.table.table");

    final TableLayoutDesc newTableLayoutDesc =
        TableLayoutDesc.newBuilder(initialLayoutDesc).build();
    assertEquals(3, (int) newTableLayoutDesc.getLocalityGroups().get(0).getMaxVersions());
    newTableLayoutDesc.getLocalityGroups().get(0).setMaxVersions(1);

    replay(mHbaseAdmin);
    final KijiTableLayout newTableLayout = mKijiAdmin.setTableLayout("table", newTableLayoutDesc);
    verify(mHbaseAdmin);

    assertEquals(
        newTableLayout.getLocalityGroupMap().get("default").getDesc().getMaxVersions(),
        getKiji().getMetaTable().getTableLayout("table").getLocalityGroupMap().get("default")
            .getDesc().getMaxVersions());
  }

  @Test(expected=IllegalArgumentException.class)
  public void testSetTableLayoutEmpty() throws Exception {
    final TableLayoutDesc tableLayoutDesc = new TableLayoutDesc();

    replay(mHbaseAdmin);
    mKijiAdmin.setTableLayout("", tableLayoutDesc);
    verify(mHbaseAdmin);
  }

  @Test(expected=KijiTableNotFoundException.class)
  public void testDeleteTable() throws Exception {
    mHbaseAdmin.disableTable("kiji.default.table.table_imaginary");
    mHbaseAdmin.deleteTable("kiji.default.table.table_imaginary");
    expect(mHbaseAdmin.tableExists("kiji.default.table.table_imaginary")).andReturn(false);

    final TableLayoutDesc tableLayoutDesc = KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE);
    getKiji().getMetaTable().updateTableLayout("table_imaginary", tableLayoutDesc);

    // There's a row in the meta table.
    assertNotNull(getKiji().getMetaTable().getTableLayout("table_imaginary"));

    replay(mHbaseAdmin);
    mKijiAdmin.deleteTable("table_imaginary");
    verify(mHbaseAdmin);

    // Make sure it was deleted from the meta table, too.
    // The following line should throw a KijiTableNotFoundException.
    getKiji().getMetaTable().getTableLayout("table_imaginary");
  }

  @Test(expected=KijiTableNotFoundException.class)
  public void testSetTableLayoutOnATableThatDoesNotExist() throws Exception {
    final TableLayoutDesc tableLayoutDesc = KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE);
    mKijiAdmin.setTableLayout("table", tableLayoutDesc);
  }
}
