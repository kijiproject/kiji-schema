/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.schema.impl.async;

import java.io.IOException;

import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.hbase.async.TableNotFoundException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.impl.TestStrictValidation;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.TableLayoutBuilder;
import org.kiji.schema.util.InstanceBuilder;

/** Tests for AsyncKiji. */
public class TestAsyncKiji extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestAsyncKiji.class);

  private KijiURI createTestURI() {
    final String instanceName = String.format("%s_%s_%d",
        getClass().getSimpleName(),
        mTestName.getMethodName(),
        0);
    final KijiURI uri = KijiURI.newBuilder(createTestHBaseURI())
        .withInstanceName(instanceName).build();
    return uri;
  }
  private void assertEqualColumnSchemas(TableLayoutDesc expected, TableLayoutDesc actual) {
    Assert.assertEquals(
        expected.getLocalityGroups().get(0).getFamilies().get(0)
            .getColumns().get(0).getColumnSchema(),
        actual.getLocalityGroups().get(0).getFamilies().get(0)
            .getColumns().get(0).getColumnSchema());

  }

  /** Tests Kiji.openTable() on a table that doesn't exist. */
  @Test
  public void testOpenUnknownTable() throws Exception {
    try {
      final Kiji kiji = new AsyncKiji(createTestURI());
      Assert.fail("Should not be able to open a table that does not exist!");
    } catch(TableNotFoundException tnfe) {
      // Expected!
      LOG.debug("Expected error: {}", tnfe);
      Assert.assertEquals(
          "\"kiji.TestAsyncKiji_testOpenUnknownTable_0.system\"",
          tnfe.getMessage());
    }
  }

  @Test
  public void testModifyTableLayoutAddRemove() throws IOException {
    final TableLayoutDesc layoutDesc = KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE);
    layoutDesc.setVersion("layout-1.3.0");

    final Kiji hbaseKiji =  new InstanceBuilder(getKiji())
        .withTable("table", KijiTableLayout.newLayout(layoutDesc))
        .withRow("foo")
        .withFamily("family")
        .withQualifier("column").withValue(1L, "foo-val")
        .build();
    final Kiji kiji = new AsyncKiji(hbaseKiji.getURI());
    final KijiTable table = kiji.openTable("table");
    final TableLayoutDesc newDesc = kiji.getMetaTable().getTableLayout("table").getDesc();
    newDesc.setReferenceLayout("1");
    newDesc.setLayoutId("2");

    /** Test modifying with no changes */
    Assert.assertEquals(kiji.modifyTableLayout(newDesc).getDesc(), newDesc);
    Assert.assertEquals(kiji.getMetaTable().getTableLayout("table").getDesc(), newDesc);

    /** Test Adding throws appropriate exception */
    final TableLayoutDesc layoutDescNewCol =
        KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE_UPDATE_NEW_COLUMN);
    layoutDescNewCol.setReferenceLayout("2");
    layoutDescNewCol.setLayoutId("3");

    try {
      kiji.modifyTableLayout(layoutDescNewCol);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "AsyncKiji cannot add or remove columns");
    }

    final TableLayoutDesc layoutDescNewFamily =
        KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE_UPDATE_NEW_FAMILY);
    layoutDescNewFamily.setReferenceLayout("2");
    layoutDescNewFamily.setLayoutId("3");
    try {
      kiji.modifyTableLayout(layoutDescNewFamily);
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "AsyncKiji cannot add or remove families");
    }

    final TableLayoutDesc layoutDescNewLocGroup =
        KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE_UPDATE_NEW_LOCALITY_GROUP);
    layoutDescNewLocGroup.setReferenceLayout("2");
    layoutDescNewLocGroup.setLayoutId("3");
    try {
      kiji.modifyTableLayout(layoutDescNewLocGroup);
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "AsyncKiji cannot add or remove locality groups");
    }

    /** Test Deleting throws appropriate exception */
    final TableLayoutDesc layoutDescDeleteCol =
        KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE_UPDATE_DELETE_COLUMN);
    layoutDescDeleteCol.setReferenceLayout("1");
    layoutDescDeleteCol.setLayoutId("2");
    try {
      kiji.modifyTableLayout(layoutDescDeleteCol);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "AsyncKiji cannot add or remove columns");
    }

    final TableLayoutDesc layoutDescDeleteFam =
        KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE_UPDATE_DELETE_FAMILY);
    layoutDescDeleteFam.setReferenceLayout("1");
    layoutDescDeleteFam.setLayoutId("2");
    try {
      kiji.modifyTableLayout(layoutDescDeleteFam);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "AsyncKiji cannot add or remove families");
    }

    final TableLayoutDesc layoutDescDeleteLocGroup =
        KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE_UPDATE_DELETE_LOCALITY_GROUP);
    layoutDescDeleteLocGroup.setReferenceLayout("1");
    layoutDescDeleteLocGroup.setLayoutId("2");
    try {
      kiji.modifyTableLayout(layoutDescDeleteLocGroup);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "AsyncKiji cannot add or remove locality groups");
    }

    table.release();
    kiji.release();

  }

  @Test
  public void testModifyTableLayoutReaderWriterSchemas() throws IOException {
    final TableLayoutDesc layoutDesc = KijiTableLayouts.getLayout(TestStrictValidation.LAYOUT_ENUM);

    final Kiji hbaseKiji =  new InstanceBuilder(getKiji())
        .withTable("table", KijiTableLayout.newLayout(layoutDesc))
        .build();
    final Kiji kiji = new AsyncKiji(hbaseKiji.getURI());
    final KijiTable table = kiji.openTable("table");

    /** Test updating Reader Schemas */

    final Schema readerEnum =
        Schema.createEnum("Gender", null, null, ImmutableList.of("FEMALE", "MALE", "OTHER"));
    final TableLayoutDesc updateWithReader =
        new TableLayoutBuilder(layoutDesc, kiji)
            .withReader(KijiColumnName.create("info", "gender"), readerEnum)
            .build();
    updateWithReader.setLayoutId("2");
    updateWithReader.setReferenceLayout("1");
    final KijiTableLayout result = kiji.modifyTableLayout(updateWithReader);

    //Check that the returned result has actually been written to the meta table
    Assert.assertEquals(
        result,
        kiji.getMetaTable().getTableLayout("table"));

    //Check that the reader schema has been updated
    assertEqualColumnSchemas(updateWithReader, result.getDesc());

    /** Test updating Writer Schemas */
    final Schema writerEnum =
        Schema.createEnum("Gender", null, null, ImmutableList.of("FEMALE", "MALE", "OTHER"));
    final TableLayoutDesc updateWithWriter =
        new TableLayoutBuilder(layoutDesc, kiji)
            .withWriter(KijiColumnName.create("info", "gender"), writerEnum)
            .build();
    updateWithWriter.setLayoutId("3");
    updateWithWriter.setReferenceLayout("2");
    final KijiTableLayout result2 = kiji.modifyTableLayout(updateWithWriter);

    //Check that the returned result has actually been written to the meta table
    Assert.assertEquals(
        result2,
        kiji.getMetaTable().getTableLayout("table"));

    //Check that the writer schema has been updated
    assertEqualColumnSchemas(updateWithWriter, result2.getDesc());

    table.release();
    kiji.release();

  }
}
