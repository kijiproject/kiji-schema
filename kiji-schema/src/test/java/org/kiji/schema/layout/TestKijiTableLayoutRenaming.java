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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.ColumnDesc;
import org.kiji.schema.avro.ComponentType;
import org.kiji.schema.avro.CompressionType;
import org.kiji.schema.avro.FamilyDesc;
import org.kiji.schema.avro.HashSpec;
import org.kiji.schema.avro.LocalityGroupDesc;
import org.kiji.schema.avro.RowKeyComponent;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.avro.SchemaStorage;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.avro.TableLayoutDesc;

import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;

/** Tests for KijiTableLayout. */
public class TestKijiTableLayoutRenaming {
  private static final Logger LOG = LoggerFactory.getLogger(TestKijiTableLayoutRenaming.class);

  private static final String TABLE_LAYOUT_VERSION = "layout-1.1";

  private RowKeyFormat2 makeHashPrefixedRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("NAME").setType(ComponentType.STRING).build());

    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat2 mFormat = makeHashPrefixedRowKeyFormat();
  /** Reference layout descriptor with a single column: "family_name:column_name". */
  private final TableLayoutDesc mRefLayoutDesc = TableLayoutDesc.newBuilder()
      .setName("table_name")
      .setKeysFormat(mFormat)
      .setVersion(TABLE_LAYOUT_VERSION)
      .setLocalityGroups(Lists.newArrayList(
          LocalityGroupDesc.newBuilder()
          .setName("locality_group_name")
          .setInMemory(false)
          .setTtlSeconds(84600)
          .setMaxVersions(1)
          .setCompressionType(CompressionType.GZ)
          .setFamilies(Lists.newArrayList(
              FamilyDesc.newBuilder()
                  .setName("family_name")
                  .setColumns(Lists.newArrayList(
                      ColumnDesc.newBuilder()
                          .setName("column_name")
                          .setColumnSchema(CellSchema.newBuilder()
                               .setStorage(SchemaStorage.UID)
                               .setType(SchemaType.INLINE)
                               .setValue("\"string\"")
                               .build())
                          .build()))
                  .build()))
          .build()))
      .build();

  /** Reference layout with a single column: "family_name:column_name". */
  private final KijiTableLayout mRefLayout;

  public TestKijiTableLayoutRenaming() throws Exception {
    mRefLayout = KijiTableLayout.newLayout(mRefLayoutDesc);
  }

  @Test
  public void testReference() throws Exception {
    final FamilyLayout fLayout = mRefLayout.getFamilyMap().get("family_name");
    assertEquals(1, fLayout.getId().getId());
    final ColumnLayout cLayout = fLayout.getColumnMap().get("column_name");
    assertEquals(1, cLayout.getId().getId());
  }

  @Test
  public void testRenameColumn() throws Exception {
    final TableLayoutDesc desc2 = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(mFormat)
        .setVersion(TABLE_LAYOUT_VERSION)
        .setLocalityGroups(Lists.newArrayList(
            LocalityGroupDesc.newBuilder()
            .setName("locality_group_name")
            .setInMemory(false)
            .setTtlSeconds(84600)
            .setMaxVersions(1)
            .setCompressionType(CompressionType.GZ)
            .setFamilies(Lists.newArrayList(
                FamilyDesc.newBuilder()
                    .setName("family_name")
                    .setColumns(Lists.newArrayList(
                        ColumnDesc.newBuilder()
                            .setName("renamed_column_name")
                            .setRenamedFrom("column_name")
                            .setColumnSchema(CellSchema.newBuilder()
                                 .setStorage(SchemaStorage.UID)
                                 .setType(SchemaType.INLINE)
                                 .setValue("\"string\"")
                                 .build())
                            .build()))
                    .build()))
            .build()))
        .build();
    final KijiTableLayout layout2 = KijiTableLayout.createUpdatedLayout(desc2, mRefLayout);
    final FamilyLayout fLayout2 = layout2.getFamilyMap().get("family_name");
    assertEquals(1, fLayout2.getColumnMap().size());
    assertNull(fLayout2.getColumnMap().get("column_name"));
    assertNotNull(fLayout2.getColumnMap().get("renamed_column_name"));
    assertEquals(1, fLayout2.getColumnMap().get("renamed_column_name").getId().getId());
  }

  @Test
  public void testIllegalImplicitColumnRenaming() throws Exception {
    final TableLayoutDesc desc2 = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(mFormat)
        .setVersion(TABLE_LAYOUT_VERSION)
        .setLocalityGroups(Lists.newArrayList(
            LocalityGroupDesc.newBuilder()
            .setName("locality_group_name")
            .setInMemory(false)
            .setTtlSeconds(84600)
            .setMaxVersions(1)
            .setCompressionType(CompressionType.GZ)
            .setFamilies(Lists.newArrayList(
                FamilyDesc.newBuilder()
                    .setName("family_name")
                    .setColumns(Lists.newArrayList(
                        ColumnDesc.newBuilder()
                            .setName("renamed_column_name")
                            .setColumnSchema(CellSchema.newBuilder()
                                 .setStorage(SchemaStorage.UID)
                                 .setType(SchemaType.INLINE)
                                 .setValue("\"string\"")
                                 .build())
                            .build()))
                    .build()))
            .build()))
        .build();
    try {
      KijiTableLayout.createUpdatedLayout(desc2, mRefLayout);
      Assert.fail("Invalid layout with implicitly renamed column did not fail");
    } catch (InvalidLayoutException ile) {
      // Expected failure
    }
  }

  @Test
  public void testRenameFamily() throws Exception {
    final TableLayoutDesc desc2 = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(mFormat)
        .setVersion(TABLE_LAYOUT_VERSION)
        .setLocalityGroups(Lists.newArrayList(
            LocalityGroupDesc.newBuilder()
            .setName("locality_group_name")
            .setInMemory(false)
            .setTtlSeconds(84600)
            .setMaxVersions(1)
            .setCompressionType(CompressionType.GZ)
            .setFamilies(Lists.newArrayList(
                FamilyDesc.newBuilder()
                    .setName("renamed_family_name")
                    .setRenamedFrom("family_name")
                    .setColumns(Lists.newArrayList(
                        ColumnDesc.newBuilder()
                            .setName("column_name")
                            .setColumnSchema(CellSchema.newBuilder()
                                 .setStorage(SchemaStorage.UID)
                                 .setType(SchemaType.INLINE)
                                 .setValue("\"string\"")
                                 .build())
                            .build()))
                    .build()))
            .build()))
        .build();
    final KijiTableLayout layout2 = KijiTableLayout.createUpdatedLayout(desc2, mRefLayout);
    assertEquals(1, layout2.getFamilies().size());
    assertNull(layout2.getFamilyMap().get("family_name"));
    final FamilyLayout fLayout2 = layout2.getFamilyMap().get("renamed_family_name");
    assertEquals(1, fLayout2.getColumns().size());
    assertEquals(1, fLayout2.getColumnMap().size());
    final ColumnLayout cLayout2 = fLayout2.getColumnMap().get("column_name");
    assertNotNull(cLayout2);
  }

  @Test
  public void testIllegalImplicitFamilyRenaming() throws Exception {
    final TableLayoutDesc desc2 = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(mFormat)
        .setVersion(TABLE_LAYOUT_VERSION)
        .setLocalityGroups(Lists.newArrayList(
            LocalityGroupDesc.newBuilder()
            .setName("locality_group_name")
            .setInMemory(false)
            .setTtlSeconds(84600)
            .setMaxVersions(1)
            .setCompressionType(CompressionType.GZ)
            .setFamilies(Lists.newArrayList(
                FamilyDesc.newBuilder()
                    .setName("renamed_family_name")
                    .setColumns(Lists.newArrayList(
                        ColumnDesc.newBuilder()
                            .setName("column_name")
                            .setColumnSchema(CellSchema.newBuilder()
                                 .setStorage(SchemaStorage.UID)
                                 .setType(SchemaType.INLINE)
                                 .setValue("\"string\"")
                                 .build())
                            .build()))
                    .build()))
            .build()))
        .build();
    try {
      KijiTableLayout.createUpdatedLayout(desc2, mRefLayout);
      Assert.fail("Invalid layout with implicitly renamed family did not fail");
    } catch (InvalidLayoutException ile) {
      // Expected failure
    }
  }

  @Test
  public void testRenameLocalityGroup() throws Exception {
    final TableLayoutDesc desc2 = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(mFormat)
        .setVersion(TABLE_LAYOUT_VERSION)
        .setLocalityGroups(Lists.newArrayList(
            LocalityGroupDesc.newBuilder()
            .setName("renamed_locality_group_name")
            .setRenamedFrom("locality_group_name")
            .setInMemory(false)
            .setTtlSeconds(84600)
            .setMaxVersions(1)
            .setCompressionType(CompressionType.GZ)
            .setFamilies(Lists.newArrayList(
                FamilyDesc.newBuilder()
                    .setName("family_name")
                    .setColumns(Lists.newArrayList(
                        ColumnDesc.newBuilder()
                            .setName("column_name")
                            .setColumnSchema(CellSchema.newBuilder()
                                 .setStorage(SchemaStorage.UID)
                                 .setType(SchemaType.INLINE)
                                 .setValue("\"string\"")
                                 .build())
                            .build()))
                    .build()))
            .build()))
        .build();
    final KijiTableLayout layout2 = KijiTableLayout.createUpdatedLayout(desc2, mRefLayout);
    assertEquals(1, layout2.getLocalityGroups().size());
    assertEquals(1, layout2.getLocalityGroupMap().size());
    final LocalityGroupLayout lgLayout2 =
        layout2.getLocalityGroupMap().get("renamed_locality_group_name");
    assertNotNull(lgLayout2);

    assertEquals(1, lgLayout2.getFamilyMap().size());
    assertNotNull(lgLayout2.getFamilyMap().get("family_name"));
  }

  @Test
  public void testIllegalImplicitLocalityGroupRenaming() throws Exception {
    final TableLayoutDesc desc2 = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(mFormat)
        .setVersion(TABLE_LAYOUT_VERSION)
        .setLocalityGroups(Lists.newArrayList(
            LocalityGroupDesc.newBuilder()
            .setName("renamed_locality_group_name")
            .setInMemory(false)
            .setTtlSeconds(84600)
            .setMaxVersions(1)
            .setCompressionType(CompressionType.GZ)
            .setFamilies(Lists.newArrayList(
                FamilyDesc.newBuilder()
                    .setName("family_name")
                    .setColumns(Lists.newArrayList(
                        ColumnDesc.newBuilder()
                            .setName("column_name")
                            .setColumnSchema(CellSchema.newBuilder()
                                 .setStorage(SchemaStorage.UID)
                                 .setType(SchemaType.INLINE)
                                 .setValue("\"string\"")
                                 .build())
                            .build()))
                    .build()))
            .build()))
        .build();
    try {
      KijiTableLayout.createUpdatedLayout(desc2, mRefLayout);
      Assert.fail("Invalid layout with implicitly renamed locality group did not fail");
    } catch (InvalidLayoutException ile) {
      // Expected failure
    }
  }
}
