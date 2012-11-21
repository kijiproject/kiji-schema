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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiColumnName;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.ColumnDesc;
import org.kiji.schema.avro.ComponentType;
import org.kiji.schema.avro.CompressionType;
import org.kiji.schema.avro.FamilyDesc;
import org.kiji.schema.avro.HashSpec;
import org.kiji.schema.avro.LocalityGroupDesc;
import org.kiji.schema.avro.RowKeyComponent;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.avro.SchemaStorage;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.util.ToJson;

/** Tests for KijiTableLayout. */
public class TestKijiTableLayout {
  private static final Logger LOG = LoggerFactory.getLogger(TestKijiTableLayout.class);

  private static final String TABLE_LAYOUT_VERSION = "kiji-1.1";

  private RowKeyFormat makeHashedRKF1() {
    return RowKeyFormat.newBuilder()
        .setEncoding(RowKeyEncoding.HASH)
        .build();
  }

  private RowKeyFormat makeHashPrefixedRKF1() {
    return RowKeyFormat.newBuilder()
        .setEncoding(RowKeyEncoding.HASH_PREFIX)
        .build();
  }

  private RowKeyFormat makeRawRKF1() {
    return RowKeyFormat.newBuilder().setEncoding(RowKeyEncoding.RAW).build();
  }

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

  // Invalid row key format
  private RowKeyFormat2 noComponentsRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setComponents(components)
        .build();

    return format;
  }

  // Invalid row key format
  private RowKeyFormat2 badNullableIndexRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("NAME").setType(ComponentType.STRING).build());

    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setNullableStartIndex(-1)
        .setComponents(components)
        .build();

    return format;
  }

  // Invalid row key format.
  private RowKeyFormat2 zeroNullableIndexRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("NAME").setType(ComponentType.STRING).build());

    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setNullableStartIndex(0)
        .setComponents(components)
        .build();

    return format;
  }

  // Invalid row key format.
  private RowKeyFormat2 tooHighNullableIndexRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("NAME").setType(ComponentType.STRING).build());

    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setNullableStartIndex(components.size() + 1)
        .setComponents(components)
        .build();

    return format;
  }

  // Invalid row key format.
  private RowKeyFormat2 badRangeScanIndexRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("NAME").setType(ComponentType.STRING).build());

    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setRangeScanStartIndex(0)
        .setComponents(components)
        .build();

    return format;
  }

  // Invalid row key format.
  private RowKeyFormat2 tooHighRangeScanIndexRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("NAME").setType(ComponentType.STRING).build());

    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setRangeScanStartIndex(components.size() + 1)
        .setComponents(components)
        .build();

    return format;
  }

  // Invalid row key format.
  private RowKeyFormat2 badCompNameRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("0").setType(ComponentType.STRING).build());

    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setRangeScanStartIndex(0)
        .setComponents(components)
        .build();

    return format;
  }

  // Invalid row key format.
  private RowKeyFormat2 badHashSizeRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("NAME").setType(ComponentType.STRING).build());

    HashSpec hs = HashSpec.newBuilder()
        .setHashSize(20).build();

    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(hs)
        .setRangeScanStartIndex(0)
        .setComponents(components)
        .build();

    return format;
  }

  // Invalid row key format.
  private RowKeyFormat2 repeatedNamesRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("NAME").setType(ComponentType.STRING).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("NAME").setType(ComponentType.STRING).build());

    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setComponents(components)
        .build();

    return format;
  }

  // Invalid row key format.
  private RowKeyFormat2 emptyCompNameRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("").setType(ComponentType.STRING).build());

    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setRangeScanStartIndex(0)
        .setComponents(components)
        .build();

    return format;
  }

  /** Tests for a empty layout with no reference layout. */
  @Test
  public void testEmptyLayoutWithNoReference() throws Exception {
    RowKeyFormat2 format = makeHashPrefixedRowKeyFormat();
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(format)
        .setVersion(TABLE_LAYOUT_VERSION)
        .build();
    final KijiTableLayout layout = KijiTableLayout.newLayout(desc);
    assertEquals("1", layout.getDesc().getLayoutId());
    assertTrue(layout.getLocalityGroups().isEmpty());
    assertTrue(layout.getLocalityGroupMap().isEmpty());
    assertTrue(layout.getFamilies().isEmpty());
    assertTrue(layout.getFamilyMap().isEmpty());
  }

  /** Tests layout IDs. */
  @Test
  public void testLayoutIDs() throws Exception {
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(makeRawRKF1())
        .setVersion("kiji-6.0")
        .build();
    final KijiTableLayout layout = KijiTableLayout.newLayout(desc);
    assertEquals("1", layout.getDesc().getLayoutId());

    final TableLayoutDesc descV2 = TableLayoutDesc.newBuilder(desc)
        .setReferenceLayout("1")
        .build();
    final KijiTableLayout layoutV2 = KijiTableLayout.createUpdatedLayout(descV2, layout);
    assertEquals("2", layoutV2.getDesc().getLayoutId());
  }

  /** Tests for a layout with a single locality group, and with no reference layout. */
  @Test
  public void testLayoutWithNoReference() throws Exception {
    RowKeyFormat2 format = makeHashPrefixedRowKeyFormat();
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(format)
        .setVersion(TABLE_LAYOUT_VERSION)
        .setLocalityGroups(Lists.newArrayList(
            LocalityGroupDesc.newBuilder()
                .setName("locality_group_name")
                .setInMemory(false)
                .setTtlSeconds(84600)
                .setMaxVersions(1)
                .setCompressionType(CompressionType.GZ)
                .build()))
        .build();
    final KijiTableLayout layout = KijiTableLayout.newLayout(desc);
    final KijiTableLayout.LocalityGroupLayout lgLayout =
        layout.getLocalityGroupMap().get("locality_group_name");
    assertNotNull(lgLayout);
    assertEquals(1, layout.getLocalityGroups().size());
    assertEquals(lgLayout, layout.getLocalityGroups().iterator().next());
    assertTrue(lgLayout.getName().equals("locality_group_name"));
    assertTrue(lgLayout.getFamilies().isEmpty());
    assertTrue(layout.getFamilies().isEmpty());
    assertTrue(layout.getFamilyMap().isEmpty());
  }

  /** Tests for a layout with one map family, and with no reference layout. */
  @Test
  public void testMapFamilyLayoutWithNoReference() throws Exception {
    RowKeyFormat2 format = makeHashPrefixedRowKeyFormat();
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(format)
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
                        .setMapSchema(CellSchema.newBuilder()
                            .setType(SchemaType.INLINE)
                            .setStorage(SchemaStorage.HASH)
                            .setValue("\"int\"")
                            .build())
                        .build()))
                .build()))
        .build();
    final KijiTableLayout layout = KijiTableLayout.newLayout(desc);
    final KijiTableLayout.LocalityGroupLayout.FamilyLayout fLayout =
        layout.getFamilyMap().get("family_name");
    assertNotNull(fLayout);
    assertTrue(fLayout.isMapType());
    assertEquals(fLayout, layout.getFamilies().iterator().next());

    assertEquals(SchemaStorage.HASH, layout.getCellFormat(new KijiColumnName("family_name")));
    assertEquals(Schema.Type.INT, layout.getSchema(new KijiColumnName("family_name")).getType());
  }

  /** Tests for a layout with one column, and with no reference layout. */
  @Test
  public void testGroupFamilyLayoutWithNoReference() throws Exception {
    RowKeyFormat2 format = makeHashPrefixedRowKeyFormat();
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(format)
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
    final KijiTableLayout layout = KijiTableLayout.newLayout(desc);
    final KijiTableLayout.LocalityGroupLayout.FamilyLayout fLayout =
        layout.getFamilyMap().get("family_name");
    assertNotNull(fLayout);
    assertTrue(fLayout.isGroupType());
    final KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout cLayout =
        fLayout.getColumnMap().get("column_name");
    assertNotNull(cLayout);
    assertEquals(cLayout, fLayout.getColumns().iterator().next());

    assertEquals(SchemaStorage.UID,
        layout.getCellFormat(new KijiColumnName("family_name:column_name")));
    assertEquals(Schema.Type.STRING,
        layout.getSchema(new KijiColumnName("family_name:column_name")).getType());
  }

  /** Tests for column removal. */
  @Test
  public void testDeleteColumn() throws Exception {
    RowKeyFormat2 format = makeHashPrefixedRowKeyFormat();
    // Reference layout with a single column : "family_name:column_name"
    final TableLayoutDesc refDesc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(format)
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
    final KijiTableLayout refLayout = KijiTableLayout.newLayout(refDesc);

    {
      // Target layout deleting the column
      final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
          .setName("table_name")
          .setKeysFormat(format)
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
                              .setDelete(true)
                              .build()))
                      .build()))
              .build()))
          .build();
      final KijiTableLayout layout = KijiTableLayout.createUpdatedLayout(desc, refLayout);
      final KijiTableLayout.LocalityGroupLayout.FamilyLayout fLayout =
          layout.getFamilyMap().get("family_name");
      assertNotNull(fLayout);
      assertTrue(fLayout.getColumns().isEmpty());
      assertTrue(fLayout.getColumnMap().isEmpty());
    }

    {
      // Target layout with an invalid column deletion
      final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
          .setName("table_name")
          .setKeysFormat(format)
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
                      .build()))
              .build()))
          .build();
      try {
        KijiTableLayout.createUpdatedLayout(desc, refLayout);
        Assert.fail("Layout update with missing column did not fail.");
      } catch (InvalidLayoutException ile) {
        // Exception is expected!
      }
    }

    {
      // Target layout with an invalid column rename
      final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
          .setName("table_name")
          .setKeysFormat(format)
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
                              .setName("column-renamed")
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
        KijiTableLayout.createUpdatedLayout(desc, refLayout);
        Assert.fail("Invalid layout update with bad column renaming did not throw.");
      } catch (InvalidLayoutException ile) {
        // Expected
      }
    }
  }

  @Test
  public void testNameAliases() throws Exception {
    RowKeyFormat2 format = makeHashPrefixedRowKeyFormat();
    // Reference layout with a single column: "family_name:column_name"
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(format)
        .setVersion(TABLE_LAYOUT_VERSION)
        .setLocalityGroups(Lists.newArrayList(
            LocalityGroupDesc.newBuilder()
            .setName("locality_group_name")
            .setAliases(Lists.newArrayList("locality_group_alias1", "locality_group_alias2"))
            .setInMemory(false)
            .setTtlSeconds(84600)
            .setMaxVersions(1)
            .setCompressionType(CompressionType.GZ)
            .setFamilies(Lists.newArrayList(
                FamilyDesc.newBuilder()
                    .setName("family_name")
                    .setAliases(Lists.newArrayList("family_alias1", "family-alias2"))
                    .setColumns(Lists.newArrayList(
                        ColumnDesc.newBuilder()
                            .setName("column_name")
                            .setAliases(Lists.newArrayList("column-alias1", "column-alias2"))
                            .setColumnSchema(CellSchema.newBuilder()
                                 .setStorage(SchemaStorage.UID)
                                 .setType(SchemaType.INLINE)
                                 .setValue("\"string\"")
                                 .build())
                            .build()))
                    .build()))
            .build()))
        .build();
    final KijiTableLayout layout = KijiTableLayout.newLayout(desc);
    assertEquals(
        layout.getLocalityGroupMap().get("locality_group_name"),
        layout.getLocalityGroupMap().get("locality_group_alias1"));
    assertEquals(
        layout.getLocalityGroupMap().get("locality_group_name"),
        layout.getLocalityGroupMap().get("locality_group_alias2"));

    assertEquals(
        layout.getFamilyMap().get("family_name"),
        layout.getFamilyMap().get("family_alias1"));
    assertEquals(
        layout.getFamilyMap().get("family_name"),
        layout.getFamilyMap().get("family-alias2"));

    final KijiTableLayout.LocalityGroupLayout.FamilyLayout fLayout =
        layout.getFamilyMap().get("family_name");
    assertEquals(
        fLayout.getColumnMap().get("column_name"), fLayout.getColumnMap().get("column-alias1"));
    assertEquals(
        fLayout.getColumnMap().get("column_name"), fLayout.getColumnMap().get("column-alias2"));
  }

  /** Tests JSON serialization. */
  @Test
  public void testFromJsonAndToJson() throws Exception {
    final KijiTableLayout layout =
        KijiTableLayout.createFromEffectiveJsonResource(
            "/org/kiji/schema/layout/full-featured-layout.json");

    final String effectiveLayout = ToJson.toJsonString(layout.getDesc());
    LOG.info(effectiveLayout);
    final KijiTableLayout reparsed =
        KijiTableLayout.createFromEffectiveJson(
            new ByteArrayInputStream(effectiveLayout.getBytes()));
    final String reserialized = ToJson.toJsonString(reparsed.getDesc());
    assertEquals(effectiveLayout, reserialized);
  }

  /** Tests the initial assignment of IDs to locality groups, families and columns. */
  @Test
  public void testIdAssignmentWithNoReference() throws Exception {
    RowKeyFormat2 format = makeHashPrefixedRowKeyFormat();
    // Reference layout with a single column: "family_name:column_name"
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(format)
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
                                .build(),
                                ColumnDesc.newBuilder()
                                .setName("column2_name")
                                .setColumnSchema(CellSchema.newBuilder()
                                     .setStorage(SchemaStorage.UID)
                                     .setType(SchemaType.INLINE)
                                     .setValue("\"bytes\"")
                                     .build())
                                .build()
                        ))
                        .build(),
                    FamilyDesc.newBuilder()
                        .setName("family2_name")
                        .setMapSchema(CellSchema.newBuilder()
                            .setStorage(SchemaStorage.FINAL)
                            .setType(SchemaType.COUNTER)
                            .build())
                        .build()
                ))
                .build(),
            LocalityGroupDesc.newBuilder()
                .setName("locality_group2_name")
                .setInMemory(false)
                .setTtlSeconds(84600)
                .setMaxVersions(1)
                .setCompressionType(CompressionType.GZ)
                .setFamilies(Lists.newArrayList(
                    FamilyDesc.newBuilder()
                        .setName("family3_name")
                        .setMapSchema(CellSchema.newBuilder()
                            .setStorage(SchemaStorage.FINAL)
                            .setType(SchemaType.COUNTER)
                            .build())
                        .build()
                ))
                .build()
        ))
        .build();

    final KijiTableLayout layout = KijiTableLayout.newLayout(desc);
    final KijiTableLayout.LocalityGroupLayout lgLayout =
        layout.getLocalityGroupMap().get("locality_group_name");
    assertEquals(1, lgLayout.getId().getId());
    final KijiTableLayout.LocalityGroupLayout.FamilyLayout fLayout =
        lgLayout.getFamilyMap().get("family_name");
    assertEquals(1, fLayout.getId().getId());
    final KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout cLayout =
        fLayout.getColumnMap().get("column_name");
    assertEquals(1, cLayout.getId().getId());
    final KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout c2Layout =
        fLayout.getColumnMap().get("column2_name");
    assertEquals(2, c2Layout.getId().getId());

    final KijiTableLayout.LocalityGroupLayout.FamilyLayout f2Layout =
        lgLayout.getFamilyMap().get("family2_name");
    assertEquals(2, f2Layout.getId().getId());

    final KijiTableLayout.LocalityGroupLayout lg2Layout =
        layout.getLocalityGroupMap().get("locality_group2_name");
    assertEquals(2, lg2Layout.getId().getId());
  }

  @Test
  public void testDuplicateFamilyName() throws Exception {
    RowKeyFormat2 format = makeHashPrefixedRowKeyFormat();
    // Reference layout with a single column: "family_name:column_name"
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(format)
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
                                .build(),
                                ColumnDesc.newBuilder()
                                .setName("column2_name")
                                .setColumnSchema(CellSchema.newBuilder()
                                     .setStorage(SchemaStorage.UID)
                                     .setType(SchemaType.INLINE)
                                     .setValue("\"bytes\"")
                                     .build())
                                .build()
                        ))
                        .build(),
                    FamilyDesc.newBuilder()
                        .setName("family2_name")
                        .setMapSchema(CellSchema.newBuilder()
                            .setStorage(SchemaStorage.FINAL)
                            .setType(SchemaType.COUNTER)
                            .build())
                        .build()
                ))
                .build(),
            LocalityGroupDesc.newBuilder()
                .setName("locality_group2_name")
                .setInMemory(false)
                .setTtlSeconds(84600)
                .setMaxVersions(1)
                .setCompressionType(CompressionType.GZ)
                .setFamilies(Lists.newArrayList(
                    FamilyDesc.newBuilder()
                        .setName("family_name")
                        .setMapSchema(CellSchema.newBuilder()
                            .setStorage(SchemaStorage.FINAL)
                            .setType(SchemaType.COUNTER)
                            .build())
                        .build()
                ))
                .build()
        ))
        .build();
    try {
      KijiTableLayout.newLayout(desc);
      fail("Invalid layout with duplicate family name did not throw.");
    } catch (InvalidLayoutException ile) {
      // Expected:
      LOG.info("Expected duplicate family error: " + ile);
      assertTrue(ile.getMessage().contains("duplicate family name 'family_name'"));
    }
  }

  @Test
  public void testDuplicateQualifierName() throws Exception {
    RowKeyFormat2 format = makeHashPrefixedRowKeyFormat();
    // Reference layout with a single column: "family_name:column_name"
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(format)
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
                                .build(),
                                ColumnDesc.newBuilder()
                                .setName("column_name")
                                .setColumnSchema(CellSchema.newBuilder()
                                     .setStorage(SchemaStorage.UID)
                                     .setType(SchemaType.INLINE)
                                     .setValue("\"bytes\"")
                                     .build())
                                .build()))
                        .build()))
                .build()))
        .build();
    try {
      KijiTableLayout.newLayout(desc);
      fail("Invalid layout with duplicate qualifier name did not throw.");
    } catch (InvalidLayoutException ile) {
      // Expected:
      LOG.info("Expected duplicate qualifier error: " + ile);
      assertTrue(ile.toString().contains("duplicate column qualifier 'column_name'"));
    }
  }

  /** Test for a family with both group and map type. */
  @Test
  public void testInvalidGroupAndMapFamily() throws Exception {
    RowKeyFormat2 format = makeHashPrefixedRowKeyFormat();
    // Reference layout with a single column: "family_name:column_name"
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(format)
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
                        .setMapSchema(CellSchema.newBuilder()
                            .setStorage(SchemaStorage.HASH)
                            .setType(SchemaType.COUNTER)
                            .build())
                        .build()))
                .build()))
        .build();
    try {
      KijiTableLayout.newLayout(desc);
      fail("Invalid family with both map-type and columnsdid not throw.");
    } catch (InvalidLayoutException ile) {
      // Expected
      LOG.info("Expected invalid family: " + ile);
      assertTrue(
          ile.toString().contains("Invalid family 'family_name' "
              + "with both map-type and columns"));
    }
  }

  @Test
  public void testInvalidLocalityGroupTTLSeconds() throws Exception {
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(makeRawRKF1())
        .setVersion(TABLE_LAYOUT_VERSION)
        .setLocalityGroups(Lists.newArrayList(LocalityGroupDesc.newBuilder()
            .setName("default")
            .setCompressionType(CompressionType.NONE)
            .setTtlSeconds(-1)
            .setMaxVersions(1)
            .setInMemory(false)
            .build()))
        .build();
    try {
      KijiTableLayout.newLayout(desc);
      fail("Invalid locality group with negative TTL seconds did not throw");
    } catch (InvalidLayoutException ile) {
      assertTrue(ile.getMessage().contains("Invalid TTL seconds for locality group"));
    }
  }

  @Test
  public void testInvalidLocalityGroupMaxVersions() throws Exception {
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(makeHashedRKF1())
        .setVersion(TABLE_LAYOUT_VERSION)
        .setLocalityGroups(Lists.newArrayList(LocalityGroupDesc.newBuilder()
            .setName("default")
            .setCompressionType(CompressionType.NONE)
            .setTtlSeconds(1)
            .setMaxVersions(-1)
            .setInMemory(false)
            .build()))
        .build();
    try {
      KijiTableLayout.newLayout(desc);
      fail("Invalid locality group with negative max versions did not throw");
    } catch (InvalidLayoutException ile) {
      assertTrue(ile.getMessage().contains("Invalid max versions for locality group"));
    }
  }

  @Test
  public void testFinalColumnSchema() throws Exception {
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(makeHashPrefixedRKF1())
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
                                 .setStorage(SchemaStorage.FINAL)
                                 .setType(SchemaType.INLINE)
                                 .setValue("\"int\"")
                                 .build())
                            .build()))
                    .build()))
            .build()))
        .build();
    final KijiTableLayout layout = KijiTableLayout.newLayout(desc);
    assertEquals(
        SchemaStorage.FINAL,
        layout.getCellSchema(new KijiColumnName("family_name", "column_name")).getStorage());
  }

  @Test
  public void testFinalColumnSchemaClassInvalid() throws Exception {
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(makeHashPrefixedRKF1())
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
                                 .setStorage(SchemaStorage.FINAL)
                                 .setType(SchemaType.CLASS)
                                 .setValue("dummy.Class")
                                 .build())
                            .build()))
                    .build()))
            .build()))
        .build();
    try {
      KijiTableLayout.newLayout(desc);
      fail("Final column schema must be inline");
    } catch (InvalidLayoutException ile) {
      assertTrue(ile.getMessage().contains("Invalid final column schema"));
    }
  }

  @Test
  public void testFinalColumnSchemaCounter() throws Exception {
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(makeHashPrefixedRKF1())
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
                                 .setStorage(SchemaStorage.FINAL)
                                 .setType(SchemaType.COUNTER)
                                 .build())
                            .build()))
                    .build()))
            .build()))
        .build();
    final KijiTableLayout layout = KijiTableLayout.newLayout(desc);
    assertEquals(
        SchemaStorage.FINAL,
        layout.getCellSchema(new KijiColumnName("family_name", "column_name")).getStorage());
  }

  @Test
  public void testKijiTableHashSize() throws Exception {
    // default hash size for RowKeyFormat2 is 16
    assertEquals(16, KijiTableLayout.getHashSize(makeHashPrefixedRowKeyFormat()));
    // default hash size for RowKeyFormat is 0
    assertEquals(0, KijiTableLayout.getHashSize(makeHashPrefixedRKF1()));
  }

  @Test(expected=InvalidLayoutException.class)
  public void testNoComponentsRKF() throws InvalidLayoutException {
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(noComponentsRowKeyFormat())
        .setVersion(TABLE_LAYOUT_VERSION)
        .build();
    final KijiTableLayout ktl = KijiTableLayout.newLayout(desc);
  }

  @Test(expected=InvalidLayoutException.class)
  public void testBadNullableIndexRKF() throws InvalidLayoutException {
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(badNullableIndexRowKeyFormat())
        .setVersion(TABLE_LAYOUT_VERSION)
        .build();
    final KijiTableLayout ktl = KijiTableLayout.newLayout(desc);
  }

  @Test(expected=InvalidLayoutException.class)
  public void badRangeScanIndexRKF() throws InvalidLayoutException {
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(badRangeScanIndexRowKeyFormat())
        .setVersion(TABLE_LAYOUT_VERSION)
        .build();
    final KijiTableLayout ktl = KijiTableLayout.newLayout(desc);
  }

  @Test(expected=InvalidLayoutException.class)
  public void badCompNameRKF() throws InvalidLayoutException {
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(badCompNameRowKeyFormat())
        .setVersion(TABLE_LAYOUT_VERSION)
        .build();
    final KijiTableLayout ktl = KijiTableLayout.newLayout(desc);
  }

  @Test(expected=InvalidLayoutException.class)
  public void badHashSizeRKF() throws InvalidLayoutException {
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(badHashSizeRowKeyFormat())
        .setVersion(TABLE_LAYOUT_VERSION)
        .build();
    final KijiTableLayout ktl = KijiTableLayout.newLayout(desc);
  }

  @Test(expected=InvalidLayoutException.class)
  public void repeatedNamesRKF() throws InvalidLayoutException {
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(repeatedNamesRowKeyFormat())
        .setVersion(TABLE_LAYOUT_VERSION)
        .build();
    final KijiTableLayout ktl = KijiTableLayout.newLayout(desc);
  }

  @Test(expected=InvalidLayoutException.class)
  public void tooHighRangeScanIndexRKF() throws InvalidLayoutException {
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(tooHighRangeScanIndexRowKeyFormat())
        .setVersion(TABLE_LAYOUT_VERSION)
        .build();
    final KijiTableLayout ktl = KijiTableLayout.newLayout(desc);
  }

  @Test(expected=InvalidLayoutException.class)
  public void zeroNullableIndexRKF() throws InvalidLayoutException {
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(zeroNullableIndexRowKeyFormat())
        .setVersion(TABLE_LAYOUT_VERSION)
        .build();
    final KijiTableLayout ktl = KijiTableLayout.newLayout(desc);
  }

  @Test(expected=InvalidLayoutException.class)
  public void tooHighNullableScanIndexRKF() throws InvalidLayoutException {
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(tooHighNullableIndexRowKeyFormat())
        .setVersion(TABLE_LAYOUT_VERSION)
        .build();
    final KijiTableLayout ktl = KijiTableLayout.newLayout(desc);
  }

  @Test(expected=InvalidLayoutException.class)
  public void emptyCompNameRKF() throws InvalidLayoutException {
    final TableLayoutDesc desc = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(emptyCompNameRowKeyFormat())
        .setVersion(TABLE_LAYOUT_VERSION)
        .build();
    final KijiTableLayout ktl = KijiTableLayout.newLayout(desc);
  }
}
