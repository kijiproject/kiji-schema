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

import java.util.ArrayList;

import com.google.common.collect.Lists;

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

import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;

/** Tests for KijiTableLayout. */
public class TestKijiTableLayoutAddColumn {
  private static final Logger LOG = LoggerFactory.getLogger(TestKijiTableLayoutAddColumn.class);

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

  /** Reference layout with a single column: "family_name:column_name". */
  private TableLayoutDesc getLayoutV1Desc() {
    RowKeyFormat2 format = makeHashPrefixedRowKeyFormat();
    return TableLayoutDesc.newBuilder()
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
  }

  /** Second layout version adds column: "family_name:second_column_name". */
  private TableLayoutDesc getLayoutV2Desc() {
    final TableLayoutDesc.Builder builder = TableLayoutDesc.newBuilder(getLayoutV1Desc());
    builder
        .getLocalityGroups().get(0)
        .getFamilies().get(0)
        .getColumns().add(ColumnDesc.newBuilder()
            .setName("second_column_name")
            .setColumnSchema(CellSchema.newBuilder()
                .setStorage(SchemaStorage.HASH)
                .setType(SchemaType.INLINE)
                .setValue("\"int\"")
                .build())
            .build());
    return builder.build();
  }

  /** Third layout removes column: "family_name:column_name". */
  private TableLayoutDesc getLayoutV3Desc() {
    final TableLayoutDesc.Builder builder = TableLayoutDesc.newBuilder(getLayoutV2Desc());
    builder
        .getLocalityGroups().get(0)
        .getFamilies().get(0)
        .getColumns().get(0)
            .setDelete(true);
    return builder.build();
  }

  /** Fourth layout version adds column: "family_name:third_column_name". */
  private TableLayoutDesc getLayoutV4Desc() {
    final TableLayoutDesc.Builder builder = TableLayoutDesc.newBuilder(getLayoutV3Desc());
    builder
        .getLocalityGroups().get(0)
        .getFamilies().get(0)
        .getColumns().remove(0);
    builder
        .getLocalityGroups().get(0)
        .getFamilies().get(0)
        .getColumns().add(ColumnDesc.newBuilder()
            .setName("third_column_name")
            .setColumnSchema(CellSchema.newBuilder()
                .setStorage(SchemaStorage.HASH)
                .setType(SchemaType.INLINE)
                .setValue("\"float\"")
                .build())
            .build());
    return builder.build();
  }

  @Test
  public void testAddColumn() throws Exception {
    final KijiTableLayout layout1 = KijiTableLayout.newLayout(getLayoutV1Desc());
    {
      final FamilyLayout fLayout1 = layout1.getFamilyMap().get("family_name");
      assertEquals(1, fLayout1.getId().getId());
      final ColumnLayout c1Layout1 = fLayout1.getColumnMap().get("column_name");
      assertEquals(1, c1Layout1.getId().getId());
    }

    final KijiTableLayout layout2 = KijiTableLayout.createUpdatedLayout(getLayoutV2Desc(), layout1);
    {
      final FamilyLayout fLayout2 = layout2.getFamilyMap().get("family_name");
      assertEquals(1, fLayout2.getId().getId());
      final ColumnLayout c1Layout2 = fLayout2.getColumnMap().get("column_name");
      assertEquals(1, c1Layout2.getId().getId());
      final ColumnLayout c2Layout2 = fLayout2.getColumnMap().get("second_column_name");
      assertEquals(2, c2Layout2.getId().getId());
    }

    final KijiTableLayout layout3 = KijiTableLayout.createUpdatedLayout(getLayoutV3Desc(), layout2);
    {
      final FamilyLayout fLayout3 = layout3.getFamilyMap().get("family_name");
      assertEquals(1, fLayout3.getId().getId());
      assertEquals(1, fLayout3.getColumnMap().size());
      final ColumnLayout c2Layout3 = fLayout3.getColumnMap().get("second_column_name");
      assertEquals(2, c2Layout3.getId().getId());
    }

    final KijiTableLayout layout4 = KijiTableLayout.createUpdatedLayout(getLayoutV4Desc(), layout3);
    {
      final FamilyLayout fLayout4 = layout4.getFamilyMap().get("family_name");
      assertEquals(1, fLayout4.getId().getId());
      assertEquals(2, fLayout4.getColumnMap().size());
      final ColumnLayout c2Layout4 = fLayout4.getColumnMap().get("second_column_name");
      assertEquals(2, c2Layout4.getId().getId());
      final ColumnLayout c3Layout4 = fLayout4.getColumnMap().get("third_column_name");
      assertEquals(1, c3Layout4.getId().getId());
    }
  }
}
