/**
 * (c) Copyright 2013 WibiData, Inc.
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

import java.io.IOException;
import java.util.Map;

import org.junit.Test;

/** Tests for the all_types_table layout contained in all-types-schema. */
public class TestAllTypesSchema {
  @Test
  public void testAllTypesSchema() throws IOException {
    // This test to ensures some guarantees on the all types schema so that it can be used across
    // client packages of KijiSchema for tests to ensure that all Avro types behave as expected.
    // This serves as a warning that edits to this schema are done with care.
    KijiTableLayout layout = KijiTableLayout.newLayout(
      KijiTableLayouts.getLayout("org/kiji/schema/layout/all-types-schema.json"));
    Map<String, KijiTableLayout.LocalityGroupLayout.FamilyLayout> familyMap =
        layout.getFamilyMap();
    // 8 primitives maps + 1 for the primitive family + 1 for the complex family
    assertEquals(10, familyMap.size());

    // Ensure that columns exist for all Avro primitive types
    Map<String, KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout> primitiveColumnMap =
        familyMap.get("primitive").getColumnMap();
    assertNotNull(primitiveColumnMap);
    assertEquals(8, primitiveColumnMap.size());
    assertNotNull(primitiveColumnMap.get("boolean_column"));
    assertNotNull(primitiveColumnMap.get("int_column"));
    assertNotNull(primitiveColumnMap.get("long_column"));
    assertNotNull(primitiveColumnMap.get("float_column"));
    assertNotNull(primitiveColumnMap.get("double_column"));
    assertNotNull(primitiveColumnMap.get("bytes_column"));
    assertNotNull(primitiveColumnMap.get("string_column"));
    assertNotNull(primitiveColumnMap.get("counter_column"));

    // Ensure that the map type families exist for all Avro primitive types
    assertNotNull(familyMap.get("boolean_map"));
    assertNotNull(familyMap.get("int_map"));
    assertNotNull(familyMap.get("long_map"));
    assertNotNull(familyMap.get("float_map"));
    assertNotNull(familyMap.get("double_map"));
    assertNotNull(familyMap.get("bytes_map"));
    assertNotNull(familyMap.get("string_map"));
    assertNotNull(familyMap.get("counter_map"));

    // Ensure that the example complex type columns all exist
    Map<String, KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout> complexColumnMap =
        familyMap.get("complex").getColumnMap();
    assertNotNull(complexColumnMap);
    assertEquals(8, complexColumnMap.size());
    assertNotNull(complexColumnMap.get("record_column"));
    assertNotNull(complexColumnMap.get("enum_column"));
    assertNotNull(complexColumnMap.get("array_column"));
    assertNotNull(complexColumnMap.get("map_column"));
    assertNotNull(complexColumnMap.get("union_column"));
    assertNotNull(complexColumnMap.get("fixed_column"));
    assertNotNull(complexColumnMap.get("recursive_record_column"));
    assertNotNull(complexColumnMap.get("class_column"));
  }
}
