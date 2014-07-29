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
package org.kiji.schema.impl.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.SortedMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.util.Utf8;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiResult.Helpers;
import org.kiji.schema.filter.KijiColumnRangeFilter;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.schema.util.InstanceBuilder.FamilyBuilder;
import org.kiji.schema.util.InstanceBuilder.QualifierBuilder;
import org.kiji.schema.util.InstanceBuilder.RowBuilder;
import org.kiji.schema.util.InstanceBuilder.TableBuilder;

public class TestHBaseKijiResult extends KijiClientTest {

  private static final String PRIMITIVE_FAMILY = "primitive";
  private static final String STRING_MAP_FAMILY = "string_map";

  private static final KijiColumnName PRIMITIVE_STRING =
      KijiColumnName.create(PRIMITIVE_FAMILY, "string_column");
  private static final KijiColumnName PRIMITIVE_DOUBLE =
      KijiColumnName.create(PRIMITIVE_FAMILY, "double_column");
  private static final KijiColumnName PRIMITIVE_LONG =
      KijiColumnName.create(PRIMITIVE_FAMILY, "long_column");
  private static final KijiColumnName STRING_MAP_1 =
      KijiColumnName.create(STRING_MAP_FAMILY, "smap_1");
  private static final KijiColumnName STRING_MAP_2 =
      KijiColumnName.create(STRING_MAP_FAMILY, "smap_2");

  private static final Integer ROW = 1;

  private static final NavigableMap<KijiColumnName, NavigableMap<Long, ?>>
      ROW_DATA = ImmutableSortedMap.<KijiColumnName, NavigableMap<Long, ?>>naturalOrder()
          .put(PRIMITIVE_STRING, ImmutableSortedMap.<Long, Utf8>reverseOrder()
              .put(10L, new Utf8("ten"))
              .put(5L, new Utf8("five"))
              .put(4L, new Utf8("four"))
              .put(3L, new Utf8("three"))
              .put(2L, new Utf8("two"))
              .put(1L, new Utf8("one"))
              .build())
          .put(PRIMITIVE_DOUBLE, ImmutableSortedMap.<Long, Double>reverseOrder()
              .put(10L, 10.0)
              .put(5L, 5.0)
              .put(4L, 4.0)
              .put(3L, 3.0)
              .put(2L, 2.0)
              .put(1L, 1.0)
              .build())
          .put(KijiColumnName.create(PRIMITIVE_FAMILY, "long_column"),
                  ImmutableSortedMap.<Long, Double>reverseOrder()
              .build())
          .put(STRING_MAP_1, ImmutableSortedMap.<Long, Utf8>reverseOrder()
              .put(10L, new Utf8("sm1-ten"))
              .put(5L, new Utf8("sm1-five"))
              .put(4L, new Utf8("sm1-four"))
              .put(3L, new Utf8("sm1-three"))
              .put(2L, new Utf8("sm1-two"))
              .put(1L, new Utf8("sm1-one"))
              .build())
          .put(STRING_MAP_2, ImmutableSortedMap.<Long, Utf8>reverseOrder()
              .put(10L, new Utf8("sm2-ten"))
              .put(5L, new Utf8("sm2-five"))
              .put(4L, new Utf8("sm2-four"))
              .put(3L, new Utf8("sm2-three"))
              .put(2L, new Utf8("sm2-two"))
              .put(1L, new Utf8("sm2-one"))
              .build())
          .build();

  private HBaseKijiTable mTable;
  private HBaseKijiTableReader mReader;

  @Before
  public void setupTestHBaseKijiResult() throws IOException {

    // Deconstruct map of column name => version => value
    // to family => qualifier => versions => value
    NavigableMap<String, NavigableMap<String, NavigableMap<Long, Object>>> families =
        Maps.newTreeMap();
    for (Entry<KijiColumnName, NavigableMap<Long, ?>> columnEntry : ROW_DATA.entrySet()) {
      final KijiColumnName column = columnEntry.getKey();
      NavigableMap<String, NavigableMap<Long, Object>> qualifiers =
          families.get(column.getFamily());
      if (qualifiers == null) {
        qualifiers = Maps.newTreeMap();
        families.put(column.getFamily(), qualifiers);
      }
      NavigableMap<Long, Object> cells = qualifiers.get(column.getQualifier());
      if (cells == null) {
        cells = Maps.newTreeMap();
        qualifiers.put(column.getQualifier(), cells);
      }
      for (Entry<Long, ?> cellEntry : columnEntry.getValue().entrySet()) {
        cells.put(cellEntry.getKey(), cellEntry.getValue());
      }
    }

    final TableBuilder tableBuilder =
        new InstanceBuilder(getKiji())
            .withTable(KijiTableLayouts.getLayout("org/kiji/schema/layout/all-types-schema.json"));

    final RowBuilder rowBuilder = tableBuilder.withRow(ROW);

    for (Entry<String, NavigableMap<String, NavigableMap<Long, Object>>> familyEntry
        : families.entrySet()) {
      final FamilyBuilder familyBuilder = rowBuilder.withFamily(familyEntry.getKey());

      for (Entry<String, NavigableMap<Long, Object>> columnEntry
          : familyEntry.getValue().entrySet()) {
        final QualifierBuilder qualifierBuilder =
            familyBuilder.withQualifier(columnEntry.getKey());

        for (Entry<Long, Object> cellEntry : columnEntry.getValue().entrySet()) {
          qualifierBuilder.withValue(cellEntry.getKey(), cellEntry.getValue());
        }
      }
    }
    tableBuilder.build();


    mTable = HBaseKijiTable.downcast(getKiji().openTable("all_types_table"));
    mReader = (HBaseKijiTableReader) mTable.openTableReader();
  }

  @After
  public void cleanupTestHBaseKijiRowView() throws IOException {
    mTable.release();
    mReader.close();
  }

  public void testViewGet(
      final KijiResult<?> view,
      final Iterable<? extends Entry<Long, ?>> expected
  ) {
    final List<Long> versions = Lists.newArrayList();
    final List<Object> values = Lists.newArrayList();

    for (Entry<Long, ?> cell : expected) {
      versions.add(cell.getKey());
      values.add(cell.getValue());
    }

    assertEquals(versions, ImmutableList.copyOf(Helpers.getVersions(view)));
    assertEquals(values, ImmutableList.copyOf(Helpers.getValues(view)));
  }

  public void testViewGet(
      final KijiDataRequest request,
      final Iterable<? extends Entry<Long, ?>> expected
  ) throws Exception {
    final EntityId eid = mTable.getEntityId(ROW);
    final KijiResult<Object> view = mReader.getResult(eid, request);
    try {
      testViewGet(view, expected);
    } finally {
      view.close();
    }
  }

  @Test
  public void testGetFullyQualifiedColumn() throws Exception {
    for (KijiColumnName column : ImmutableList.of(PRIMITIVE_STRING, STRING_MAP_1)) {
      for (int pageSize : ImmutableList.of(0, 1, 2, 10)) {
        { // Single version | no timerange
          final KijiDataRequest request = KijiDataRequest
              .builder()
              .addColumns(
                  ColumnsDef.create()
                      .withPageSize(pageSize)
                      .add(column.getFamily(), column.getQualifier()))
              .build();

          testViewGet(request, Iterables.limit(ROW_DATA.get(column).entrySet(), 1));
        }

        { // Single version | timerange
          final KijiDataRequest request = KijiDataRequest
              .builder()
              .addColumns(
                  ColumnsDef.create()
                      .withPageSize(pageSize)
                      .add(column.getFamily(), column.getQualifier()))
              .withTimeRange(4, 6)
              .build();

          testViewGet(
              request,
              Iterables.limit(ROW_DATA.get(column).subMap(6L, false, 4L, true).entrySet(), 1));
        }

        { // Multiple versions | no timerange
          final KijiDataRequest request = KijiDataRequest
              .builder()
              .addColumns(
                  ColumnsDef.create()
                      .withPageSize(pageSize)
                      .withMaxVersions(100)
                      .add(column.getFamily(), column.getQualifier()))
              .build();

          testViewGet(
              request,
              ROW_DATA.get(column).entrySet());
        }

        { // Multiple versions | timerange
          final KijiDataRequest request = KijiDataRequest
              .builder()
              .addColumns(
                  ColumnsDef.create()
                      .withPageSize(pageSize)
                      .withMaxVersions(100)
                      .add(column.getFamily(), column.getQualifier()))
              .withTimeRange(4, 6)
              .build();

          testViewGet(
              request,
              ROW_DATA.get(column).subMap(6L, false, 4L, true).entrySet());
        }
      }
    }
  }

  @Test
  public void testGetMultipleFullyQualifiedColumns() throws Exception {
    final KijiColumnName column1 = PRIMITIVE_STRING;
    final KijiColumnName column2 = STRING_MAP_1;

    for (int pageSize : ImmutableList.of(0, 1, 2, 10)) {

      { // Single version | no timerange
        final KijiDataRequest request = KijiDataRequest
            .builder()
            .addColumns(
                ColumnsDef.create()
                    .withPageSize(pageSize)
                    .add(column1.getFamily(), column1.getQualifier())
                    .add(column2.getFamily(), column2.getQualifier()))
            .build();

        final Iterable<? extends Entry<Long, ?>> column1Entries =
            Iterables.limit(ROW_DATA.get(column1).entrySet(), 1);
        final Iterable<? extends Entry<Long, ?>> column2Entries =
            Iterables.limit(ROW_DATA.get(column2).entrySet(), 1);

        testViewGet(request, Iterables.concat(column1Entries, column2Entries));
      }

      { // Single version | timerange
        final KijiDataRequest request = KijiDataRequest
            .builder()
            .addColumns(
                ColumnsDef.create()
                    .withPageSize(pageSize)
                    .add(column1.getFamily(), column1.getQualifier())
                    .add(column2.getFamily(), column2.getQualifier()))
            .withTimeRange(4, 6)
            .build();

        final Iterable<? extends Entry<Long, ?>> column1Entries =
            Iterables.limit(ROW_DATA.get(column1).subMap(6L, false, 4L, true).entrySet(), 1);
        final Iterable<? extends Entry<Long, ?>> column2Entries =
            Iterables.limit(ROW_DATA.get(column2).subMap(6L, false, 4L, true).entrySet(), 1);

        testViewGet(request, Iterables.concat(column1Entries, column2Entries));
      }

      { // Multiple versions | no timerange
        final KijiDataRequest request = KijiDataRequest
            .builder()
            .addColumns(
                ColumnsDef.create()
                    .withPageSize(pageSize)
                    .withMaxVersions(100)
                    .add(column1.getFamily(), column1.getQualifier())
                    .add(column2.getFamily(), column2.getQualifier()))
            .build();

        final Iterable<? extends Entry<Long, ?>> column1Entries = ROW_DATA.get(column1).entrySet();
        final Iterable<? extends Entry<Long, ?>> column2Entries = ROW_DATA.get(column2).entrySet();

        testViewGet(request, Iterables.concat(column1Entries, column2Entries));
      }

      { // Multiple versions | timerange
        final KijiDataRequest request = KijiDataRequest
            .builder()
            .addColumns(
                ColumnsDef.create()
                    .withPageSize(pageSize)
                    .withMaxVersions(100)
                    .add(column1.getFamily(), column1.getQualifier())
                    .add(column2.getFamily(), column2.getQualifier()))
            .withTimeRange(4, 6)
            .build();

        final Iterable<? extends Entry<Long, ?>> column1Entries =
            ROW_DATA.get(column1).subMap(6L, false, 4L, true).entrySet();
        final Iterable<? extends Entry<Long, ?>> column2Entries =
            ROW_DATA.get(column2).subMap(6L, false, 4L, true).entrySet();

        testViewGet(request, Iterables.concat(column1Entries, column2Entries));
      }

      { // Mixed versions | timerange
        final KijiDataRequest request = KijiDataRequest
            .builder()
            .addColumns(
                ColumnsDef.create()
                    .withPageSize(pageSize)
                    .withMaxVersions(100)
                    .add(column1.getFamily(), column1.getQualifier()))
            .addColumns(
                ColumnsDef.create()
                    .withPageSize(pageSize)
                    .withMaxVersions(1)
                    .add(column2.getFamily(), column2.getQualifier()))
            .withTimeRange(4, 6)
            .build();

        final Iterable<? extends Entry<Long, ?>> column1Entries =
            ROW_DATA.get(column1).subMap(6L, false, 4L, true).entrySet();
        final Iterable<? extends Entry<Long, ?>> column2Entries =
            Iterables.limit(ROW_DATA.get(column2).subMap(6L, false, 4L, true).entrySet(), 1);

        testViewGet(request, Iterables.concat(column1Entries, column2Entries));
      }

      { // Mixed versions | no timerange
        final KijiDataRequest request = KijiDataRequest
            .builder()
            .addColumns(
                ColumnsDef.create()
                    .withPageSize(pageSize)
                    .withMaxVersions(1)
                    .add(column1.getFamily(), column1.getQualifier()))
            .addColumns(
                ColumnsDef.create()
                    .withPageSize(pageSize)
                    .withMaxVersions(100)
                    .add(column2.getFamily(), column2.getQualifier()))
            .build();

        final Iterable<? extends Entry<Long, ?>> column1Entries =
            Iterables.limit(ROW_DATA.get(column1).entrySet(), 1);
        final Iterable<? extends Entry<Long, ?>> column2Entries =
            ROW_DATA.get(column2).entrySet();

        testViewGet(request, Iterables.concat(column1Entries, column2Entries));
      }
    }
  }

  @Test
  public void testGetFamilyColumn() throws Exception {
    final Map<String, ? extends List<KijiColumnName>> families =
        ImmutableMap.of(
            PRIMITIVE_FAMILY, ImmutableList.of(PRIMITIVE_DOUBLE, PRIMITIVE_STRING),
            STRING_MAP_FAMILY, ImmutableList.of(STRING_MAP_1, STRING_MAP_2));

    for (Entry<String, ? extends List<KijiColumnName>> family : families.entrySet()) {
      for (int pageSize : ImmutableList.of(0, 1, 2, 10)) {

        final KijiColumnName familyColumn = KijiColumnName.create(family.getKey(), null);
        final KijiColumnName column1 = family.getValue().get(0);
        final KijiColumnName column2 = family.getValue().get(1);

        { // Single version | no timerange
          final KijiDataRequest request = KijiDataRequest
              .builder()
              .addColumns(ColumnsDef.create().withPageSize(pageSize).add(familyColumn))
              .build();

          final Iterable<? extends Entry<Long, ?>> column1Entries =
              Iterables.limit(ROW_DATA.get(column1).entrySet(), 1);
          final Iterable<? extends Entry<Long, ?>> column2Entries =
              Iterables.limit(ROW_DATA.get(column2).entrySet(), 1);

          testViewGet(request, Iterables.concat(column1Entries, column2Entries));
        }

        { // Single version | timerange
          final KijiDataRequest request = KijiDataRequest
              .builder()
              .addColumns(ColumnsDef.create().withPageSize(pageSize).add(familyColumn))
              .withTimeRange(4, 6)
              .build();

          final Iterable<? extends Entry<Long, ?>> column1Entries =
              Iterables.limit(ROW_DATA.get(column1).subMap(6L, false, 4L, true).entrySet(), 1);
          final Iterable<? extends Entry<Long, ?>> column2Entries =
              Iterables.limit(ROW_DATA.get(column2).subMap(6L, false, 4L, true).entrySet(), 1);

          testViewGet(request, Iterables.concat(column1Entries, column2Entries));
        }

        { // Multiple versions | no timerange
          final KijiDataRequest request = KijiDataRequest
              .builder()
              .addColumns(
                  ColumnsDef
                      .create()
                      .withPageSize(pageSize)
                      .withMaxVersions(100)
                      .add(familyColumn))
              .build();

          final Iterable<? extends Entry<Long, ?>> column1Entries =
              ROW_DATA.get(column1).entrySet();
          final Iterable<? extends Entry<Long, ?>> column2Entries =
              ROW_DATA.get(column2).entrySet();

          testViewGet(request, Iterables.concat(column1Entries, column2Entries));
        }

        { // Multiple versions | timerange
          final KijiDataRequest request = KijiDataRequest
              .builder()
              .addColumns(
                  ColumnsDef
                      .create()
                      .withPageSize(pageSize)
                      .withMaxVersions(100)
                      .add(familyColumn))
              .withTimeRange(4, 6)
              .build();

          final Iterable<? extends Entry<Long, ?>> column1Entries =
              ROW_DATA.get(column1).subMap(6L, false, 4L, true).entrySet();
          final Iterable<? extends Entry<Long, ?>> column2Entries =
              ROW_DATA.get(column2).subMap(6L, false, 4L, true).entrySet();

          testViewGet(request, Iterables.concat(column1Entries, column2Entries));
        }
      }
    }
  }

  @Test
  public void testGetMultipleFamilyColumns() throws Exception {
    final KijiColumnName familyColumn1 = KijiColumnName.create(PRIMITIVE_FAMILY, null);
    final KijiColumnName familyColumn2 = KijiColumnName.create(STRING_MAP_FAMILY, null);

    final KijiColumnName column1 = PRIMITIVE_DOUBLE;
    final KijiColumnName column2 = PRIMITIVE_STRING;
    final KijiColumnName column3 = STRING_MAP_1;
    final KijiColumnName column4 = STRING_MAP_2;

    for (int pageSize : ImmutableList.of(0, 1, 2, 10)) {

      { // Single version | no timerange
        final KijiDataRequest request = KijiDataRequest
            .builder()
            .addColumns(ColumnsDef.create().add(familyColumn1))
            .addColumns(ColumnsDef.create().add(familyColumn2))
            .build();

        final Iterable<? extends Entry<Long, ?>> column1Entries =
            Iterables.limit(ROW_DATA.get(column1).entrySet(), 1);
        final Iterable<? extends Entry<Long, ?>> column2Entries =
            Iterables.limit(ROW_DATA.get(column2).entrySet(), 1);
        final Iterable<? extends Entry<Long, ?>> column3Entries =
            Iterables.limit(ROW_DATA.get(column3).entrySet(), 1);
        final Iterable<? extends Entry<Long, ?>> column4Entries =
            Iterables.limit(ROW_DATA.get(column4).entrySet(), 1);

        testViewGet(
            request,
            Iterables.concat(column1Entries, column2Entries, column3Entries, column4Entries));
      }

      { // Single version | timerange
        final KijiDataRequest request = KijiDataRequest
            .builder()
            .addColumns(ColumnsDef.create().withPageSize(pageSize).add(familyColumn1))
            .addColumns(ColumnsDef.create().withPageSize(pageSize).add(familyColumn2))
            .withTimeRange(4, 6)
            .build();

        final Iterable<? extends Entry<Long, ?>> column1Entries =
            Iterables.limit(ROW_DATA.get(column1).subMap(6L, false, 4L, true).entrySet(), 1);
        final Iterable<? extends Entry<Long, ?>> column2Entries =
            Iterables.limit(ROW_DATA.get(column2).subMap(6L, false, 4L, true).entrySet(), 1);
        final Iterable<? extends Entry<Long, ?>> column3Entries =
            Iterables.limit(ROW_DATA.get(column3).subMap(6L, false, 4L, true).entrySet(), 1);
        final Iterable<? extends Entry<Long, ?>> column4Entries =
            Iterables.limit(ROW_DATA.get(column4).subMap(6L, false, 4L, true).entrySet(), 1);

        testViewGet(
            request,
            Iterables.concat(column1Entries, column2Entries, column3Entries, column4Entries));
      }

      { // Multiple versions | no timerange
        final KijiDataRequest request = KijiDataRequest
            .builder()
            .addColumns(
                ColumnsDef.create().withPageSize(pageSize).withMaxVersions(100).add(familyColumn1))
            .addColumns(
                ColumnsDef.create().withPageSize(pageSize).withMaxVersions(100).add(familyColumn2))
            .build();

        final Iterable<? extends Entry<Long, ?>> column1Entries = ROW_DATA.get(column1).entrySet();
        final Iterable<? extends Entry<Long, ?>> column2Entries = ROW_DATA.get(column2).entrySet();
        final Iterable<? extends Entry<Long, ?>> column3Entries = ROW_DATA.get(column3).entrySet();
        final Iterable<? extends Entry<Long, ?>> column4Entries = ROW_DATA.get(column4).entrySet();

        testViewGet(
            request,
            Iterables.concat(column1Entries, column2Entries, column3Entries, column4Entries));
      }

      { // Multiple versions | timerange
        final KijiDataRequest request = KijiDataRequest
            .builder()
            .addColumns(
                ColumnsDef.create().withPageSize(pageSize).withMaxVersions(100).add(familyColumn1))
            .addColumns(
                ColumnsDef.create().withPageSize(pageSize).withMaxVersions(100).add(familyColumn2))
            .withTimeRange(4, 6)
            .build();

        final Iterable<? extends Entry<Long, ?>> column1Entries =
            ROW_DATA.get(column1).subMap(6L, false, 4L, true).entrySet();
        final Iterable<? extends Entry<Long, ?>> column2Entries =
            ROW_DATA.get(column2).subMap(6L, false, 4L, true).entrySet();
        final Iterable<? extends Entry<Long, ?>> column3Entries =
            ROW_DATA.get(column3).subMap(6L, false, 4L, true).entrySet();
        final Iterable<? extends Entry<Long, ?>> column4Entries =
            ROW_DATA.get(column4).subMap(6L, false, 4L, true).entrySet();

        testViewGet(
            request,
            Iterables.concat(column1Entries, column2Entries, column3Entries, column4Entries));
      }

      { // Mixed versions | no timerange
        final KijiDataRequest request = KijiDataRequest
            .builder()
            .addColumns(
                ColumnsDef.create().withPageSize(pageSize).withMaxVersions(2).add(familyColumn1))
            .addColumns(
                ColumnsDef.create().withPageSize(pageSize).withMaxVersions(100).add(familyColumn2))
            .build();

        final Iterable<? extends Entry<Long, ?>> column1Entries =
            Iterables.limit(ROW_DATA.get(column1).entrySet(), 2);
        final Iterable<? extends Entry<Long, ?>> column2Entries =
            Iterables.limit(ROW_DATA.get(column2).entrySet(), 2);
        final Iterable<? extends Entry<Long, ?>> column3Entries = ROW_DATA.get(column3).entrySet();
        final Iterable<? extends Entry<Long, ?>> column4Entries = ROW_DATA.get(column4).entrySet();

        testViewGet(
            request,
            Iterables.concat(column1Entries, column2Entries, column3Entries, column4Entries));
      }

      { // Multiple versions | timerange
        final KijiDataRequest request = KijiDataRequest
            .builder()
            .addColumns(
                ColumnsDef.create().withPageSize(pageSize).withMaxVersions(100).add(familyColumn1))
            .addColumns(
                ColumnsDef.create().withPageSize(pageSize).withMaxVersions(1).add(familyColumn2))
            .withTimeRange(4, 6)
            .build();

        final Iterable<? extends Entry<Long, ?>> column1Entries =
            ROW_DATA.get(column1).subMap(6L, false, 4L, true).entrySet();
        final Iterable<? extends Entry<Long, ?>> column2Entries =
            ROW_DATA.get(column2).subMap(6L, false, 4L, true).entrySet();
        final Iterable<? extends Entry<Long, ?>> column3Entries =
            Iterables.limit(ROW_DATA.get(column3).subMap(6L, false, 4L, true).entrySet(), 1);
        final Iterable<? extends Entry<Long, ?>> column4Entries =
            Iterables.limit(ROW_DATA.get(column4).subMap(6L, false, 4L, true).entrySet(), 1);

        testViewGet(
            request,
            Iterables.concat(column1Entries, column2Entries, column3Entries, column4Entries));
      }
    }
  }

  @Test
  public void testNarrowView() throws Exception {
    final KijiColumnName familyColumn1 = KijiColumnName.create(PRIMITIVE_FAMILY, null);
    final KijiColumnName familyColumn2 = KijiColumnName.create(STRING_MAP_FAMILY, null);

    final KijiColumnName column1 = PRIMITIVE_DOUBLE;
    final KijiColumnName column2 = PRIMITIVE_STRING;
    final KijiColumnName column3 = STRING_MAP_1;
    final KijiColumnName column4 = STRING_MAP_2;

    for (int pageSize : ImmutableList.of(0)) {

      final KijiDataRequest request = KijiDataRequest
          .builder()
          .addColumns(
              ColumnsDef.create().withPageSize(pageSize).withMaxVersions(100).add(familyColumn1))
          .addColumns(
              ColumnsDef.create().withPageSize(pageSize).withMaxVersions(100).add(column3))
          .addColumns(ColumnsDef.create().withPageSize(pageSize).add(column4))
          .withTimeRange(2, 10)
          .build();

      final KijiResult<Object> view = mReader.getResult(mTable.getEntityId(ROW), request);
      try {
        testViewGet(view.narrowView(PRIMITIVE_LONG), ImmutableList.<Entry<Long, ?>>of());

        final Iterable<? extends Entry<Long, ?>> column1Entries =
            ROW_DATA.get(column1).subMap(10L, false, 2L, true).entrySet();
        testViewGet(view.narrowView(column1), column1Entries);

        final Iterable<? extends Entry<Long, ?>> column2Entries =
            ROW_DATA.get(column2).subMap(10L, false, 2L, true).entrySet();
        testViewGet(view.narrowView(column2), column2Entries);

        testViewGet(view.narrowView(familyColumn1),
            Iterables.concat(column1Entries, column2Entries));

        final Iterable<? extends Entry<Long, ?>> column3Entries =
            ROW_DATA.get(column3).subMap(10L, false, 2L, true).entrySet();
        testViewGet(view.narrowView(column3), column3Entries);

        final Iterable<? extends Entry<Long, ?>> column4Entries =
            Iterables.limit(ROW_DATA.get(column4).subMap(10L, false, 2L, true).entrySet(), 1);
        testViewGet(view.narrowView(column4), column4Entries);

        testViewGet(view.narrowView(familyColumn2),
            Iterables.concat(column3Entries, column4Entries));
      } finally {
        view.close();
      }
    }
  }

  @Test
  public void testGetWithFilters() throws Exception {
    final KijiColumnName column1 = PRIMITIVE_STRING;
    final KijiColumnName column2 = STRING_MAP_1;

    for (int pageSize : ImmutableList.of(0, 1, 2, 10)) {
      { // single column | CRF
        final KijiDataRequest request = KijiDataRequest
            .builder()
            .addColumns(
                ColumnsDef.create()
                    .withPageSize(pageSize)
                    .withFilter(
                        new KijiColumnRangeFilter(
                            STRING_MAP_1.getQualifier(), true,
                            STRING_MAP_2.getQualifier(), false))
                    .withMaxVersions(10)
                    .add(column2.getFamily(), null))
            .build();

        final Iterable<? extends Entry<Long, ?>> column1Entries = ROW_DATA.get(column2).entrySet();

        testViewGet(request, column1Entries);
      }
    }
  }

  @Test
  public void testGetMatContents() throws Exception {
    KijiDataRequestBuilder builder = KijiDataRequest.builder().addColumns(ColumnsDef.create()
        .withMaxVersions(10)
        .add(PRIMITIVE_STRING, null)
            .add(STRING_MAP_1, null));
    KijiDataRequest request = builder.build();
    final EntityId eid = mTable.getEntityId(ROW);
    KijiResult<Object> view = mReader.getResult(eid, request);
    SortedMap<KijiColumnName, List<KijiCell<Object>>> map =
        KijiResult.Helpers.getMaterializedContents(view);
    for (KijiColumnName col: map.keySet()) {
      KijiResult<Object> newResult = view.narrowView(col);
      Iterator<KijiCell<Object>> it = newResult.iterator();
      for (KijiCell<Object> cell: map.get(col)) {
        assertEquals(cell, it.next());
      }
      assertTrue(!it.hasNext());
    }

  }

}
