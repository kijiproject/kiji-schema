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

package org.kiji.schema.filter;

import static org.junit.Assert.assertEquals;

import static org.kiji.schema.avro.ComponentType.INTEGER;
import static org.kiji.schema.avro.ComponentType.LONG;
import static org.kiji.schema.avro.ComponentType.STRING;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Random;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.avro.ComponentType;
import org.kiji.schema.avro.HashSpec;
import org.kiji.schema.avro.HashType;
import org.kiji.schema.avro.RowKeyComponent;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat2;

/** Tests the FormattedEntityIdRowFilter. */
public class TestFormattedEntityIdRowFilter {
  // filter is a backwards operation, so false means the row will be included
  private static final boolean INCLUDE = false;
  private static final boolean EXCLUDE = true;

  private static final Random RANDOM = new Random(1001L);

  private static final RowKeyFormat2[] TEST_CASES = new RowKeyFormat2[] {
    createRowKeyFormat(1, INTEGER),
    createRowKeyFormat(5, INTEGER),

    createRowKeyFormat(1, LONG),
    createRowKeyFormat(5, LONG),

    createRowKeyFormat(1, STRING),
    createRowKeyFormat(5, STRING),

    createRowKeyFormat(1, STRING, STRING),
    createRowKeyFormat(5, STRING, STRING),

    createRowKeyFormat(1, INTEGER, INTEGER),
    createRowKeyFormat(5, INTEGER, INTEGER),

    createRowKeyFormat(1, LONG, LONG),
    createRowKeyFormat(5, LONG, LONG),

    createRowKeyFormat(1, INTEGER, LONG, STRING),
    createRowKeyFormat(5, INTEGER, LONG, STRING),

    createRowKeyFormat(1, STRING, INTEGER, LONG, STRING),
    createRowKeyFormat(5, STRING, INTEGER, LONG, STRING),
  };

  private static RowKeyFormat2 createRowKeyFormat(int hashLength, ComponentType... componentTypes) {
    RowKeyFormat2.Builder builder = RowKeyFormat2.newBuilder()
        .setEncoding(RowKeyEncoding.FORMATTED);
    if (hashLength > 0) {
      builder.setSalt(new HashSpec(HashType.MD5, hashLength, false));
    }
    List<RowKeyComponent> components = Lists.newArrayList();
    char field = 'a';
    for (ComponentType componentType : componentTypes) {
      components.add(new RowKeyComponent(String.valueOf(field), componentType));
      field = (char) (field + 1);
    }
    builder.setComponents(components);
    return builder.build();
  }

  private static FormattedEntityIdRowFilter createFilter(RowKeyFormat2 format, Object... components)
      throws Exception {
    return new FormattedEntityIdRowFilter(format, components);
  }

  private static Object createStableValue(ComponentType type) {
    switch (type) {
      case INTEGER:
        return 42;
      case LONG:
        return 349203L;
      case STRING:
        return "value";
      default:
        throw new IllegalArgumentException("Unknown ComponentType: " + type);
    }
  }

  private static Object createRandomValue(ComponentType type) {
    switch (type) {
      case INTEGER:
        return RANDOM.nextInt();
      case LONG:
        return RANDOM.nextLong();
      case STRING:
        byte[] bytes = new byte[16];
        RANDOM.nextBytes(bytes);
        return Bytes.toStringBinary(bytes);
      default:
        throw new IllegalArgumentException("Unknown ComponentType: " + type);
    }
  }

  private static Object createMinValue(ComponentType type) {
    switch (type) {
      case INTEGER:
        return Integer.MIN_VALUE;
      case LONG:
        return Long.MIN_VALUE;
      case STRING:
        return "";
      default:
        throw new IllegalArgumentException("Unknown ComponentType: " + type);
    }
  }

  private static class FilterAndTestValues {
    private List<Object> mFilterValues = Lists.newArrayList();
    private List<List<Object>> mIncludedTestValues = Lists.newArrayList();
    private List<List<Object>> mExcludedTestValues = Lists.newArrayList();
  }

  private static List<FilterAndTestValues> createFilterAndTestValues(
      List<RowKeyComponent> components) {
    List<FilterAndTestValues> filterAndTestValues = Lists.newArrayList();

    List<List<Object>> filterCombinations = createFilterCombinations(components);
    // skip over the last all-null combination, which does not make much sense
    // for a filter
    for (List<Object> filterValues : filterCombinations.subList(0, filterCombinations.size() - 1)) {
      FilterAndTestValues fatv = new FilterAndTestValues();
      fatv.mFilterValues = filterValues;

      fatv.mIncludedTestValues.add(correctEntityComponents(components, filterValues));

      List<List<Object>> excludedCombinations =
          createExcludedCombinations(components, filterValues);
      for (List<Object> excludedCombination : excludedCombinations) {
        fatv.mExcludedTestValues.add(excludedCombination);
      }

      filterAndTestValues.add(fatv);
    }

    return filterAndTestValues;
  }

  // corrects values so that the EntityId constructed from this set can be
  // constructed. returns a new list with the corrected values
  private static List<Object> correctEntityComponents(
      List<RowKeyComponent> components, List<Object> values) {
    List<Object> correctedValues = Lists.newArrayList(values);
    for (int i = 0; i < correctedValues.size(); i++) {
      if (null == correctedValues.get(i)) {
        correctedValues.set(i, createRandomValue(components.get(i).getType()));
      }
    }
    return correctedValues;
  }

  private static List<List<Object>> createFilterCombinations(List<RowKeyComponent> components) {
    List<List<Object>> combinations = Lists.newArrayList();
    ComponentType type = components.get(0).getType();
    if (components.size() == 1) {
      combinations.add(Lists.newArrayList(createStableValue(type)));
      combinations.add(Lists.newArrayList((Object)null));
    } else {
      List<List<Object>> subCombinations =
          createFilterCombinations(components.subList(1, components.size()));
      for (List<Object> subCombination : subCombinations) {
        List<Object> newCombination = Lists.newArrayList(createStableValue(type));
        newCombination.addAll(subCombination);
        combinations.add(newCombination);

        newCombination = Lists.newArrayList((Object)null);
        newCombination.addAll(subCombination);
        combinations.add(newCombination);
      }
    }
    return combinations;
  }

  private static List<List<Object>> createExcludedCombinations(
      List<RowKeyComponent> components, List<Object> filterValues) {
    List<List<Object>> combinations = Lists.newArrayList();
    ComponentType type = components.get(0).getType();
    if (filterValues.size() == 1) {
      combinations.add(Lists.newArrayList(createRandomValue(type)));
      combinations.add(Lists.newArrayList(createMinValue(type)));
    } else {
      List<List<Object>> subCombinations = createExcludedCombinations(
        components.subList(1, components.size()), filterValues.subList(1, filterValues.size()));
      for (List<Object> subCombination : subCombinations) {
        List<Object> newCombination = Lists.newArrayList(createRandomValue(type));
        newCombination.addAll(subCombination);
        combinations.add(newCombination);

        newCombination = Lists.newArrayList(createMinValue(type));
        newCombination.addAll(subCombination);
        combinations.add(newCombination);
      }
    }
    return combinations;
  }

  @Test
  public void testAllCases() throws Exception {
    for (RowKeyFormat2 rowKeyFormat : TEST_CASES) {
      EntityIdFactory factory = EntityIdFactory.getFactory(rowKeyFormat);
      List<FilterAndTestValues> filterAndTestValues =
          createFilterAndTestValues(rowKeyFormat.getComponents());
      for (FilterAndTestValues filterAndTest : filterAndTestValues) {
        FormattedEntityIdRowFilter filter =
            createFilter(rowKeyFormat, filterAndTest.mFilterValues.toArray());
        for (List<Object> includedValues : filterAndTest.mIncludedTestValues) {
          runTest(rowKeyFormat, filter, factory, INCLUDE, includedValues.toArray());
        }
        for (List<Object> excludedValues : filterAndTest.mExcludedTestValues) {
          runTest(rowKeyFormat, filter, factory, EXCLUDE, excludedValues.toArray());
        }
      }
    }
  }

  private final RowKeyFormat2 mRowKeyFormat = createRowKeyFormat(1, INTEGER, LONG, STRING);

  private final EntityIdFactory mFactory = EntityIdFactory.getFactory(mRowKeyFormat);

  @Test
  public void testFormattedEntityIdRowFilter() throws Exception {
    FormattedEntityIdRowFilter filter = createFilter(mRowKeyFormat, 100, null, "value");
    runTest(mRowKeyFormat, filter, mFactory, INCLUDE, 100, 2000L, "value");
    runTest(mRowKeyFormat, filter, mFactory, EXCLUDE, 100, null, null);
    runTest(mRowKeyFormat, filter, mFactory, EXCLUDE, 0, null, null);
  }

  @Test
  public void testPrefixMatching() throws Exception {
    FormattedEntityIdRowFilter filter = createFilter(mRowKeyFormat, 42, null, null);
    runTest(mRowKeyFormat, filter, mFactory, INCLUDE, 42, 4200L, "name");
    runTest(mRowKeyFormat, filter, mFactory, INCLUDE, 42, 4200L, null);
    runTest(mRowKeyFormat, filter, mFactory, INCLUDE, 42, null, null);
    runTest(mRowKeyFormat, filter, mFactory, EXCLUDE, 43, 4200L, "name");
  }

  @Test
  public void testMidComponentMatching() throws Exception {
    FormattedEntityIdRowFilter filter = createFilter(mRowKeyFormat, null, 6000L, null);
    runTest(mRowKeyFormat, filter, mFactory, INCLUDE, 50, 6000L, "anything");
    runTest(mRowKeyFormat, filter, mFactory, INCLUDE, 50, 6000L, null);
    runTest(mRowKeyFormat, filter, mFactory, EXCLUDE, 50, 5999L, "anything");
  }

  @Test
  public void testSuffixComponentMatching() throws Exception {
    FormattedEntityIdRowFilter filter = createFilter(mRowKeyFormat, null, null, "value");
    runTest(mRowKeyFormat, filter, mFactory, INCLUDE, 50, 6000L, "value");
    runTest(mRowKeyFormat, filter, mFactory, EXCLUDE, 50, 6000L, null);
    runTest(mRowKeyFormat, filter, mFactory, EXCLUDE, 50, 5999L, "anything");
  }

  @Test
  public void testPrefixNumberMatching() throws Exception {
    RowKeyFormat2 rowKeyFormat = createRowKeyFormat(1, LONG, LONG);
    EntityIdFactory factory = EntityIdFactory.getFactory(rowKeyFormat);
    FormattedEntityIdRowFilter filter = createFilter(rowKeyFormat, 4224L, null);
    runTest(rowKeyFormat, filter, factory, INCLUDE, 4224L, 5005L);
    runTest(rowKeyFormat, filter, factory, INCLUDE, 4224L, null);
    runTest(rowKeyFormat, filter, factory, INCLUDE, 4224L, Long.MAX_VALUE);
    runTest(rowKeyFormat, filter, factory, INCLUDE, 4224L, Long.MIN_VALUE);
    runTest(rowKeyFormat, filter, factory, EXCLUDE, Long.MIN_VALUE, 5005L);
    runTest(rowKeyFormat, filter, factory, EXCLUDE, Long.MIN_VALUE, null);
    runTest(rowKeyFormat, filter, factory, EXCLUDE, Long.MIN_VALUE, Long.MAX_VALUE);
    runTest(rowKeyFormat, filter, factory, EXCLUDE, Long.MIN_VALUE, Long.MIN_VALUE);
  }

  @Test
  public void testUnicodeStringInFilterMatching() throws Exception {
    RowKeyFormat2 rowKeyFormat = createRowKeyFormat(1, STRING);
    EntityIdFactory factory = EntityIdFactory.getFactory(rowKeyFormat);
    String match = "This is a star: \u2605";
    String noMatch = "This is not a star";
    FormattedEntityIdRowFilter filter = createFilter(rowKeyFormat, match);
    runTest(rowKeyFormat, filter, factory, INCLUDE, match);
    runTest(rowKeyFormat, filter, factory, EXCLUDE, noMatch);
  }

  @Test
  public void testUnicodeStringInEntityIdMatching() throws Exception {
    RowKeyFormat2 rowKeyFormat = createRowKeyFormat(1, STRING);
    EntityIdFactory factory = EntityIdFactory.getFactory(rowKeyFormat);
    String match = "This is not a star";
    String noMatch = "This is a star: \u2605";
    FormattedEntityIdRowFilter filter = createFilter(rowKeyFormat, match);
    runTest(rowKeyFormat, filter, factory, INCLUDE, match);
    runTest(rowKeyFormat, filter, factory, EXCLUDE, noMatch);
  }

  @Test
  public void testPrefixDefinedByFewerThanFormatComponents() throws Exception {
    // this is the same as a filter defined with (100, null, null)
    FormattedEntityIdRowFilter filter = createFilter(mRowKeyFormat, 100);
    runTest(mRowKeyFormat, filter, mFactory, INCLUDE, 100, 2000L, "value");
    runTest(mRowKeyFormat, filter, mFactory, INCLUDE, 100, null, null);
    runTest(mRowKeyFormat, filter, mFactory, EXCLUDE, 0, 2000L, "value");
    runTest(mRowKeyFormat, filter, mFactory, EXCLUDE, 0, null, null);
  }

  @Test
  public void testLatinNewlineCharacterInclusion() throws Exception {
    RowKeyFormat2 rowKeyFormat = createRowKeyFormat(1, INTEGER, LONG);
    EntityIdFactory factory = EntityIdFactory.getFactory(rowKeyFormat);

    // Create and serialize a filter.
    FormattedEntityIdRowFilter filter = createFilter(rowKeyFormat, 10);
    byte[] serializedFilter = filter.toHBaseFilter(null).toByteArray();

    // Deserialize the filter.
    Filter deserializedFilter = FilterList.parseFrom(serializedFilter);

    // Filter an entity with the deserialized filter.
    EntityId entityId = factory.getEntityId(10, 10L);
    byte[] hbaseKey = entityId.getHBaseRowKey();
    boolean filtered = deserializedFilter.filterRowKey(hbaseKey, 0, hbaseKey.length);
    assertEquals(INCLUDE, filtered);
  }

  @Test
  public void testHashIsCalculatedWhenAllHashComponentsAreSpecified() throws Exception {
    final int hashLength = 2;
    RowKeyFormat2.Builder builder = RowKeyFormat2.newBuilder()
        .setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(new HashSpec(HashType.MD5, hashLength, false))
        .setRangeScanStartIndex(1);

    List<RowKeyComponent> components = ImmutableList.of(
            new RowKeyComponent("id", INTEGER), // this one is included in the hash
            new RowKeyComponent("ts", LONG));   // this one is not
    builder.setComponents(components);
    RowKeyFormat2 rowKeyFormat = builder.build();

    EntityIdFactory factory = EntityIdFactory.getFactory(rowKeyFormat);
    FormattedEntityIdRowFilter filter = createFilter(rowKeyFormat, 100);
    Object[] componentValues = new Object[] { Integer.valueOf(100), Long.valueOf(900000L) };
    runTest(rowKeyFormat, filter, factory, INCLUDE, componentValues);

    EntityId entityId = factory.getEntityId(componentValues);
    byte[] hbaseKey = entityId.getHBaseRowKey();
    Filter hbaseFilter = filter.toHBaseFilter(null);

    // A row key with a different hash but the same first component should be
    // excluded by the filter. The hash is 0x9f0f
    hbaseKey[0] = (byte) 0x7F;
    hbaseKey[1] = (byte) 0xFF;
    boolean filtered = hbaseFilter.filterRowKey(hbaseKey, 0, hbaseKey.length);
    doInclusionAssert(rowKeyFormat, filter, entityId, hbaseFilter, hbaseKey, EXCLUDE);
  }

  @Test
  public void testHashWildcardIsUsedForMissingHashComponents() throws Exception {
    RowKeyFormat2 rowKeyFormat = createRowKeyFormat(1, INTEGER, LONG, STRING);
    rowKeyFormat.setRangeScanStartIndex(2);
    EntityIdFactory factory = EntityIdFactory.getFactory(rowKeyFormat);

    FormattedEntityIdRowFilter filter = createFilter(rowKeyFormat, 100, null, "value");
    runTest(rowKeyFormat, filter, factory, INCLUDE, 100, 2000L, "value");
    runTest(rowKeyFormat, filter, factory, EXCLUDE, 100, null, null);
    runTest(rowKeyFormat, filter, factory, EXCLUDE, 0, null, null);
  }

  @Test
  public void testPrefixFilterHaltsFiltering() throws Exception {
    RowKeyFormat2 rowKeyFormat = createRowKeyFormat(1, INTEGER, LONG, LONG);
    EntityIdFactory factory = EntityIdFactory.getFactory(rowKeyFormat);
    FormattedEntityIdRowFilter filter = createFilter(rowKeyFormat, 100, null, 9000L);
    Filter hbaseFilter = filter.toHBaseFilter(null);

    EntityId passingEntityId = factory.getEntityId(100, 100L, 9000L);
    byte[] passingHbaseKey = passingEntityId.getHBaseRowKey();
    doInclusionAssert(rowKeyFormat, filter, passingEntityId, hbaseFilter, passingHbaseKey, INCLUDE);
    boolean filterAllRemaining = hbaseFilter.filterAllRemaining();
    String message = createFailureMessage(rowKeyFormat, filter, passingEntityId, hbaseFilter,
        passingHbaseKey, filterAllRemaining);
    assertEquals(message, false, filterAllRemaining);

    EntityId failingEntityId = factory.getEntityId(101, 100L, 9000L);
    byte[] failingHbaseKey = failingEntityId.getHBaseRowKey();
    doInclusionAssert(rowKeyFormat, filter, failingEntityId, hbaseFilter, failingHbaseKey, EXCLUDE);
    filterAllRemaining = hbaseFilter.filterAllRemaining();
    message = createFailureMessage(rowKeyFormat, filter, failingEntityId, hbaseFilter,
        failingHbaseKey, filterAllRemaining);
    assertEquals(message, true, filterAllRemaining);
  }

  private void runTest(RowKeyFormat2 rowKeyFormat, FormattedEntityIdRowFilter filter,
      EntityIdFactory factory, boolean expectedFilter, Object... components) throws Exception {
    EntityId entityId = factory.getEntityId(components);
    byte[] hbaseKey = entityId.getHBaseRowKey();
    Filter hbaseFilter = filter.toHBaseFilter(null);
    doInclusionAssert(rowKeyFormat, filter, entityId, hbaseFilter, hbaseKey, expectedFilter);
  }

  private void doInclusionAssert(RowKeyFormat2 rowKeyFormat, FormattedEntityIdRowFilter filter,
      EntityId entityId, Filter hbaseFilter, byte[] hbaseKey, boolean expectedFilter)
      throws Exception {
    boolean filtered = hbaseFilter.filterRowKey(hbaseKey, 0, hbaseKey.length);
    String message = createFailureMessage(rowKeyFormat, filter, entityId, hbaseFilter,
        hbaseKey, filtered);
    assertEquals(message, expectedFilter, filtered);
  }

  private String createFailureMessage(RowKeyFormat2 rowKeyFormat, FormattedEntityIdRowFilter filter,
      EntityId entityId, Filter hbaseFilter, byte[] hbaseKey, boolean filtered)
      throws Exception {
    return String.format(
        "RowKeyFormat: %s%nComponents: %s%nEntityId: %s%nFilter: %s%nHBase key: %s%nIncluded: %s%n",
        rowKeyFormat, fetchComponents(filter), entityId.toShellString(),
        filterToString(hbaseFilter), toBinaryString(hbaseKey), !filtered);
  }

  private String toBinaryString(byte[] bytes) {
    StringBuilder buf = new StringBuilder();
    for (byte b : bytes) {
      buf.append(String.format("\\x%02x", b & 0xFF));
    }
    return buf.toString();
  }

  private String fetchComponents(FormattedEntityIdRowFilter filter) throws Exception {
    Field componentField = filter.getClass().getDeclaredField("mComponents");
    componentField.setAccessible(true);
    return Lists.newArrayList((Object[])componentField.get(filter)).toString();
  }

  private String filterToString(Filter filter) throws Exception {
    if (filter instanceof FilterList) {
      List<Filter> filters = ((FilterList) filter).getFilters();
      return String.format("[%s] AND [%s]",
          prefixFilterToString((PrefixFilter) filters.get(0)),
          filter.toString());
    } else {
      return filter.toString();
    }
  }

  private String prefixFilterToString(PrefixFilter prefixFilter) throws Exception {
    return toBinaryString(prefixFilter.getPrefix());
  }
}
