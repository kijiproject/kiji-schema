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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import junit.framework.Assert;
import org.apache.avro.util.Utf8;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiResult;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestHBaseKijiResult extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseKijiResult.class);

  private static final KijiDataRequest REQUEST = KijiDataRequest.builder().addColumns(
      ColumnsDef.create()
          .withMaxVersions(10)
          .add("primitive", "string_column")
          .add("primitive", "double_column")
  ).build();
  private static final KijiDataRequest PAGED_REQUEST = KijiDataRequest.builder().addColumns(
      ColumnsDef.create()
          .withPageSize(2)
          .withMaxVersions(10)
          .add("primitive", "string_column")
          .add("primitive", "double_column")
  ).build();
  private static final KijiDataRequest MAP_REQUEST = KijiDataRequest.builder().addColumns(
      ColumnsDef.create().withMaxVersions(10).add("string_map", null)).build();
  private static final KijiDataRequest PAGED_MAP_REQUEST = KijiDataRequest.builder().addColumns(
      ColumnsDef.create().withPageSize(1).withMaxVersions(10).add("string_map", null)).build();
  private static final KijiDataRequest COMPLETE_REQUEST = KijiDataRequest.builder().addColumns(
      ColumnsDef.create()
          .withMaxVersions(10)
          .add("string_map", null)
          .add("primitive", "string_column")
          .add("primitive", "double_column")
  ).build();
  private static final KijiDataRequest PAGED_COMPLETE_REQUEST = KijiDataRequest.builder()
      .addColumns(ColumnsDef.create()
          .withPageSize(2)
          .withMaxVersions(10)
          .add("string_map", null)
          .add("primitive", "string_column")
          .add("primitive", "double_column")
      ).build();
  private static final KijiDataRequest MAX_VERSIONS_REQUEST = KijiDataRequest.builder()
      .addColumns(ColumnsDef.create()
          .withMaxVersions(3)
          .add("string_map", null)
          .add("primitive", "string_column"))
      .addColumns(ColumnsDef.create()
          .withMaxVersions(10)
          .add("primitive", "double_column"))
      .build();
  private static final KijiDataRequest PAGED_MAX_VERSIONS_REQUEST = KijiDataRequest.builder()
      .addColumns(ColumnsDef.create()
          .withPageSize(2)
          .withMaxVersions(3)
          .add("string_map", null)
          .add("primitive", "string_column"))
      .addColumns(ColumnsDef.create()
          .withPageSize(2)
          .withMaxVersions(10)
          .add("primitive", "double_column"))
      .build();
  private static final KijiDataRequest EMPTY_COLUMN_REQUEST = KijiDataRequest.builder()
      .addColumns(ColumnsDef.create()
          .withMaxVersions(3)
          .add("primitive", "string_column")
          .add("primitive", "boolean_column"))
      .build();


  private static final KijiColumnName PRIMITIVE_STRING =
      new KijiColumnName("primitive", "string_column");
  private static final KijiColumnName PRIMITIVE_DOUBLE =
      new KijiColumnName("primitive", "double_column");
  private static final KijiColumnName PRIMITIVE_BOOLEAN =
      new KijiColumnName("primitive", "boolean_column");
  private static final KijiColumnName STRING_MAP = new KijiColumnName("string_map", null);
  private static final KijiColumnName STRING_MAP_1 =
      new KijiColumnName("string_map", "smap_1");
  private static final KijiColumnName STRING_MAP_2 =
      new KijiColumnName("string_map", "smap_2");

  private static final String EXPECTED_PAGED_GET_EXCEPTION_PREFIX =
      "Cannot get a cell from a paged column. Found column: ";
  private static final String EXPECTED_PAGED_MOST_RECENT_EXCEPTION_PREFIX =
      "Cannot get the most recent version of a paged column. Found column: ";
  private static final String EXPECTED_NO_REQUEST_EXCEPTION_PREFIX =
      "No request for column: ";

  // The reader is not used it tests, but it may not be closed until @After.
  private HBaseKijiTableReader mReader = null;
  private KijiResult mResult = null;
  private KijiResult mPagedResult = null;
  private KijiResult mMapResult = null;
  private KijiResult mPagedMapResult = null;
  private KijiResult mCompleteResult = null;
  private KijiResult mPagedCompleteResult = null;
  private KijiResult mMaxVersionsResult = null;
  private KijiResult mPagedMaxVersionsResult = null;
  private KijiResult mEmptyColumnResult = null;

  @Before
  public void setupTestHBaseKijiResult() throws IOException {
    new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout("org/kiji/schema/layout/all-types-schema.json"))
            .withRow(1)
                .withFamily("primitive")
                    .withQualifier("string_column")
                        .withValue(10, "ten")
                        .withValue(5, "five")
                        .withValue(4, "four")
                        .withValue(3, "three")
                        .withValue(2, "two")
                        .withValue(1, "one")
                    .withQualifier("double_column")
                        .withValue(10, 10.0)
                        .withValue(5, 5.0)
                        .withValue(4, 4.0)
                        .withValue(3, 3.0)
                        .withValue(2, 2.0)
                        .withValue(1, 1.0)
                .withFamily("string_map")
                    .withQualifier("smap_1")
                        .withValue(10, "sm1-ten")
                        .withValue(5, "sm1-five")
                        .withValue(4, "sm1-four")
                        .withValue(3, "sm1-three")
                        .withValue(2, "sm1-two")
                        .withValue(1, "sm1-one")
                    .withQualifier("smap_2")
                        .withValue(10, "sm2-ten")
                        .withValue(5, "sm2-five")
                        .withValue(4, "sm2-four")
                        .withValue(3, "sm2-three")
                        .withValue(2, "sm2-two")
                        .withValue(1, "sm2-one")
        .build();
    final HBaseKijiTable table = HBaseKijiTable.downcast(getKiji().openTable("all_types_table"));
    try {
      final EntityId eid = table.getEntityId(1);
      mReader = (HBaseKijiTableReader) table.openTableReader();
      mResult = mReader.getResult(eid, REQUEST);
      mPagedResult = mReader.getResult(eid, PAGED_REQUEST);
      mMapResult = mReader.getResult(eid, MAP_REQUEST);
      mPagedMapResult = mReader.getResult(eid, PAGED_MAP_REQUEST);
      mCompleteResult = mReader.getResult(eid, COMPLETE_REQUEST);
      mPagedCompleteResult = mReader.getResult(eid, PAGED_COMPLETE_REQUEST);
      mMaxVersionsResult = mReader.getResult(eid, MAX_VERSIONS_REQUEST);
      mPagedMaxVersionsResult = mReader.getResult(eid, PAGED_MAX_VERSIONS_REQUEST);
      mEmptyColumnResult = mReader.getResult(eid, EMPTY_COLUMN_REQUEST);
    } finally {
      table.release();
    }
  }

  @After
  public void cleanupTestHBaseKijiResult() throws IOException {
    mReader.close();
  }

  @Test
  public void testGets() throws IOException {
    {
      final String expected = "ten";
      final String actual = mResult.getMostRecentCell(PRIMITIVE_STRING).getData().toString();
      Assert.assertEquals(expected, actual);
    }
    {
      final Double expected = 10.0;
      final Double actual = mResult.<Double>getMostRecentCell(PRIMITIVE_DOUBLE).getData();
      Assert.assertEquals(expected, actual);
    }
    {
      final String expected = "three";
      final String actual = mResult.getCell(PRIMITIVE_STRING, 3).getData().toString();
      Assert.assertEquals(expected, actual);
    }
    {
      final Double expected = 3.0;
      final Double actual = mResult.<Double>getCell(PRIMITIVE_DOUBLE, 3).getData();
      Assert.assertEquals(expected, actual);
    }
    {
      try {
        mPagedResult.getMostRecentCell(PRIMITIVE_STRING);
        Assert.fail();
      } catch (IllegalArgumentException iae) {
        Assert.assertTrue(iae.getMessage().startsWith(EXPECTED_PAGED_MOST_RECENT_EXCEPTION_PREFIX));
      }
      try {
        mPagedResult.getMostRecentCell(PRIMITIVE_DOUBLE);
        Assert.fail();
      } catch (IllegalArgumentException iae) {
        Assert.assertTrue(iae.getMessage().startsWith(EXPECTED_PAGED_MOST_RECENT_EXCEPTION_PREFIX));
      }
    }
    {
      try {
        mPagedResult.getCell(PRIMITIVE_STRING, 3);
        Assert.fail();
      } catch (IllegalArgumentException iae) {
        Assert.assertTrue(iae.getMessage().startsWith(EXPECTED_PAGED_GET_EXCEPTION_PREFIX));
      }
      try {
        mPagedResult.getCell(PRIMITIVE_DOUBLE, 3);
        Assert.fail();
      } catch (IllegalArgumentException iae) {
        Assert.assertTrue(iae.getMessage().startsWith(EXPECTED_PAGED_GET_EXCEPTION_PREFIX));
      }
    }
  }

  @Test
  public void testIterator() throws NoSuchColumnException {
    {
      final List<String> expected =
          Lists.newArrayList("ten", "five", "four", "three", "two", "one");
      final List<String> actual = Lists.newArrayList();
      final Iterator<KijiCell<Utf8>> nameIterator = mResult.iterator(PRIMITIVE_STRING);
      while (nameIterator.hasNext()) {
        actual.add(nameIterator.next().getData().toString());
      }
      Assert.assertEquals(expected, actual);
    }
    {
      final List<Double> expected = Lists.newArrayList(10.0, 5.0, 4.0, 3.0, 2.0, 1.0);
      final List<Double> actual = Lists.newArrayList();
      final Iterator<KijiCell<Double>> nameIterator = mResult.iterator(PRIMITIVE_DOUBLE);
      while (nameIterator.hasNext()) {
        actual.add(nameIterator.next().getData());
      }
      Assert.assertEquals(expected, actual);
    }
    {
      final List<String> expected =
          Lists.newArrayList("ten", "five", "four", "three", "two", "one");
      final List<String> actual = Lists.newArrayList();
      final Iterator<KijiCell<Utf8>> nameIterator = mPagedResult.iterator(PRIMITIVE_STRING);
      while (nameIterator.hasNext()) {
        actual.add(nameIterator.next().getData().toString());
      }
      Assert.assertEquals(expected, actual);
    }
    {
      final List<Double> expected = Lists.newArrayList(10.0, 5.0, 4.0, 3.0, 2.0, 1.0);
      final List<Double> actual = Lists.newArrayList();
      final Iterator<KijiCell<Double>> nameIterator = mPagedResult.iterator(PRIMITIVE_DOUBLE);
      while (nameIterator.hasNext()) {
        actual.add(nameIterator.next().getData());
      }
      Assert.assertEquals(expected, actual);
    }
  }

  @Test
  public void testMapGets() throws IOException {
    {
      final String expected = "sm1-ten";
      final String actual = mMapResult.getMostRecentCell(STRING_MAP_1).getData().toString();
      Assert.assertEquals(expected, actual);
    }
    {
      final String expected = "sm2-three";
      final String actual = mMapResult.getCell(STRING_MAP_2, 3).getData().toString();
      Assert.assertEquals(expected, actual);
    }
    {
      try {
        mPagedMapResult.getMostRecentCell(STRING_MAP_1);
        Assert.fail();
      } catch (IllegalArgumentException iae) {
        Assert.assertTrue(iae.getMessage().startsWith(EXPECTED_PAGED_MOST_RECENT_EXCEPTION_PREFIX));
      }
    }
    {
      try {
        mPagedMapResult.getCell(STRING_MAP_2, 3);
        Assert.fail();
      } catch (IllegalArgumentException iae) {
        Assert.assertTrue(iae.getMessage().startsWith(EXPECTED_PAGED_GET_EXCEPTION_PREFIX));
      }
    }
  }

  @Test
  public void testMapIterator() throws NoSuchColumnException {
    {
      final List<String> expected = Lists.newArrayList(
          "sm1-ten", "sm1-five", "sm1-four", "sm1-three", "sm1-two", "sm1-one");
      final List<String> actual = Lists.newArrayList();
      final Iterator<KijiCell<Utf8>> it = mMapResult.iterator(STRING_MAP_1);
      while (it.hasNext()) {
        actual.add(it.next().getData().toString());
      }
      Assert.assertEquals(expected, actual);
    }
    {
      final List<String> expected = Lists.newArrayList(
          "sm1-ten", "sm1-five", "sm1-four", "sm1-three", "sm1-two", "sm1-one",
          "sm2-ten", "sm2-five", "sm2-four", "sm2-three", "sm2-two", "sm2-one");
      final List<String> actual = Lists.newArrayList();
      final Iterator<KijiCell<Utf8>> it = mMapResult.iterator(STRING_MAP);
      while (it.hasNext()) {
        actual.add(it.next().getData().toString());
      }
      Assert.assertEquals(expected, actual);
    }
    {
      final List<String> expected = Lists.newArrayList(
          "sm1-ten", "sm1-five", "sm1-four", "sm1-three", "sm1-two", "sm1-one");
      final List<String> actual = Lists.newArrayList();
      final Iterator<KijiCell<Utf8>> it = mPagedMapResult.iterator(STRING_MAP_1);
      while (it.hasNext()) {
        actual.add(it.next().getData().toString());
      }
      Assert.assertEquals(expected, actual);
    }
    {
      final List<String> expected = Lists.newArrayList(
          "sm1-ten", "sm1-five", "sm1-four", "sm1-three", "sm1-two", "sm1-one",
          "sm2-ten", "sm2-five", "sm2-four", "sm2-three", "sm2-two", "sm2-one");
      final List<String> actual = Lists.newArrayList();
      final Iterator<KijiCell<Utf8>> it = mPagedMapResult.iterator(STRING_MAP);
      while (it.hasNext()) {
        actual.add(it.next().getData().toString());
      }
      Assert.assertEquals(expected, actual);
    }
  }

  @Test
  public void testIterable() {
    {
      final Set<String> expected = Sets.newHashSet(
          "sm1-ten", "sm1-five", "sm1-four", "sm1-three", "sm1-two", "sm1-one",
          "sm2-ten", "sm2-five", "sm2-four", "sm2-three", "sm2-two", "sm2-one",
          "10.0", "5.0", "4.0", "3.0", "2.0", "1.0",
          "ten", "five", "four", "three", "two", "one");
      final Set<String> actual = Sets.newHashSet();
      for (KijiCell<?> cell : mCompleteResult) {
        actual.add(cell.getData().toString());
      }
      Assert.assertEquals(expected, actual);
    }
    {
      final Set<String> expected = Sets.newHashSet(
          "sm1-ten", "sm1-five", "sm1-four", "sm1-three", "sm1-two", "sm1-one",
          "sm2-ten", "sm2-five", "sm2-four", "sm2-three", "sm2-two", "sm2-one",
          "10.0", "5.0", "4.0", "3.0", "2.0", "1.0",
          "ten", "five", "four", "three", "two", "one");
      final Set<String> actual = Sets.newHashSet();
      for (KijiCell<?> cell : mPagedCompleteResult) {
        actual.add(cell.getData().toString());
      }
      Assert.assertEquals(expected, actual);
    }
  }

  @Test
  public void testMaxVersions() {
    {
      final Set<String> expected = Sets.newHashSet(
          "10.0", "5.0", "4.0", "3.0", "2.0", "1.0",
          "sm1-ten", "sm1-five", "sm1-four",
          "sm2-ten", "sm2-five", "sm2-four",
          "ten", "five", "four");
      final Set<String> actual = Sets.newHashSet();
      for (KijiCell<?> cell : mMaxVersionsResult) {
        actual.add(cell.getData().toString());
      }
      Assert.assertEquals(expected, actual);
    }
    {
      final Set<String> expected = Sets.newHashSet(
          "sm1-ten", "sm1-five", "sm1-four");
      final Set<String> actual = Sets.newHashSet();
      final Iterator<KijiCell<Utf8>> it = mMaxVersionsResult.iterator(STRING_MAP_1);
      while (it.hasNext()) {
        actual.add(it.next().getData().toString());
      }
      Assert.assertEquals(expected, actual);
    }
    {
      final Set<String> expected = Sets.newHashSet(
          "sm1-ten", "sm1-five", "sm1-four",
          "sm2-ten", "sm2-five", "sm2-four");
      final Set<String> actual = Sets.newHashSet();
      final Iterator<KijiCell<Utf8>> it = mMaxVersionsResult.iterator(STRING_MAP);
      while (it.hasNext()) {
        actual.add(it.next().getData().toString());
      }
      Assert.assertEquals(expected, actual);
    }
    {
      final Set<String> expected = Sets.newHashSet(
          "10.0", "5.0", "4.0", "3.0", "2.0", "1.0",
          "sm1-ten", "sm1-five", "sm1-four",
          "sm2-ten", "sm2-five", "sm2-four",
          "ten", "five", "four");
      final Set<String> actual = Sets.newHashSet();
      for (KijiCell<?> cell : mPagedMaxVersionsResult) {
        actual.add(cell.getData().toString());
      }
      Assert.assertEquals(expected, actual);
    }
    {
      final Set<String> expected = Sets.newHashSet(
          "sm1-ten", "sm1-five", "sm1-four");
      final Set<String> actual = Sets.newHashSet();
      final Iterator<KijiCell<Utf8>> it = mPagedMaxVersionsResult.iterator(STRING_MAP_1);
      while (it.hasNext()) {
        actual.add(it.next().getData().toString());
      }
      Assert.assertEquals(expected, actual);
    }
    {
      final Set<String> expected = Sets.newHashSet(
          "sm1-ten", "sm1-five", "sm1-four",
          "sm2-ten", "sm2-five", "sm2-four");
      final Set<String> actual = Sets.newHashSet();
      final Iterator<KijiCell<Utf8>> it = mPagedMaxVersionsResult.iterator(STRING_MAP);
      while (it.hasNext()) {
        actual.add(it.next().getData().toString());
      }
      Assert.assertEquals(expected, actual);
    }

    Assert.assertNull(mMaxVersionsResult.getCell(STRING_MAP_1, 2));
  }

  @Test
  public void testUnrequestedColumns() {
    final KijiColumnName bogusColumn = new KijiColumnName("bogus", "bogus");
    final KijiColumnName bogusFamily = new KijiColumnName("bogus", null);

    try {
      mCompleteResult.getMostRecentCell(bogusColumn);
      Assert.fail();
    } catch (NullPointerException npe) {
      Assert.assertTrue(npe.getMessage().startsWith(EXPECTED_NO_REQUEST_EXCEPTION_PREFIX));
    }
    try {
      mCompleteResult.getCell(bogusColumn, 5);
      Assert.fail();
    } catch (NullPointerException npe) {
      Assert.assertTrue(npe.getMessage().startsWith(EXPECTED_NO_REQUEST_EXCEPTION_PREFIX));
    }
    try {
      mCompleteResult.iterator(bogusColumn);
      Assert.fail();
    } catch (NullPointerException npe) {
      Assert.assertTrue(npe.getMessage().startsWith(EXPECTED_NO_REQUEST_EXCEPTION_PREFIX));
    }
    try {
      mCompleteResult.iterator(bogusFamily);
      Assert.fail();
    } catch (NullPointerException npe) {
      Assert.assertTrue(npe.getMessage().startsWith(EXPECTED_NO_REQUEST_EXCEPTION_PREFIX));
    }
  }

  @Test
  public void testEmptyColumn() {
    {
      final KijiCell<Boolean> expected = null;
      final KijiCell<Boolean> actual = mEmptyColumnResult.getMostRecentCell(PRIMITIVE_BOOLEAN);
      Assert.assertEquals(expected, actual);
    }
    {
      final KijiCell<Boolean> expected = null;
      final KijiCell<Boolean> actual = mEmptyColumnResult.getCell(PRIMITIVE_BOOLEAN, 10);
      Assert.assertEquals(expected, actual);
    }
  }
}
