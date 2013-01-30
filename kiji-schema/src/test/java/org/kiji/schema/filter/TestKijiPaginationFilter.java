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

package org.kiji.schema.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.impl.HashedEntityId;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestKijiPaginationFilter extends KijiClientTest {
   private static final Logger LOG = LoggerFactory.getLogger(TestKijiPaginationFilter.class);
   private KijiTableReader mReader;
   private KijiTableLayout mTableLayout;
   private KijiTable mTable;

  @Before
  public void setupInstance() throws Exception {

    final Kiji kiji = getKiji();
    mTableLayout =
        new KijiTableLayout(KijiTableLayouts.getLayout(KijiTableLayouts.PAGING_TEST), null);
    kiji.createTable("user", mTableLayout);

    mTable = kiji.openTable("user");
    mReader = mTable.openTableReader();
  }

  @Test
  public void testGroupTypeColumnPaging() throws IOException {
    EntityId id = mTable.getEntityId("me");
   final KijiTableWriter writer = mTable.openTableWriter();
      writer.put(id, "info", "name", 1L, "me");
      writer.put(id, "info", "name", 2L, "me-too");
      writer.put(id, "info", "name", 3L, "me-three");
      writer.put(id, "info", "name", 4L, "me-four");
      writer.put(id, "info", "name", 5L, "me-five");
      writer.close();
    final KijiColumnFilter columnFilter = new KijiPaginationFilter(2, 1);
    final KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.addColumns().withMaxVersions(5).withFilter(columnFilter).add("info", "name");
    final KijiDataRequest dataRequest = builder.build();
    EntityId meId = HashedEntityId.fromKijiRowKey(
        Bytes.toBytes("me"), mTableLayout.getDesc().getKeysFormat());
    KijiRowData myRowData = mReader.get(meId, dataRequest);
    final NavigableMap<Long, CharSequence> resultMap = myRowData.getValues("info", "name");
    assertEquals("The number of returned values is incorrect:", 2, resultMap.size());
    assertTrue(null != resultMap.get(4L));
    assertEquals("me-four", resultMap.get(4L).toString());
    assertTrue(null != resultMap.get(3L));
    assertEquals("me-three", resultMap.get(3L).toString());
  }

  @Test
  public void testGroupTypeColumnPaging2() throws IOException {
    EntityId id = mTable.getEntityId("me");
   final KijiTableWriter writer = mTable.openTableWriter();
      writer.put(id, "info", "name", 1L, "me");
      writer.put(id, "info", "name", 2L, "me-too");
      writer.put(id, "info", "name", 3L, "me-three");
      writer.put(id, "info", "name", 4L, "me-four");
      writer.put(id, "info", "name", 5L, "me-five");
      writer.close();
    final KijiColumnFilter columnFilter = new KijiPaginationFilter(2, 0);
    final KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.addColumns().withMaxVersions(5).withFilter(columnFilter).add("info", "name");
    final KijiDataRequest dataRequest = builder.build();
    EntityId meId = HashedEntityId.fromKijiRowKey(
        Bytes.toBytes("me"), mTableLayout.getDesc().getKeysFormat());
    KijiRowData myRowData = mReader.get(meId, dataRequest);
    final NavigableMap<Long, CharSequence> resultMap = myRowData.getValues("info", "name");
    assertEquals("The number of returned values is incorrect:", 2, resultMap.size());
    assertTrue(null != resultMap.get(5L));
    assertEquals("me-five", resultMap.get(5L).toString());
    assertTrue(null != resultMap.get(4L));
    assertEquals("me-four", resultMap.get(4L).toString());
  }

  @Test
  public void testMapTypeColumnPaging() throws IOException {
   final KijiTableWriter writer = mTable.openTableWriter();
     EntityId id = mTable.getEntityId("me");
      writer.put(id, "jobs", "e", 1L, "always coming in 5th");
      writer.put(id, "jobs", "d", 2L, "always coming in 4th");
      writer.put(id, "jobs", "c", 3L, "always coming in 3rd");
      writer.put(id, "jobs", "b", 4L, "always coming in 2nd");
      writer.put(id, "jobs", "a", 5L, "always coming in 1st");
      writer.close();
    final KijiColumnFilter columnFilter = new KijiPaginationFilter(2, 1);
    final KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.addColumns().withMaxVersions(5).withFilter(columnFilter).addFamily("jobs");
    final KijiDataRequest dataRequest = builder.build();
    EntityId meId = HashedEntityId.fromKijiRowKey(
        Bytes.toBytes("me"), mTableLayout.getDesc().getKeysFormat());
    KijiRowData myRowData = mReader.get(meId, dataRequest);
    final NavigableMap<String, NavigableMap<Long, CharSequence>> resultMap
        = myRowData.<CharSequence>getValues("jobs");
    assertEquals("The number of returned values is incorrect:", 2, resultMap.size());
    assertTrue(null != resultMap.get("b"));
    assertEquals("always coming in 2nd", resultMap.get("b").get(4L).toString());
    assertTrue(null != resultMap.get("c"));
    assertEquals("always coming in 3rd", resultMap.get("c").get(3L).toString());
  }

    @Test
  public void testFilterMergeColumnPaging() throws IOException {
   final KijiTableWriter writer = mTable.openTableWriter();
     EntityId id = mTable.getEntityId("me");
      writer.put(id, "jobs", "b", 1L, "always coming in 5th");
      writer.put(id, "jobs", "b", 2L, "always coming in 4th");
      writer.put(id, "jobs", "b", 3L, "always coming in 3rd");
      writer.put(id, "jobs", "a", 4L, "always coming in 2nd");
      writer.put(id, "jobs", "a", 5L, "always coming in 1st");
      writer.close();
    final KijiColumnFilter columnFilter = new KijiPaginationFilter(2, 0,
        new RegexQualifierColumnFilter("b"));
    final KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.addColumns().withMaxVersions(5).withFilter(columnFilter).addFamily("jobs");
    final KijiDataRequest dataRequest = builder.build();
    EntityId meId = HashedEntityId.fromKijiRowKey(
        Bytes.toBytes("me"), mTableLayout.getDesc().getKeysFormat());
    KijiRowData myRowData = mReader.get(meId, dataRequest);
    final NavigableMap<String, NavigableMap<Long, CharSequence>> resultMap
        = myRowData.<CharSequence>getValues("jobs");
    assertEquals("The number of returned values is incorrect: ", 2, resultMap.get("b").size());
    assertEquals("Incorrect first value of first page:", "always coming in 3rd",
        resultMap.get("b").get(3L).toString());
    assertEquals("Incorrect second value of first page:", "always coming in 4th",
        resultMap.get("b").get(2L).toString());
  }

  @After
  public void cleaup() throws IOException {
    mReader.close();
  }
}
