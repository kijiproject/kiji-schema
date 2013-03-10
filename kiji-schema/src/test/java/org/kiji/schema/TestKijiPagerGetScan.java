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

package org.kiji.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestKijiPagerGetScan extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestKijiPagerGetScan.class);

  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  private KijiTable mTable;
  private KijiTableReader mTableReader;

  /**
   * Computes the timestamp in ms since the Epoch for a given UTC date (year, month, day).
   *
   * @param year Year.
   * @param month Month.
   * @param day Day.
   * @return the timestamp in ms since the Epoch for the given date.
   */
  private static long ts(int year, int month, int day) {
    final GregorianCalendar gc = new GregorianCalendar(year, month, day);
    gc.setTimeZone(UTC);
    return gc.getTimeInMillis();
  }

  @Before
  public final void setupTestKijiPager2() throws Exception {
    final TableLayoutDesc layout = KijiTableLayouts.getLayout(KijiTableLayouts.PAGING_TEST);

    final Kiji kiji = new InstanceBuilder(getKiji())
        .withTable(layout)
            .withRow("garrett")
                .withFamily("info")
                    .withQualifier("name").withValue("garrett")
                    .withQualifier("location")
                        .withValue(ts(1984, 9, 9), "Bothell, WA")
                        .withValue(ts(2002, 9, 20), "Seattle, WA")
                        .withValue(ts(2006, 10, 1), "San Jose, CA")
                        .withValue(ts(2008, 9, 1), "New York, NY")
                        .withValue(ts(2010, 10, 3), "San Francisco, CA")
                .withFamily("jobs")
                    .withQualifier("Papa Murphy's Pizza")
                        .withValue(ts(1999, 10, 10), "Pizza Maker")
                    .withQualifier("The Catalyst Group")
                        .withValue(ts(2004, 9, 10), "Software Developer")
                    .withQualifier("Google").withValue(ts(2006, 6, 26), "Software Engineer")
                    .withQualifier("WibiData").withValue(ts(2010, 10, 4), "MTS")
        .build();

    mTable = kiji.openTable("user");
    mTableReader = mTable.openTableReader();
  }

  @After
  public final void teardownTestKijiPager2() throws Exception {
    mTableReader.close();
    mTable.release();
  }


  @Test
  public void testGet() throws IOException {
    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(5).withPageSize(2)
            .add("info", "name")
            .add("info", "location"))
        .addColumns(ColumnsDef.create().withPageSize(2).addFamily("jobs"))
        .build();
    final KijiRowData input = mTableReader.get(mTable.getEntityId("garrett"), dataRequest);

    // Read the user name.
    if (!input.containsColumn("info", "name")) {
      return;
    }
    assertEquals("garrett", input.getMostRecentValue("info", "name").toString());

    // Page over location column.
    final List<CharSequence> locations = Lists.newArrayList();
    final KijiPager locationPager = input.getPager("info", "location");
    try {
      assertTrue("Our pager always has a first page.", locationPager.hasNext());
      final KijiRowData page1 = locationPager.next();
      locations.addAll(page1.<CharSequence>getValues("info", "location").values());
      LOG.debug("This size of our locations list is [{}].", locations.size());
      assertEquals(2, locations.size());

      // Read the second page of locations (2 cells).
      assertTrue("Our pager should have a second page.", locationPager.hasNext());
      final KijiRowData page2 = locationPager.next();
      locations.addAll(page2.<CharSequence>getValues("info", "location").values());
      LOG.debug("This size of our locations list is [{}].", locations.size());
      assertEquals(4, locations.size());

      // Read the last page of locations (1 cell).
      assertTrue("Our pager should have a third page", locationPager.hasNext());
      final KijiRowData page3 = locationPager.next();
      locations.addAll(page3.<CharSequence>getValues("info", "location").values());
      LOG.debug("This size of our locations list is [{}].", locations.size());
      assertEquals(5, locations.size());

      assertFalse("Our page should not have a fifth page.", locationPager.hasNext());
    } finally {
      locationPager.close();
    }

    // Page over jobs column.
    final KijiPager jobsPager = input.getPager("jobs");
    try {
      assertTrue("Our pagers always have a first page.", jobsPager.hasNext());
      final List<CharSequence> jobs = Lists.newArrayList();
      final KijiRowData page1 = jobsPager.next();
      for (Map.Entry<String, CharSequence> employment
               : page1.<CharSequence>getMostRecentValues("jobs").entrySet()) {
        jobs.add(employment.getValue().toString() + " @ " + employment.getKey());
      }
      assertEquals(2, jobs.size());

      // Read the second page of jobs.
      assertTrue(jobsPager.hasNext());
      final KijiRowData page2 = jobsPager.next();
      for (Map.Entry<String, CharSequence> employment
               : page2.<CharSequence>getMostRecentValues("jobs").entrySet()) {
        jobs.add(employment.getValue().toString() + " @ " + employment.getKey());
      }
      assertEquals(4, jobs.size());
      // We should try to get the next page
      assertTrue(jobsPager.hasNext());
      //But it should be empty.
      final KijiRowData page3 = jobsPager.next();
      assertTrue(page3.getValues("jobs").isEmpty());
    } finally {
      jobsPager.close();
    }
  }

  @Test
  public void testScan() throws IOException {
    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(5).add("info", "name"))
        .addColumns(ColumnsDef.create().withMaxVersions(5).withPageSize(2)
            .add("info", "location")
            .addFamily("jobs"))
        .build();
    final KijiRowData input = mTableReader.get(mTable.getEntityId("garrett"), dataRequest);

    // Read the user name.
    if (!input.containsColumn("info", "name")) {
      return;
    }
    assertEquals("garrett", input.getMostRecentValue("info", "name").toString());

    // Page over location column.
    final List<CharSequence> locations = Lists.newArrayList();
    final KijiPager locationPager = input.getPager("info", "location");
    try {
      assertTrue("Our pager always has a first page.", locationPager.hasNext());
      for (CharSequence location : locationPager.next().<CharSequence>getValues("info", "location")
          .values()) {
        locations.add(location);
      }
      LOG.debug("This size of our locations list is [{}].", locations.size());
      assertEquals(2, locations.size());

      // Read the second page of locations (2 cells).
      assertTrue("Our pager should have a second page.", locationPager.hasNext());
      for (CharSequence location : locationPager.next().<CharSequence>getValues("info", "location")
          .values()) {
        locations.add(location);
      }
      LOG.debug("This size of our locations list is [{}].", locations.size());
      assertEquals(4, locations.size());

      // Read the last page of locations (1 cell).
      assertTrue("Our pager should have a third page", locationPager.hasNext());
      for (CharSequence location : locationPager.next().<CharSequence>getValues("info", "location")
          .values()) {
        locations.add(location);
      }
      LOG.debug("This size of our locations list is [{}].", locations.size());
      assertEquals(5, locations.size());

      assertFalse("Our page should not have a fifth page.", locationPager.hasNext());
    } finally {
      locationPager.close();
    }

    // Page over jobs column.
    final KijiPager jobsPager = input.getPager("jobs");
    try {
      assertTrue("Our pagers always have a first page.", jobsPager.hasNext());
      final List<CharSequence> jobs = Lists.newArrayList();
      final KijiRowData page1 = jobsPager.next();
      for (Map.Entry<String, CharSequence> employment
               : page1.<CharSequence>getMostRecentValues("jobs").entrySet()) {
        jobs.add(employment.getValue().toString() + " @ " + employment.getKey());
      }
      assertEquals(2, jobs.size());

      // Read the second page of jobs.
      assertTrue(jobsPager.hasNext());
      final KijiRowData page2 = jobsPager.next();
      for (Map.Entry<String, CharSequence> employment
               : page2.<CharSequence>getMostRecentValues("jobs").entrySet()) {
        jobs.add(employment.getValue().toString() + " @ " + employment.getKey());
      }
      assertEquals(4, jobs.size());
      // We should try to get the next page
      assertTrue(jobsPager.hasNext());
      //But it should be empty.
      final KijiRowData page3 = jobsPager.next();
      assertTrue(page3.getValues("jobs").isEmpty());
    } finally {
      jobsPager.close();
    }
  }
}
