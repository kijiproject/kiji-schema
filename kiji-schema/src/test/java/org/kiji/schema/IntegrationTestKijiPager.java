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
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;

//TODO: Merge with TestKijiPager
public class IntegrationTestKijiPager extends AbstractKijiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestKijiPager.class);

  private Kiji mKiji;
  private KijiTable mUserTable;
  private KijiTableReader mTableReader;
  private EntityId mGarrettId;
  private EntityId mJulietId;

  @Before
  public final void setup() throws Exception {
    mKiji = Kiji.Factory.open(getKijiURI());
    mKiji.createTable("user", KijiTableLayouts.getTableLayout(KijiTableLayouts.PAGING_TEST));
    mUserTable = mKiji.openTable("user");
    mTableReader = mUserTable.openTableReader();
    mGarrettId =  mUserTable.getEntityId("garrett");
    mJulietId =  mUserTable.getEntityId("juliet");

    final KijiTableWriter writer = mUserTable.openTableWriter();
    try {
      writer.put(mGarrettId, "info", "name", "garrett");

      writer.put(mGarrettId, "info", "location",
          new GregorianCalendar(1984, 9, 9).getTime().getTime(),
          "Bothell, WA");
      writer.put(mGarrettId, "info", "location",
          new GregorianCalendar(2002, 9, 20).getTime().getTime(),
          "Seattle, WA");
      writer.put(mGarrettId, "info", "location",
          new GregorianCalendar(2006, 10, 1).getTime().getTime(),
          "San Jose, CA");
      writer.put(mGarrettId, "info", "location",
          new GregorianCalendar(2008, 9, 1).getTime().getTime(),
          "New York, NY");
      writer.put(mGarrettId, "info", "location",
          new GregorianCalendar(2010, 10, 3).getTime().getTime(),
          "San Francisco, CA");

      writer.put(mGarrettId, "jobs", "Papa Murphy's Pizza",
          new GregorianCalendar(1999, 10, 10).getTime().getTime(),
          "Pizza Maker");
      writer.put(mGarrettId, "jobs", "The Catalyst Group",
          new GregorianCalendar(2004, 9, 10).getTime().getTime(),
          "Software Developer");
      writer.put(mGarrettId, "jobs", "Google",
          new GregorianCalendar(2006, 6, 26).getTime().getTime(),
          "Software Engineer");
      writer.put(mGarrettId, "jobs", "WibiData",
          new GregorianCalendar(2010, 10, 4).getTime().getTime(),
          "MTS");
    } finally {
      writer.close();
    }
  }

  @After
  public void teardown() throws Exception {
    mTableReader.close();
    mUserTable.close();
    mKiji.release();
  }


  @Test
  public void testGet() throws IOException {
      KijiDataRequestBuilder builder = KijiDataRequest.builder();
      builder.newColumnsDef().withMaxVersions(5).withPageSize(2)
          .add("info", "name")
          .add("info", "location");
      builder.newColumnsDef().withPageSize(2).addFamily("jobs");
      KijiDataRequest dataRequest = builder.build();
      KijiRowData input = mTableReader.get(mGarrettId, dataRequest);

      // Read the user name.
      if (!input.containsColumn("info", "name")) {
        return;
      }
      CharSequence name = input.getMostRecentValue("info", "name");

      // Page over location column.
      List<CharSequence> locations = new ArrayList<CharSequence>();
      KijiPager locationPager = input.getPager("info", "location");
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

      // Page over jobs column.
      KijiPager jobsPager = input.getPager("jobs");
      assertTrue("Our pagers always have a first page.", jobsPager.hasNext());
      List<CharSequence> jobs = new ArrayList<CharSequence>();
      for (Map.Entry<String, CharSequence> employment
               : jobsPager.next().<CharSequence>getMostRecentValues("jobs").entrySet()) {
        jobs.add(employment.getValue().toString() + " @ " + employment.getKey());
      }
      assertEquals(2, jobs.size());

      // Read the second page of jobs.
      assertTrue(jobsPager.hasNext());
      for (Map.Entry<String, CharSequence> employment
               : jobsPager.next().<CharSequence>getMostRecentValues("jobs").entrySet()) {
        jobs.add(employment.getValue().toString() + " @ " + employment.getKey());
      }
      assertEquals(4, jobs.size());
      // We should try to get the next page
      assertTrue(jobsPager.hasNext());
      //But it should be empty.
      assertTrue(jobsPager.next().getValues("jobs").isEmpty());
  }

  @Test
  public void testScan() throws IOException {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(5).add("info", "name");
    builder.newColumnsDef().withMaxVersions(5).withPageSize(2)
        .add("info", "location").addFamily("jobs");
    KijiDataRequest dataRequest = builder.build();
    KijiRowScanner scanner = mTableReader.getScanner(dataRequest);
    try {
    Iterator<KijiRowData> rowDataIterator = scanner.iterator();
    assertTrue(rowDataIterator.hasNext());
    KijiRowData garrettInput = rowDataIterator.next();
    CharSequence name = garrettInput.getMostRecentValue("info", "name");

    // Page over location column.
    List<CharSequence> locations = new ArrayList<CharSequence>();
    KijiPager locationPager = garrettInput.getPager("info", "location");
    assertTrue("Our pager always has a first page.", locationPager.hasNext());
    for (CharSequence location : locationPager.next()
      .<CharSequence>getValues("info", "location").values()) {
      locations.add(location);
    }
    LOG.debug("The size of our locations list is [{}].", locations.size());
    assertEquals(2, locations.size());

    // Read the second page of locations (2 cells).
    assertTrue("Our pager should have a second page.", locationPager.hasNext());
    for (CharSequence location : locationPager.next()
      .<CharSequence>getValues("info", "location").values()) {
      LOG.debug("Adding location [{}].", location);
      locations.add(location);
    }
    LOG.debug("The size of our locations list is [{}].", locations.size());
    assertEquals(4, locations.size());

    // Read the last page of locations (1 cell).
    assertTrue("Our pager should have a third page", locationPager.hasNext());
    for (CharSequence location : locationPager.next()
      .<CharSequence>getValues("info", "location").values()) {
      locations.add(location);
    }
    LOG.debug("This size of our locations list is [{}].", locations.size());
    assertEquals(5, locations.size());

    assertFalse("Our page should not have a fifth page.", locationPager.hasNext());

    // Page over jobs column.
    KijiPager jobsPager = garrettInput.getPager("jobs");
    assertTrue("Our pagers always have a first page.", jobsPager.hasNext());
    List<CharSequence> jobs = new ArrayList<CharSequence>();
    for (Map.Entry<String, CharSequence> employment : jobsPager.next()
      .<CharSequence>getMostRecentValues("jobs").entrySet()) {
      jobs.add(employment.getValue().toString() + " @ " + employment.getKey());
    }
    assertEquals(2, jobs.size());

    // Read the second page of jobs.
    assertTrue(jobsPager.hasNext());
    for (Map.Entry<String, CharSequence> employment : jobsPager.next()
      .<CharSequence>getMostRecentValues("jobs").entrySet()) {
      jobs.add(employment.getValue().toString() + " @ " + employment.getKey());
    }
    assertEquals(4, jobs.size());
    // We should try to get the next page
    assertTrue(jobsPager.hasNext());
    //But it should be empty.
    assertTrue(jobsPager.next().getValues("jobs").isEmpty());
    } finally {
      scanner.close();
    }
  }
}
