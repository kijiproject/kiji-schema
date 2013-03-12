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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.NavigableMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

/**
 * Ensure that column family requests are limited to map-type columns. This ensures ensures type
 * safety on getMethods for data from column families.
 */
public class TestRowDataColumnFamilyOps extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestRowDataColumnFamilyOps.class);
  private Kiji mKiji;

  @Before
  public void setupInstance() throws Exception {
   final KijiTableLayout layout =
      KijiTableLayout.newLayout(KijiTableLayouts.getLayout(
          KijiTableLayouts.TWO_COLUMN_DIFFERENT_TYPES));
    //
   mKiji = new InstanceBuilder()
        .withTable("table", layout)
            .withRow("row1")
               .withFamily("family")
                  .withQualifier("column1")
                    .withValue(1L, "I am a String")
                  .withQualifier("column2").withValue(1L, 1)
        .build();
  }

  @After
  public void teardownInstance() throws Exception {
    mKiji.release();
  }

  @Test(expected=IllegalStateException.class)
  public void testGetValues() throws IOException {
     final KijiTable table = mKiji.openTable("table");
     final KijiTableReader reader = table.openTableReader();
     final KijiRowData row1 = reader.get(table.getEntityId("row1"),
              KijiDataRequest.create("family"));
     assertTrue("Row does not contain column family [family].", row1.containsColumn("family"));
     assertTrue("Row does not contain column [family:column1].",
         row1.containsColumn("family", "column1"));
     assertTrue("Row does not contain column [family:column2].",
         row1.containsColumn("family", "column2"));
     final NavigableMap<String, NavigableMap<Long, CharSequence>> stringGet = row1.getValues(
         "family");
     final NavigableMap<String, NavigableMap<Long, Integer>> intGet = row1.getValues("family");
     assertEquals("I am not a string! () for the stringGet.", "I am a String",
         stringGet.get("column1").get(1L).toString());
     intGet.get("column1").get(1L);
  }

  @Test(expected=IllegalStateException.class)
  public void testGetMostRecentValues() throws IOException {
     final KijiTable table = mKiji.openTable("table");
     final KijiTableReader reader = table.openTableReader();
     final KijiRowData row1 = reader.get(table.getEntityId("row1"),
              KijiDataRequest.create("family"));
     assertTrue("Row does not contain column family [family].", row1.containsColumn("family"));
     assertTrue("Row does not contain column [family:column1].",
         row1.containsColumn("family", "column1"));
     assertTrue("Row does not contain column [family:column2].",
         row1.containsColumn("family", "column2"));
     final NavigableMap<String, CharSequence> stringGet =
         row1.getMostRecentValues("family");
     final NavigableMap<String, Integer> intGet = row1.getMostRecentValues("family");
     assertEquals("I am not a string! () for the stringGet.", "I am a String",
         stringGet.get("column1").toString());
     intGet.get("column1");
  }

  @Test(expected=IllegalStateException.class)
  public void testGetMostRecentCells() throws IOException {
     final KijiTable table = mKiji.openTable("table");
     final KijiTableReader reader = table.openTableReader();
     final KijiRowData row1 = reader.get(table.getEntityId("row1"),
              KijiDataRequest.create("family"));
     assertTrue("Row does not contain column family [family].", row1.containsColumn("family"));
     assertTrue("Row does not contain column [family:column1].",
         row1.containsColumn("family", "column1"));
     assertTrue("Row does not contain column [family:column2].",
         row1.containsColumn("family", "column2"));
     final NavigableMap<String, KijiCell<CharSequence>> stringGet =
         row1.getMostRecentCells("family");
     final NavigableMap<String, KijiCell<Integer>> intGet = row1.getMostRecentCells("family");
     assertEquals("I am not a string! () for the stringGet.", "I am a String",
         stringGet.get("column1").getData().toString());
     assertEquals("I am not a string! () for the intGet.", "I am a String", intGet.get("column1")
       .getData().toString());
  }

  @Test(expected=IllegalStateException.class)
  public void testGetCells() throws IOException {
     final KijiTable table = mKiji.openTable("table");
     final KijiTableReader reader = table.openTableReader();
     final KijiRowData row1 = reader.get(table.getEntityId("row1"),
              KijiDataRequest.create("family"));
     assertTrue("Row does not contain column family [family].", row1.containsColumn("family"));
     assertTrue("Row does not contain column [family:column1].",
         row1.containsColumn("family", "column1"));
     assertTrue("Row does not contain column [family:column2].",
         row1.containsColumn("family", "column2"));
     final NavigableMap<String, NavigableMap<Long, KijiCell<CharSequence>>> stringGet =
         row1.getCells("family");
     final NavigableMap<String, NavigableMap<Long, KijiCell<Integer>>> intGet =
         row1.getCells("family");
     assertEquals("I am not a string! () for the stringGet.", "I am a String",
         stringGet.get("column1").get(1L).getData().toString());
     assertEquals("I am not a string! () for the intGet.", "I am a String", intGet.get("column1")
       .get(1L).getData().toString());
  }

  @Test(expected=IllegalStateException.class)
  public void testGetPager() throws IOException {
     final KijiTable table = mKiji.openTable("table");
     final KijiTableReader reader = table.openTableReader();
     KijiDataRequestBuilder builder = KijiDataRequest.builder();
     builder.newColumnsDef().withMaxVersions(5).withPageSize(2).addFamily("family");
    final KijiDataRequest dataRequest = builder.build();
    final KijiRowData row1 = reader.get(table.getEntityId("row1"),
              dataRequest);
    KijiPager pager = row1.getPager("family");
  }
}
