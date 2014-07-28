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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Iterator;
import java.util.NavigableMap;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

/**
 * Ensure that column family requests are limited to map-type columns. This ensures type safety
 * on getMethods for data from column families.
 */
public class TestRowDataColumnFamilyOps extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestRowDataColumnFamilyOps.class);
  private KijiRowData mRow1;

  @Before
  public void setupInstance() throws Exception {
   final KijiTableLayout layout =
      KijiTableLayout.newLayout(KijiTableLayouts.getLayout(
          KijiTableLayouts.TWO_COLUMN_DIFFERENT_TYPES));
   new InstanceBuilder(getKiji())
        .withTable("table", layout)
            .withRow("row1")
               .withFamily("family")
                  .withQualifier("column1")
                    .withValue(1L, "I am a String")
                  .withQualifier("column2").withValue(1L, 1)
        .build();
     final KijiTable table = getKiji().openTable("table");
     try {
       final KijiTableReader reader = table.openTableReader();
       try {
         mRow1 = reader.get(table.getEntityId("row1"),
                  KijiDataRequest.create("family"));
       } finally {
         reader.close();
       }
     } finally {
       table.release();
     }
  }

  @Test
  public void testGetValues() throws IOException {
     assertTrue("Row does not contain column family [family].", mRow1.containsColumn("family"));
     assertTrue("Row does not contain column [family:column1].",
         mRow1.containsColumn("family", "column1"));
     assertTrue("Row does not contain column [family:column2].",
         mRow1.containsColumn("family", "column2"));
     try {
       final NavigableMap<String, NavigableMap<Long, CharSequence>> stringGet =
           mRow1.getValues("family");
       final NavigableMap<String, NavigableMap<Long, Integer>> intGet =
           mRow1.getValues("family");
       assertEquals(
           "I am not a string! () for the stringGet.",
           "I am a String",
           stringGet.get("column1").get(1L).toString());
       intGet.get("column1").get(1L);
       fail("Didn't throw an exception!");
     } catch (IllegalStateException ex) {
       assertEquals("Incorrect Exception message: ", "getValues(String family) is only enabled on"
         + " map type column families. The column family [family], is a group type column family."
         + " Please use the getValues(String family, String qualifier) method.", ex.getMessage());
     }
  }

  @Test
  public void testGetMostRecentValues() throws IOException {
     assertTrue("Row does not contain column family [family].", mRow1.containsColumn("family"));
     assertTrue("Row does not contain column [family:column1].",
         mRow1.containsColumn("family", "column1"));
     assertTrue("Row does not contain column [family:column2].",
         mRow1.containsColumn("family", "column2"));
     try {
     final NavigableMap<String, CharSequence> stringGet =
         mRow1.getMostRecentValues("family");
     final NavigableMap<String, Integer> intGet = mRow1.getMostRecentValues("family");
     assertEquals("I am not a string! () for the stringGet.", "I am a String",
         stringGet.get("column1").toString());
       intGet.get("column1");
       fail("Didn't throw an exception!");
     } catch (IllegalStateException ex) {
       assertEquals("Incorrect Exception message: ", "getMostRecentValues(String family) is only "
         + "enabled on map type column families. The column family [family], is a group type column"
         + " family. Please use the getMostRecentValues(String family, String qualifier) method.",
         ex.getMessage());
     }
  }

  @Test
  public void testGetMostRecentCells() throws IOException {
     assertTrue("Row does not contain column family [family].", mRow1.containsColumn("family"));
     assertTrue("Row does not contain column [family:column1].",
         mRow1.containsColumn("family", "column1"));
     assertTrue("Row does not contain column [family:column2].",
         mRow1.containsColumn("family", "column2"));
     try {
     final NavigableMap<String, KijiCell<CharSequence>> stringGet =
         mRow1.getMostRecentCells("family");
     final NavigableMap<String, KijiCell<Integer>> intGet = mRow1.getMostRecentCells("family");
     assertEquals("I am not a string! () for the stringGet.", "I am a String",
         stringGet.get("column1").getData().toString());
       intGet.get("column1").getData();
       fail("Didn't throw an exception!");
     } catch (IllegalStateException ex) {
       assertEquals("Incorrect Exception message: ", "getMostRecentCells(String family) is only "
         + "enabled on map type column families. The column family [family], is a group type column"
         + " family. Please use the getMostRecentCells(String family, String qualifier) method.",
         ex.getMessage());
     }
  }

  @Test
  public void testGetCells() throws IOException {
     assertTrue("Row does not contain column family [family].", mRow1.containsColumn("family"));
     assertTrue("Row does not contain column [family:column1].",
         mRow1.containsColumn("family", "column1"));
     assertTrue("Row does not contain column [family:column2].",
         mRow1.containsColumn("family", "column2"));
     try {
     final NavigableMap<String, NavigableMap<Long, KijiCell<CharSequence>>> stringGet =
         mRow1.getCells("family");
     final NavigableMap<String, NavigableMap<Long, KijiCell<Integer>>> intGet =
         mRow1.getCells("family");
     assertEquals("I am not a string! () for the stringGet.", "I am a String",
         stringGet.get("column1").get(1L).getData().toString());
       intGet.get("column1").get(1L).getData();
       fail("Didn't throw an exception!");
     } catch (IllegalStateException ex) {
       assertEquals("Incorrect Exception message: ", "getCells(String family) is only "
         + "enabled on map type column families. The column family [family], is a group type column"
         + " family. Please use the getCells(String family, String qualifier) method.",
         ex.getMessage());
     }
  }

  @Test
  public void testIterator() throws IOException {
     assertTrue("Row does not contain column family [family].", mRow1.containsColumn("family"));
     assertTrue("Row does not contain column [family:column1].",
         mRow1.containsColumn("family", "column1"));
     assertTrue("Row does not contain column [family:column2].",
         mRow1.containsColumn("family", "column2"));
     try {
       final Iterator<KijiCell<CharSequence>> stringGet = mRow1.iterator("family");
       fail("Didn't throw an exception!");
     } catch (IllegalStateException ex) {
       assertEquals("Incorrect Exception message: ", "iterator(String family) is only "
         + "enabled on map type column families. The column family [family], is a group type column"
         + " family. Please use the iterator(String family, String qualifier) method.",
         ex.getMessage());
     }
  }

  @Test
  public void testGetPager() throws IOException {
    final KijiTable table = getKiji().openTable("table");
    try {
      final KijiTableReader reader = table.openTableReader();
      try {
        KijiDataRequestBuilder builder = KijiDataRequest.builder();
        builder.newColumnsDef().withMaxVersions(5).withPageSize(2).addFamily("family");
        final KijiDataRequest dataRequest = builder.build();
        final KijiRowData row1 = reader.get(table.getEntityId("row1"),
                  dataRequest);
        try {
          KijiPager pager = row1.getPager("family");
          pager.close();
          fail("Didn't throw an exception!");
        } catch (IllegalStateException ex) {
          assertEquals("getPager(String family) is only enabled on map type column families. "
            + "The column family 'family' is a group type column family. "
            + "Please use the getPager(String family, String qualifier) method.",
            ex.getMessage());
        }
      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }
}
