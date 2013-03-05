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
import java.util.List;

import org.junit.Test;

import org.kiji.schema.layout.KijiTableLayouts;

public class TestKijiTable extends KijiClientTest {

  @Test
  public void testGetRegions() throws IOException {
    final int numRegions = 3;

    final Kiji mKiji = getKiji();
    mKiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.FULL_FEATURED), numRegions);
    final KijiTable mTable = mKiji.openTable("user");

    // Check there are the right number of regions.
    List<KijiRegion> regions = mTable.getRegions();
    assertEquals(numRegions, regions.size());

    // Check that all KijiRegions have location info.
    for (KijiRegion region : regions) {
      assertTrue(region.getLocations().size() > 0);
    }
  }
}
