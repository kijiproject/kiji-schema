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

package org.kiji.schema.layout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;

import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.impl.InMemoryTableLayoutDatabase;
import org.kiji.schema.util.Clock;
import org.kiji.schema.util.IncrementingClock;

public class TestInMemoryTableLayoutDatabase {
  private Clock mClock;
  private InMemoryTableLayoutDatabase mDb;

  @Before
  public void setup() {
    mClock = new IncrementingClock(1L);
    mDb = new InMemoryTableLayoutDatabase(mClock);
  }

  @Test(expected=KijiTableNotFoundException.class)
  public void testNotFound() throws IOException {
    mDb.getTableLayout("doesn't-exist");
  }

  @Test(expected=IllegalArgumentException.class)
  public void testZeroVersions() throws IOException {
    mDb.getTableLayoutVersions("foo", 0);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testNegativeVersions() throws IOException {
    mDb.getTableLayoutVersions("foo", -1);
  }

  @Test
  public void testDb() throws Exception {
    final TableLayoutDesc layoutDesc = KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE);
    final KijiTableLayout layout = new KijiTableLayout(layoutDesc, null);

    mDb.updateTableLayout(layout.getName(), layoutDesc);
    assertEquals(layout, mDb.getTableLayout(layout.getName()));

    List<KijiTableLayout> layouts =
        mDb.getTableLayoutVersions(layout.getName(), 3);
    assertEquals(1, layouts.size());
    assertEquals(layout, layouts.get(0));

    final TableLayoutDesc layout2Desc = KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE);
    layout2Desc.setDescription("version 2");
    final KijiTableLayout layout2 = new KijiTableLayout(layout2Desc, layout);
    mDb.updateTableLayout(layout2.getName(), layout2Desc);

    assertEquals(layout2, mDb.getTableLayout(layout2.getName()));

    layouts = mDb.getTableLayoutVersions(layout.getName(), 3);
    assertEquals(2, layouts.size());
    assertEquals(layout2, layouts.get(0));
    assertEquals(layout, layouts.get(1));

    assertEquals(1, mDb.getTableLayoutVersions(layout.getName(), 1).size());

    final TableLayoutDesc layout3Desc = KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE);
    layout3Desc.setDescription("version 3");
    final KijiTableLayout layout3 = new KijiTableLayout(layout3Desc, layout2);
    mDb.updateTableLayout(layout3.getName(), layout3Desc);

    final NavigableMap<Long, KijiTableLayout> timedLayouts =
        mDb.getTimedTableLayoutVersions(layout.getName(), 2);
    assertEquals(2, timedLayouts.size());
    assertEquals(layout3, timedLayouts.get(timedLayouts.firstKey()));
    assertEquals(layout2, timedLayouts.get(timedLayouts.lastKey()));

    // List tables.
    final List<String> tableNames = mDb.listTables();
    assertNotNull(tableNames);
    assertEquals(1, tableNames.size());
    assertEquals(layout.getName(), tableNames.get(0));

    // Delete the last two layouts for the table.
    mDb.removeRecentTableLayoutVersions(layout.getName(), 2);
    final List<KijiTableLayout> remainingLayouts =
        mDb.getTableLayoutVersions(layout.getName(), 3);
    assertEquals(1, remainingLayouts.size());
    assertEquals(layout, remainingLayouts.get(0));

    // Delete all layout information for the table.
    mDb.removeAllTableLayoutVersions(layout.getName());

    // No more layout data.
    assertTrue(mDb.getTableLayoutVersions(layout.getName(), 1).isEmpty());
    assertTrue(mDb.getTimedTableLayoutVersions(layout.getName(), 1).isEmpty());

    // No more tables.
    assertTrue(mDb.listTables().isEmpty());
  }
}
