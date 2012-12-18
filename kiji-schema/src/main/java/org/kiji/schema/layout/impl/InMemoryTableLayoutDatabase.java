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

package org.kiji.schema.layout.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.avro.MetadataBackup;
import org.kiji.schema.avro.TableBackup;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayoutDatabase;
import org.kiji.schema.util.Clock;
import org.kiji.schema.util.TimestampComparator;

/**
 * An in-memory database of Kiji Table Layouts.
 *
 * <p>This class is thread-safe.</p>
 */
@ApiAudience.Private
public final class InMemoryTableLayoutDatabase implements KijiTableLayoutDatabase {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryTableLayoutDatabase.class);

  /** A clock. */
  private final Clock mClock;

  /** Map from table names to versioned table layouts. */
  private final Map<String, NavigableMap<Long, KijiTableLayout>> mLayouts;

  /**
   * Creates a new <code>InMemoryTableLayoutDatabase</code> instance.
   */
  public InMemoryTableLayoutDatabase() {
    this(Clock.getDefaultClock());
  }

  /**
   * Creates a new <code>InMemoryTableLayoutDatabase</code> instance.
   *
   * @param clock A clock.
   */
  public InMemoryTableLayoutDatabase(Clock clock) {
    mClock = clock;
    mLayouts = new HashMap<String, NavigableMap<Long, KijiTableLayout>>();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized KijiTableLayout updateTableLayout(String table, TableLayoutDesc update)
      throws IOException {
    final List<KijiTableLayout> layouts = getTableLayoutVersions(table, 1);
    final KijiTableLayout currentLayout = layouts.isEmpty() ? null : layouts.get(0);
    final KijiTableLayout tableLayout = new KijiTableLayout(update, currentLayout);
    if (!mLayouts.containsKey(table)) {
      mLayouts.put(table, new TreeMap<Long, KijiTableLayout>(TimestampComparator.INSTANCE));
    }
    mLayouts.get(table).put(mClock.getTime(), tableLayout);
    return tableLayout;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized KijiTableLayout getTableLayout(String table) throws IOException {
    if (!mLayouts.containsKey(table)) {
      throw new KijiTableNotFoundException(table);
    }
    return mLayouts.get(table).firstEntry().getValue();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized List<KijiTableLayout> getTableLayoutVersions(String table, int numVersions)
      throws IOException {
    if (numVersions < 1) {
      throw new IllegalArgumentException("numVersions must be positive");
    }

    Map<Long, KijiTableLayout> allVersions = mLayouts.get(table);
    if (null == allVersions) {
      LOG.debug("No prior table layouts found for table " + table);
      return Collections.emptyList();
    }

    List<KijiTableLayout> recentVersions = new ArrayList<KijiTableLayout>();
    List<KijiTableLayout> versions = new ArrayList<KijiTableLayout>(allVersions.values());
    LOG.debug("Found " + versions.size() + " prior version(s) for table " + table);
    for (int i = 0; i < Math.min(versions.size(), numVersions); i++) {
      recentVersions.add(versions.get(i));
    }
    return recentVersions;
  }

  /** {@inheritDoc} */
  @Override
  public NavigableMap<Long, KijiTableLayout> getTimedTableLayoutVersions(String table,
          int numVersions) throws IOException {
    if (numVersions < 1) {
      throw new IllegalArgumentException("numVersions must be positive");
    }

    NavigableMap<Long, KijiTableLayout> allLayouts = mLayouts.get(table);
    if (null == allLayouts) {
      LOG.debug("No prior table layouts found for table " + table);
      return new TreeMap<Long, KijiTableLayout>();
    } else {
      NavigableSet<Long> timestamps = allLayouts.navigableKeySet();
      Iterator<Long> timeIterator = timestamps.iterator();
      long mostRecentTimeStamp = timeIterator.next();
      long leastRecentTimeStamp = mostRecentTimeStamp;
      for (int i = 1; i < Math.min(numVersions, timestamps.size()); i++) {
        leastRecentTimeStamp = timeIterator.next();
      }
      return allLayouts.subMap(mostRecentTimeStamp, true, leastRecentTimeStamp, true);
    }
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void removeAllTableLayoutVersions(String table) throws IOException {
    mLayouts.remove(table);
  }

  /** {@inheritDoc} */
  @Override
  public void removeRecentTableLayoutVersions(String table, int numVersions) throws IOException {
    if (numVersions < 1) {
      throw new IllegalArgumentException("numVersions must be positive");
    }
    for (int i = 0; i < Math.min(numVersions, mLayouts.get(table).size()); i++) {
      mLayouts.get(table).pollFirstEntry();
    }
  }

  /** {@inheritDoc} */
  @Override
  public List<String> listTables() throws IOException {
    return Collections.unmodifiableList(new ArrayList<String>(mLayouts.keySet()));
  }

  /** {@inheritDoc} */
  @Override
  public void writeToBackup(MetadataBackup.Builder backup) throws IOException {
    throw new RuntimeException("Not implemented");
  }

  /** {@inheritDoc} */
  @Override
  public void restoreFromBackup(MetadataBackup backup) throws IOException {
    throw new RuntimeException("Not implemented");
  }

  /** {@inheritDoc} */
  @Override
  public void restoreTableFromBackup(TableBackup tableBackup) throws IOException {
    throw new RuntimeException("Not implemented");
  }
}
