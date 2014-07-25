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

package org.kiji.schema;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.schema.util.ResourceUtils;

/**
 * Test of {@link org.kiji.schema.KijiRowData} for HBase Kiji scans.
 */
public abstract class KijiRowScannerTest extends KijiClientTest {

  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableReader mReader;

  public abstract KijiRowScanner getRowScanner(
      final KijiTable table,
      final KijiTableReader reader,
      final KijiDataRequest dataRequest
  ) throws IOException;

  @Before
  public final void setupEnvironment() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout = KijiTableLayout.newLayout(
        KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));

    // Populate the environment.
    mKiji = new InstanceBuilder(getKiji())
        .withTable("user", layout)
        .withRow("foo")
        .withFamily("info")
        .withQualifier("name").withValue(1L, "foo-val")
        .withRow("bar")
        .withFamily("info")
        .withQualifier("name").withValue(1L, "bar-val")
        .build();

    // Fill local variables.
    mTable = mKiji.openTable("user");
    mReader = mTable.openTableReader();
  }

  @After
  public final void cleanupEnvironment() throws IOException {
    mReader.close();
    mTable.release();
  }

  @Test
  public void testScanner() throws Exception {
    final KijiDataRequest request = KijiDataRequest.create("info", "name");
    final KijiRowScanner scanner = getRowScanner(mTable, mReader, request);
    final Iterator<KijiRowData> iterator = scanner.iterator();

    final String actual1 = iterator.next().getValue("info", "name", 1L).toString();
    final String actual2 = iterator.next().getValue("info", "name", 1L).toString();

    assertEquals("bar-val", actual1);
    assertEquals("foo-val", actual2);

    ResourceUtils.closeOrLog(scanner);
  }

  @Test
  public void testScannerOptionsStart() throws Exception {
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    final EntityId startRow = mTable.getEntityId("bar");
    final KijiRowScanner scanner = mReader.getScanner(
        request, new KijiScannerOptions().setStartRow(startRow));
    final Iterator<KijiRowData> iterator = scanner.iterator();

    final String first = iterator.next().getValue("info", "name", 1L).toString();
    assertEquals("bar-val", first);

    ResourceUtils.closeOrLog(scanner);
  }
}
