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

package org.kiji.schema;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.EnvironmentBuilder;

public class TestKijiRowScanner {
  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableReader mReader;

  @Before
  public void setupEnvironment() throws Exception {
    // TODO: Put this in a withInstance() method.
    final String instance = java.util.UUID.randomUUID().toString().replace('-', 'x');

    // Get the test table layouts.
    final KijiTableLayout layout = new KijiTableLayout(
        KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST), null);

    // Populate the environment.
    Map<String, Kiji> environment = new EnvironmentBuilder()
        .withInstance(instance)
            .withTable("user", layout)
                .withRow("foo")
                    .withFamily("info")
                        .withQualifier("name").withValue(1L, "foo-val")
                .withRow("bar")
                    .withFamily("info")
                        .withQualifier("name").withValue(1L, "bar-val")
        .build();

    // Fill local variables.
    mKiji = environment.get(instance);
    mTable = mKiji.openTable("user");
    mReader = mTable.openTableReader();
  }

  @After
  public void cleanupEnvironment() {
    IOUtils.closeQuietly(mReader);
    IOUtils.closeQuietly(mTable);
    IOUtils.closeQuietly(mKiji);
  }

  @Test
  public void testScanner() throws Exception {
    final KijiDataRequest request = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("info", "name"));
    final Iterator<KijiRowData> scanner = mReader.getScanner(request).iterator();

    final String actual1 = scanner.next().getValue("info", "name", 1L).toString();
    final String actual2 = scanner.next().getValue("info", "name", 1L).toString();

    assertEquals("bar-val", actual1);
    assertEquals("foo-val", actual2);
  }
}
