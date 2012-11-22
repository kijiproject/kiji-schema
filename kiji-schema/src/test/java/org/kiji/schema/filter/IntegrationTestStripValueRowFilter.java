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

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.HBaseScanOptions;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.testutil.FooTableIntegrationTest;


public class IntegrationTestStripValueRowFilter extends FooTableIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(
      IntegrationTestStripValueRowFilter.class);

  private KijiTableReader mReader;
  private KijiRowScanner mScanner;

  @Before
  public void setupReader() throws IOException {
    mReader = getFooTable().openTableReader();
    KijiDataRequest dataRequest = new KijiDataRequest()
      .addColumn(new KijiDataRequest.Column("info", "name"));
    KijiRowFilter rowFilter = new StripValueRowFilter();

    mScanner = mReader.getScanner(dataRequest, null, null, rowFilter,
      new HBaseScanOptions());
  }

  @After
  public void teardownReader() throws IOException {
    mScanner.close();
    mReader.close();
  }

  /**
   * Verifies that values has been stripped if the StripValueRowFilter has been applied.
   */
  @Test(expected=IOException.class)
  public void testStripValuesFilter() throws IOException {
    KijiRowScanner rowScanner = null;
    try {
      KijiDataRequest dataRequest = new KijiDataRequest()
          .addColumn(new KijiDataRequest.Column("info", "name"));
      KijiRowFilter rowFilter = new StripValueRowFilter();

      rowScanner = mReader.getScanner(dataRequest, null, null, rowFilter,
          new HBaseScanOptions());
      for (KijiRowData rowData : rowScanner) {
        rowData.getMostRecentValue("info", "name");
      }
    } finally {
      IOUtils.closeQuietly(rowScanner);

    }
    fail("Should have gotten an IOException in attempting to get filtered values.");
  }
}
