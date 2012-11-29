// (c) Copyright 2012 WibiData, Inc.

package org.kiji.schema.filter;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kiji.schema.HBaseScanOptions;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wibidata.core.FooTableIntegrationTest;
import com.wibidata.core.client.LocalWibiTableReader;

public class IntegrationTestStripValueRowFilter extends FooTableIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(
      IntegrationTestStripValueRowFilter.class);

  private LocalWibiTableReader mReader;

  @Before
  public void setupReader() throws IOException {
    mReader = new LocalWibiTableReader(getFooTable());
  }

  @After
  public void teardownReader() throws IOException {
    mReader.close();
  }

  /**
   * Verifies that values has been stripped if the StripValueRowFilter has been applied.
   */
  @Test(expected=IOException.class)
  public void testStripValuesFilter() throws IOException {
    KijiDataRequest dataRequest = new KijiDataRequest()
      .addColumn(new KijiDataRequest.Column("info", "name"));
    KijiRowFilter rowFilter = new StripValueRowFilter();

    KijiRowScanner rowScanner = mReader.getScanner(dataRequest, null, null, rowFilter,
      new HBaseScanOptions());
    for (KijiRowData rowData : rowScanner) {
      rowData.getStringValue("info", "name");
    }
    fail("Should have gotten an IOException in attempting to get filtered values.");
  }
}
