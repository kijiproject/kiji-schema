// (c) Copyright 2011 WibiData, Inc.

package org.kiji.schema;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.wibidata.core.WibiIntegrationTest;
import com.wibidata.core.license.LicenseTestObjects;
import com.wibidata.core.license.SignedLicense;

public class IntegrationTestKijiSystemTable extends WibiIntegrationTest {
  private Kiji mKiji;
  private static final String KEY = "some.system.property";
  private static final byte[] VALUE = Bytes.toBytes("property value I want to store.");

  @Before
  public void setup() throws IOException {
    mKiji = new Kiji(getKijiConfiguration());
  }

  @After
  public void teardown() throws IOException {
    mKiji.close(); // closes derived system table.
  }

  @Test
  public void testStoreVersion() throws IOException {
    KijiSystemTable systemTable = mKiji.getSystemTable();
    systemTable.setDataVersion("99");
    assertEquals("99", systemTable.getDataVersion());
  }

  @Test

  public void testPutGet() throws IOException {
    KijiSystemTable systemTable = mKiji.getSystemTable();
    SignedLicense license = LicenseTestObjects.getMockLicense();
    systemTable.putValue(KEY, VALUE);
    byte[] bytesBack = systemTable.getValue(KEY);
    assertArrayEquals(VALUE, bytesBack);
  }
}
