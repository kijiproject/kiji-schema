// (c) Copyright 2012 WibiData, Inc.

package org.kiji.schema;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;
import org.kiji.schema.impl.InMemorySystemTable;
import org.kiji.schema.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wibidata.core.license.LicenseTestObjects;
import com.wibidata.core.license.SignedLicense;

public class TestInMemorySystemTable {
  private static final Logger LOG = LoggerFactory.getLogger(TestInMemorySystemTable.class);

  @Test
  public void testDataVersion() throws IOException {
    LOG.info("Constructing a system table");
    KijiSystemTable systemTable = new InMemorySystemTable();

    LOG.info("Verifying the data version has been set to the default client version");
    String expectedDataVersion = VersionInfo.getClientDataVersion();
    assertEquals(expectedDataVersion, systemTable.getDataVersion());

    LOG.info("Setting the data version");
    systemTable.setDataVersion("fooVersion");

    LOG.info("Verifying data version was set");
    assertEquals("fooVersion", systemTable.getDataVersion());

    LOG.info("Closing system table");
    systemTable.close();
  }

  // TODO: Kiji-76. Update this test to not be specific to licensing.
  @Test
  public void testSignedLicense() throws IOException {
    KijiSystemTable systemTable = new InMemorySystemTable();
    SignedLicense license = LicenseTestObjects.getMockLicense();
    systemTable.putValue(SignedLicense.KEY_LICENSE, license.toBytes());
    byte[] licenseBytes = systemTable.getValue(SignedLicense.KEY_LICENSE);

    assertEquals(license,
        SignedLicense.fromBytes(licenseBytes, LicenseTestObjects.getMockPublicKey()));
    systemTable.close();
  }
}
