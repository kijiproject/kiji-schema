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

package org.kiji.schema.util;

import java.util.regex.Pattern;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.kiji.schema.IncompatibleKijiVersionException;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.impl.Versions;

public class TestVersionInfo {
  @Test
  public void testGetSoftwareVersion() throws Exception {
    // We cannot compare against any concrete value here, or else the test will fail
    // depending on whether this is a development version or a release...
    assertFalse(VersionInfo.getSoftwareVersion().isEmpty());
  }

  @Test
  public void testGetClientDataVersion() {
    // This is the actual version we expect to be in there right now.
    assertEquals(Versions.MAX_SYSTEM_VERSION, VersionInfo.getClientDataVersion());
  }

  @Test
  public void testGetClusterDataVersion() throws Exception {
    final KijiSystemTable systemTable = createMock(KijiSystemTable.class);
    // This version number for this test was picked out of a hat.
    expect(systemTable.getDataVersion()).andReturn(ProtocolVersion.parse("system-1.1")).anyTimes();

    final Kiji kiji = createMock(Kiji.class);
    expect(kiji.getSystemTable()).andReturn(systemTable).anyTimes();

    replay(systemTable);
    replay(kiji);
    assertEquals(ProtocolVersion.parse("system-1.1"), VersionInfo.getClusterDataVersion(kiji));
    verify(systemTable);
    verify(kiji);
  }

  @Test
  public void testValidateVersion() throws Exception {
    final KijiSystemTable systemTable = createMock(KijiSystemTable.class);
    expect(systemTable.getDataVersion()).andReturn(VersionInfo.getClientDataVersion()).anyTimes();

    final Kiji kiji = createMock(Kiji.class);
    expect(kiji.getSystemTable()).andReturn(systemTable).anyTimes();

    replay(systemTable);
    replay(kiji);

    // This should validate, since we set the cluster data version to match the client version.
    VersionInfo.validateVersion(kiji);

    verify(systemTable);
    verify(kiji);
  }

  @Test
  public void testValidateVersionFail() throws Exception {
    final KijiSystemTable systemTable = createMock(KijiSystemTable.class);
    expect(systemTable.getDataVersion()).andReturn(ProtocolVersion.parse("kiji-0.9")).anyTimes();

    final Kiji kiji = createMock(Kiji.class);
    expect(kiji.getSystemTable()).andReturn(systemTable).anyTimes();

    replay(systemTable);
    replay(kiji);

    // This should throw an invalid version exception.
    try {
      VersionInfo.validateVersion(kiji);
      fail("An exception should have been thrown.");
    } catch (IncompatibleKijiVersionException ikve) {
      assertTrue(ikve.getMessage(), Pattern.matches(
          "Data format of Kiji instance \\(kiji-0\\.9\\) cannot operate "
          + "with client \\(system-[0-9]\\.[0-9]\\)",
          ikve.getMessage()));
    }
  }

  @Test
  public void testKijiVsSystemProtocol() {
    // New client (1.0.0-rc4 or higher) interacting with a table installed via 1.0.0-rc3.
    final ProtocolVersion clientVersion = ProtocolVersion.parse("system-1.0");
    final ProtocolVersion clusterVersion = ProtocolVersion.parse("kiji-1.0");

    assertTrue(VersionInfo.areInstanceVersionsCompatible(clientVersion, clusterVersion));
  }

  @Test
  public void testOldKijiRefusesToReadNewSystemProtocol() {
    // The old 1.0.0-rc3 client should refuse to interop with a new instance created by
    // 1.0.0-rc4 or higher due to the data version protocol namespace change. Forwards
    // compatibility is not supported by the release candidates, even though we honor
    // backwards compatibility.
    final ProtocolVersion clientVersion = ProtocolVersion.parse("kiji-1.0");
    final ProtocolVersion clusterVersion = ProtocolVersion.parse("system-1.0");

    assertFalse(VersionInfo.areInstanceVersionsCompatible(clientVersion, clusterVersion));
  }

  @Test
  public void testKijiVsNewerSystemProtocol() {
    // An even newer client (not yet defined as of 1.0.0-rc4) that uses a 'system-1.1'
    // protocol should still be compatible with a table installed via 1.0.0-rc3.
    // kiji-1.0 => system-1.0, and all system-1.x versions should be compatible.
    final ProtocolVersion clientVersion = ProtocolVersion.parse("system-1.1");
    final ProtocolVersion clusterVersion = ProtocolVersion.parse("kiji-1.0");

    assertTrue(VersionInfo.areInstanceVersionsCompatible(clientVersion, clusterVersion));
  }

  @Test
  public void testDifferentSystemProtocols() {
    // In the future, when we release it, system-1.1 should be compatible with system-1.0.
    final ProtocolVersion clientVersion = ProtocolVersion.parse("system-1.1");
    final ProtocolVersion clusterVersion = ProtocolVersion.parse("system-1.0");

    assertTrue(VersionInfo.areInstanceVersionsCompatible(clientVersion, clusterVersion));
  }

  @Test
  public void testSystemProtocolName() {
    // A client (1.0.0-rc4 or higher) interacting with a table installed via 1.0.0-rc4.
    final ProtocolVersion clientVersion = ProtocolVersion.parse("system-1.0");
    final ProtocolVersion clusterVersion = ProtocolVersion.parse("system-1.0");

    assertTrue(VersionInfo.areInstanceVersionsCompatible(clientVersion, clusterVersion));
  }

  @Test
  public void testNoForwardCompatibility() {
    // A system-1.0-capable client should not be able to read a system-2.0 installation.
    final ProtocolVersion clientVersion = ProtocolVersion.parse("system-1.0");
    final ProtocolVersion clusterVersion = ProtocolVersion.parse("system-2.0");

    assertFalse(VersionInfo.areInstanceVersionsCompatible(clientVersion, clusterVersion));
  }

  @Test
  public void testBackCompatibilityThroughMajorVersions() {
    // A system-2.0-capable client should still be able to read a system-1.0 installation.
    final ProtocolVersion clientVersion = ProtocolVersion.parse("system-2.0");
    final ProtocolVersion clusterVersion = ProtocolVersion.parse("system-1.0");

    assertTrue(VersionInfo.areInstanceVersionsCompatible(clientVersion, clusterVersion));
  }

}
