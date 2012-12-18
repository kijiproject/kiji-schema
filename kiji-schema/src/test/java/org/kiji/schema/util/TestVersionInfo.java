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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import org.kiji.schema.IncompatibleKijiVersionException;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiSystemTable;

public class TestVersionInfo {
  @Test
  public void testGetSoftwareVersion() throws Exception {
    // We cannot compare against any concrete value here, or else the test will fail
    // depending on whether this is a development version or a release...
    assertFalse(VersionInfo.getSoftwareVersion().isEmpty());
  }

  @Test
  public void testGetClientDataVersion() {
    assertEquals("kiji-1.0", VersionInfo.getClientDataVersion());
  }

  @Test
  public void testGetClusterDataVersion() throws Exception {
    final KijiSystemTable systemTable = createMock(KijiSystemTable.class);
    expect(systemTable.getDataVersion()).andReturn("someVersion").anyTimes();

    final Kiji kiji = createMock(Kiji.class);
    expect(kiji.getSystemTable()).andReturn(systemTable).anyTimes();

    replay(systemTable);
    replay(kiji);
    assertEquals("someVersion", VersionInfo.getClusterDataVersion(kiji));
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

  @Test(expected = IncompatibleKijiVersionException.class)
  public void testValidateVersionFail() throws Exception {
    final KijiSystemTable systemTable = createMock(KijiSystemTable.class);
    expect(systemTable.getDataVersion()).andReturn("an-incompatible-version").anyTimes();

    final Kiji kiji = createMock(Kiji.class);
    expect(kiji.getSystemTable()).andReturn(systemTable).anyTimes();

    replay(systemTable);
    replay(kiji);

    // This should throw an invalid version exception.
    VersionInfo.validateVersion(kiji);
  }
}
