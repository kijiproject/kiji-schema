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
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.impl.InMemoryKiji;
import org.kiji.schema.util.VersionInfo;


public class TestInMemoryKiji {
  private static final Logger LOG = LoggerFactory.getLogger(TestInMemoryKiji.class);

  @Test
  public void testGetSystemTables() throws IOException {
    LOG.info("Opening an in-memory kiji instance");
    Kiji kiji = new InMemoryKiji();

    assertEquals("Should have opened the default instance", "default", kiji.getName());

    KijiSystemTable systemTable = kiji.getSystemTable();
    assertEquals("Client version should match installed version",
        VersionInfo.getClientDataVersion(), systemTable.getDataVersion());

    assertNotNull(kiji.getSchemaTable());
    assertNotNull(kiji.getMetaTable());

    kiji.close();
  }
}
