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

import java.io.IOException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.impl.InMemorySystemTable;
import org.kiji.schema.util.VersionInfo;

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

}
