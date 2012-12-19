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

package org.kiji.schema.platform;

import static org.junit.Assert.*;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSchemaPlatformBridge {
  private static final Logger LOG = LoggerFactory.getLogger(TestSchemaPlatformBridge.class);

  @Test
  public void testGetBridge() {
    LOG.info("Hadoop version: " + org.apache.hadoop.util.VersionInfo.getVersion());
    LOG.info("HBase version: " + org.apache.hadoop.hbase.util.VersionInfo.getVersion());

    SchemaPlatformBridge bridge = SchemaPlatformBridge.get();
    assertNotNull(bridge);
    LOG.info("Got platform bridge: " + bridge.getClass().getName());
  }
}
