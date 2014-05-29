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
package org.kiji.schema.impl.hbase;

import java.io.IOException;

import junit.framework.Assert;
import org.junit.Test;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;
import org.kiji.schema.util.ProtocolVersion;

/** Tests for HBaseKiji. */
public class IntegrationTestHBaseKiji extends AbstractKijiIntegrationTest {
  /** Validates the constructor of HBaseKiji for data version "system-2.0". */
  @Test
  public void testHBaseKijiSystem2dot0() throws Exception {
    final KijiURI uri = getKijiURI();
    setDataVersion(uri, ProtocolVersion.parse("system-2.0"));
    final HBaseKiji kiji = (HBaseKiji) Kiji.Factory.open(uri);
    try {
      Assert.assertNotNull(kiji.getZKClient());
    } finally {
      kiji.release();
    }
  }

  /** Validates HBaseKiji.modifyTableLayout() for data version "system-2.0". */
  @Test
  public void testHBaseKijiSystemModifyTableLayout2dot0() throws Exception {
    final KijiURI uri = getKijiURI();
    setDataVersion(uri, ProtocolVersion.parse("system-2.0"));
    final HBaseKiji kiji = (HBaseKiji) Kiji.Factory.open(uri);
    try {
      Assert.assertNotNull(kiji.getZKClient());

      kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST));
      final KijiTable table = kiji.openTable("foo");
      try {
        final TableLayoutDesc layoutUpdate =
            TableLayoutDesc.newBuilder(table.getLayout().getDesc()).build();
        layoutUpdate.setReferenceLayout(layoutUpdate.getLayoutId());
        layoutUpdate.setLayoutId("2");

        final KijiTableLayout newLayout = kiji.modifyTableLayout(layoutUpdate);

      } finally {
        table.release();
      }

    } finally {
      kiji.release();
    }
  }


  /**
   * Sets the data version of a Kiji instance.
   *
   * @param uri URI of the Kiji instance to configure the data version of.
   * @param version Data version to use.
   * @throws IOException on I/O error.
   */
  private static void setDataVersion(final KijiURI uri, final ProtocolVersion version)
      throws IOException {
    final Kiji kiji = Kiji.Factory.open(uri);
    try {
      kiji.getSystemTable().setDataVersion(version);
    } finally {
      kiji.release();
    }
  }
}
