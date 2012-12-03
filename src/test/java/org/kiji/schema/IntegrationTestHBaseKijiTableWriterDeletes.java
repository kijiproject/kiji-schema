// (c) Copyright 2011 WibiData, Inc.

package com.wibidata.core.client;

import java.io.IOException;

import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.impl.HBaseKijiTableWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests delete functionality on HBaseKijiTableWriter.
 */
public class IntegrationTestHBaseKijiTableWriterDeletes
    extends BaseWibiTableWriterDeletesIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(
      IntegrationTestHBaseKijiTableWriterDeletes.class);

  @Override
  protected KijiTableWriter createWriter(KijiTable table) throws IOException {
    LOG.debug("Creating LocalWibiTableWriter.");
    return new HBaseKijiTableWriter(table);
  }
}
