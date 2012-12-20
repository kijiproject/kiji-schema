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

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;

/**
 * Hadoop 1.x and HBase 0.92.x-backed implementation of the SchemaPlatformBridge API.
 */
@ApiAudience.Private
public final class Hadoop1xSchemaBridge extends SchemaPlatformBridge {
  private static final Logger LOG = LoggerFactory.getLogger(Hadoop1xSchemaBridge.class);

  /** {@inheritDoc} */
  @Override
  public void setAutoFlush(HTableInterface hTable, boolean autoFlush) {
    // The HTable implementation of HTableInterface can do this; downcast if available.
    if (hTable instanceof HTable) {
      ((HTable) hTable).setAutoFlush(autoFlush);
    } else {
      LOG.error("Cannot set autoFlush=" + autoFlush + " for HTableInterface impl "
          + hTable.getClass().getName());
    }
  }

  /** {@inheritDoc} */
  @Override
  public void setWriteBufferSize(HTableInterface hTable, long bufSize)
      throws IOException {
    // The HTable implementation of HTableInterface can do this; downcast if available.
    if (hTable instanceof HTable) {
      ((HTable) hTable).setWriteBufferSize(bufSize);
    } else {
      LOG.error("Cannot set writeBufSize=" + bufSize + " for HTableInterface impl "
          + hTable.getClass().getName());
    }
  }
}

