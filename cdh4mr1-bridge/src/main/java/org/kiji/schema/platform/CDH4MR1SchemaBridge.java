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

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.mapred.JobConf;

import org.kiji.annotations.ApiAudience;

/**
 * CDH4-backed implementation of the SchemaPlatformBridge API.
 */
@ApiAudience.Private
public final class CDH4MR1SchemaBridge extends SchemaPlatformBridge {
  /** {@inheritDoc} */
  @Override
  public void initializeHadoopResources() {
    // Force load class HdfsConfiguration to register hdfs-site.xml and hdfs-default.xml resources:
    HdfsConfiguration.class.getName();

    // Force load class JobConf to register mapred-site.xml and mapred-default.xml resources:
    JobConf.class.getName();
  }

  /** {@inheritDoc} */
  @Override
  public void setAutoFlush(HTableInterface hTable, boolean autoFlush) {
    // We can do this directly in CDH4.
    hTable.setAutoFlush(autoFlush);
  }

  /** {@inheritDoc} */
  @Override
  public void setWriteBufferSize(HTableInterface hTable, long bufSize)
      throws IOException {
    // We can do this directly in CDH4.
    hTable.setWriteBufferSize(bufSize);
  }
}

