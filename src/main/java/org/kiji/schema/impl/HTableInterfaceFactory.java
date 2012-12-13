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

package org.kiji.schema.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;

import org.kiji.annotations.ApiAudience;

/**
 * Factory for HTableInterface instances.
 *
 * This interface exists because the HBase HTableInstanceFactory doesn't throw IOException.
 */
@ApiAudience.Private
public interface HTableInterfaceFactory {
  /**
   * Creates a new HTableInterface instance.
   *
   * @param conf The configuration for the HBase cluster.
   * @param hbaseTableName The name of the HBase table to create a connection to.
   * @return a new HTableInterface for the specified table.
   * @throws IOException on I/O error.
   */
  HTableInterface create(Configuration conf, String hbaseTableName) throws IOException;
}
