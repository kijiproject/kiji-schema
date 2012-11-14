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
import org.apache.hadoop.hbase.client.HTable;

/**
 * <p>A factory for creating HBase HTable instances.  We have our own version of this factory
 * despite the fact that HBase ships with an HTableFactory class of its own.  This is
 * because org.apache.hadoop.hbase.client.HTableFactory doesn't throw IOException.
 * Instead, it wraps all checked exceptions with RuntimException.</p>
 *
 * <p>Our implementation throws IOException, so callers can do things like check for
 * TableNotFoundException and special case it.</p>
 */
public class HTableFactory {
  /**
   * Creates a new <code>HTableFactory</code> instance.
   */
  public HTableFactory() {
  }

  /**
   * Creates a new HTable instance.
   *
   * @param conf The configuration for the HBase cluster.
   * @param hbaseTableName The name of the HBase table to create a connection to.
   * @return A new HTable instance.
   * @throws IOException If there is an error.
   */
  public HTable create(Configuration conf, String hbaseTableName) throws IOException {
    return new HTable(conf, hbaseTableName);
  }
}
