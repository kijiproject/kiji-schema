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
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * Factory for HBaseAdmin.
 *
 * Note: there is no interface for HBaseAdmin :(
 */
public interface HBaseAdminFactory {
  /**
   * Creates a new HBaseAdmin instance.
   *
   * @param conf Configuration.
   * @return a new HBaseAdmin.
   * @throws IOException on I/O error.
   */
  HBaseAdmin create(Configuration conf) throws IOException;
}
