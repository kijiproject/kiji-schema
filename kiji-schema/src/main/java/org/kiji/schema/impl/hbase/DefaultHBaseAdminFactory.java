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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.impl.HBaseAdminFactory;

/** Factory for HBaseAdmin that creates concrete HBaseAdmin instances. */
@ApiAudience.Private
public final class DefaultHBaseAdminFactory implements HBaseAdminFactory {
  /** Singleton. */
  private static final HBaseAdminFactory DEFAULT = new DefaultHBaseAdminFactory();

  /** @return an instance of the default factory. */
  public static HBaseAdminFactory get() {
    return DEFAULT;
  }

  /** Disallow new instances, enforce singleton. */
  private DefaultHBaseAdminFactory() {
  }

  /** {@inheritDoc} */
  @Override
  public HBaseAdmin create(Configuration conf) throws IOException {
    return new HBaseAdmin(conf);
  }
}
