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

import java.util.Map;

import org.kiji.annotations.ApiAudience;

/**
 * Factory for CDH4-specific SchemaPlatformBridge implementation.
 */
@ApiAudience.Private
public final class CDH4SchemaBridgeFactory extends SchemaPlatformBridgeFactory {

  private static SchemaPlatformBridge mSingletonBridge = null;

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public SchemaPlatformBridge getBridge() {
    synchronized (getClass()) {
      if (null != mSingletonBridge) {
        return mSingletonBridge;
      }

      try {
        Class<? extends SchemaPlatformBridge> bridgeClass =
            (Class<? extends SchemaPlatformBridge>) Class.forName(
                "org.kiji.schema.platform.CDH4SchemaBridge");
        mSingletonBridge = bridgeClass.newInstance();
        return mSingletonBridge;
      } catch (Exception e) {
        throw new RuntimeException("Could not instantiate platform bridge", e);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public int getPriority(Map<String, String> runtimeHints) {
    String hadoopVer = org.apache.hadoop.util.VersionInfo.getVersion();
    String hbaseVer = org.apache.hadoop.hbase.util.VersionInfo.getVersion();

    if (hadoopVer.matches("2\\..*-cdh4\\..*") && hbaseVer.matches("0.92.1-cdh4\\..*")) {
      // Hadoop 2.x-cdh4.* and HBase 0.92.1-cdh4.* match correctly; use this bridge.
      return 1000;
    } else {
      // Can't provide for this implementation.
      return 0;
    }
  }
}

