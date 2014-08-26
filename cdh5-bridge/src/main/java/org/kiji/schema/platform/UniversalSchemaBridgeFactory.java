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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.delegation.Priority;

/**
 * Factory for fallback SchemaPlatformBridge implementation, if a better provider cannot
 * be found.
 *
 * <p>Defaults to using cdh5mr1-bridge and hopes for the best. This class should always point to
 * the latest bridge and move accordingly.</p>
 */
@ApiAudience.Private
public final class UniversalSchemaBridgeFactory extends SchemaPlatformBridgeFactory {
  private static final Logger LOG = LoggerFactory.getLogger(UniversalSchemaBridgeFactory.class);

  /** {@inheritDoc} */
  @Override
  public SchemaPlatformBridge getBridge() {
    LOG.warn("No suitable schema bridge provider found, falling back on Universal Bridge.");
    LOG.warn("This may be an error.  If you have trouble, please file an issue on jira.kiji.org"
        + " with your hadoop and hbase versions.");
    LOG.warn("Hadoop version: {}", org.apache.hadoop.util.VersionInfo.getVersion());
    LOG.warn("HBase version: {}", org.apache.hadoop.hbase.util.VersionInfo.getVersion());
    return new CDH5SchemaBridgeFactory().getBridge();
  }

  /** {@inheritDoc} */
  @Override
  public int getPriority(Map<String, String> runtimeHints) {
    // Always volunteer, but have a very low priority. Prefer virtually any other
    // provider to this one.
    return Priority.VERY_LOW;
  }
}

