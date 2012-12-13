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

import java.util.Comparator;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.annotations.ApiAudience;

/**
 * Comparator for HColumnDescriptors.  They are sorted by name, then
 * by max versions, ttl, and whether it is in memory.
 */
@ApiAudience.Private
public class HColumnDescriptorComparator implements Comparator<HColumnDescriptor> {
  @Override
  public int compare(HColumnDescriptor o1, HColumnDescriptor o2) {
    int nameResult = Bytes.compareTo(o1.getName(), o2.getName());
    if (nameResult != 0) {
      return nameResult;
    }
    int maxVersionsResult = Integer.valueOf(o1.getMaxVersions()).compareTo(o2.getMaxVersions());
    if (maxVersionsResult != 0) {
      return maxVersionsResult;
    }
    int timeToLiveResult = Integer.valueOf(o1.getTimeToLive()).compareTo(o2.getTimeToLive());
    if (timeToLiveResult != 0) {
      return timeToLiveResult;
    }
    int inMemoryResult = Boolean.valueOf(o1.isInMemory()).compareTo(o2.isInMemory());
    if (inMemoryResult != 0) {
      return inMemoryResult;
    }
    // TODO: Add things like bloomfiltertype as necessary.
    return o1.getCompressionType().toString().compareTo(o2.getCompressionType().toString());
  }
}
