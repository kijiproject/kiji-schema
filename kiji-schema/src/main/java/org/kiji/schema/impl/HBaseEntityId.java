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

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;

/**
 * Entity ID encapsulating an HBase row key. This literally
 * represents a byte[] containing an hbase row key.
 */
@ApiAudience.Private
public class HBaseEntityId extends EntityId {
  private byte[] mHBaseRowKey;

  /**
   * Creates an HBaseEntityId from the specified HBase row key.
   *
   * @param hbaseRowKey HBase row key.
   */
  public HBaseEntityId(byte[] hbaseRowKey) {
    mHBaseRowKey = hbaseRowKey;
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getHBaseRowKey() {
    return mHBaseRowKey;
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public <T> T getComponentByIndex(int idx) {
    Preconditions.checkArgument(idx == 0);
    return (T)mHBaseRowKey.clone();
  }

  /** {@inheritDoc} */
  @Override
  public List<Object> getComponents() {
    List<Object> resp = new ArrayList<Object>();
    resp.add(mHBaseRowKey);
    return resp;
  }

}
