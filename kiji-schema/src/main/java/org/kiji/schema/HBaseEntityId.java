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

package org.kiji.schema;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.util.ByteArrayFormatter;

/**
 * Entity ID encapsulating an HBase row key. This literally
 * represents a byte[] containing an hbase row key.
 */
@ApiAudience.Private
public final class HBaseEntityId extends EntityId {
  private byte[] mHBaseRowKey;

  /**
   * Creates an HBaseEntityId from the specified HBase row key.
   *
   * @param hbaseRowKey HBase row key.
   */
  private HBaseEntityId(byte[] hbaseRowKey) {
    mHBaseRowKey = hbaseRowKey;
  }

  /**
   * Creates a new entity id using the specified hbase row key. This should be used by Kiji
   * project developers, and not by end-users.
   *
   * @param hbaseRowKey that will back the new entity id.
   * @return a new entity id that uses the specified HBase row key.
   */
  public static HBaseEntityId fromHBaseRowKey(byte[] hbaseRowKey) {
    return new HBaseEntityId(hbaseRowKey);
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

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(HBaseEntityId.class)
        .add("hbase", Bytes.toStringBinary(mHBaseRowKey))
        .toString();
  }

  /**
   * Shell friendly string representation of the HBaseEntityId.
   * When used as input to a CLI, will be converted to the EntityId type specified in the
   * provided table layout.
   *
   * @return a copyable string.
   */
  @Override
  public String toShellString() {
    return String.format("hbase=hex:%s", ByteArrayFormatter.toHex(mHBaseRowKey));
  }
}
