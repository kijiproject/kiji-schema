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

import com.google.common.base.Preconditions;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.avro.HashType;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat;

/** Implements the raw row key format. */
@ApiAudience.Private
public final class RawEntityId extends EntityId {
  private static final RowKeyFormat RAW_KEY_FORMAT = RowKeyFormat.newBuilder()
      .setEncoding(RowKeyEncoding.RAW)
      .setHashType(HashType.MD5)  // HashType.NONE or INVALID?
      .setHashSize(0)
      .build();

  /**
   * Creates a RawEntityId from the specified Kiji row key.
   *
   * @param kijiRowKey Kiji row key.
   * @return a new RawEntityId with the specified Kiji row key.
   */
  public static RawEntityId fromKijiRowKey(byte[] kijiRowKey) {
    return new RawEntityId(kijiRowKey);
  }

  /**
   * Creates a RawEntityId from the specified HBase row key.
   *
   * @param hbaseRowKey HBase row key.
   * @return a new RawEntityId with the specified HBase row key.
   */
  public static RawEntityId fromHBaseRowKey(byte[] hbaseRowKey) {
    return new RawEntityId(hbaseRowKey);
  }

  /**
   * Kiji row key bytes.
   * This is also the HBase row key bytes, as the RAW format uses the identity transform.
   */
  private final byte[] mBytes;

  /**
   * Creates a raw entity ID.
   *
   * @param rowKey Kiji/HBase row key (both row keys are identical).
   */
  public RawEntityId(byte[] rowKey) {
    mBytes = Preconditions.checkNotNull(rowKey);
  }

  /** {@inheritDoc} */
  @Override
  public RowKeyFormat getFormat() {
    return RAW_KEY_FORMAT;
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getKijiRowKey() {
    return mBytes;
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getHBaseRowKey() {
    return mBytes;
  }
}
