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

import org.kiji.schema.EntityId;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.util.Hasher;

/** Implements the hashed row key format. */
public final class HashedEntityId extends EntityId {
  private final RowKeyFormat mFormat;

  /** Kiji row key bytes. May be null if we only know the HBase row key. */
  private final byte[] mKijiRowKey;

  /** HBase row key bytes. */
  private final byte[] mHBaseRowKey;

  /**
   * Creates a HashedEntityId from the specified Kiji row key.
   *
   * @param kijiRowKey Kiji row key.
   * @param format Row key hashing specification.
   * @return a new HashedEntityId with the specified Kiji row key.
   */
  public static HashedEntityId fromKijiRowKey(byte[] kijiRowKey, RowKeyFormat format) {
    Preconditions.checkNotNull(format);
    final byte[] hbaseRowKey = hashKijiRowKey(format, kijiRowKey);
    return new HashedEntityId(kijiRowKey, hbaseRowKey, format);
  }

  /**
   * Creates a HashedEntityId from the specified HBase row key.
   *
   * @param hbaseRowKey HBase row key.
   * @param format Row key hashing specification.
   * @return a new HashedEntityId with the specified HBase row key.
   */
  public static HashedEntityId fromHBaseRowKey(byte[] hbaseRowKey, RowKeyFormat format) {
    // TODO Validate that hbaseRowKey has the characteristics of the hashing method
    // specified in format
    // kijiRowKey is null: there is no (known) way to reverse the hash
    return new HashedEntityId(null, hbaseRowKey, format);
  }

  /**
   * Hashes a Kiji row key.
   *
   * @param format Hashing specification.
   * @param kijiRowKey Kiji row key to hash.
   * @return a hash of the given Kiji row key.
   */
  public static byte[] hashKijiRowKey(RowKeyFormat format, byte[] kijiRowKey) {
    // TODO refactor into hash factories:
    switch (format.getHashType()) {
    case MD5: return Hasher.hash(kijiRowKey);
    default:
      throw new RuntimeException(String.format("Unexpected hashing type: '%s'.", format));
    }
  }

  /**
   * Creates a new HashedEntityId.
   *
   * @param kijiRowKey Kiji row key.
   * @param hbaseRowKey HBase row key.
   * @param format Row key hashing specification.
   */
  private HashedEntityId(byte[] kijiRowKey, byte[] hbaseRowKey, RowKeyFormat format) {
    mKijiRowKey = kijiRowKey;
    mHBaseRowKey = Preconditions.checkNotNull(hbaseRowKey);
    mFormat = Preconditions.checkNotNull(format);
    Preconditions.checkArgument(format.getEncoding() == RowKeyEncoding.HASH);
  }

  /** {@inheritDoc} */
  @Override
  public RowKeyFormat getFormat() {
    return mFormat;
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getKijiRowKey() {
    return mKijiRowKey;
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getHBaseRowKey() {
    return mHBaseRowKey;
  }
}
