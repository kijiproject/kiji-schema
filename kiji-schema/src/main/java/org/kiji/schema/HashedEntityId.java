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
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.util.ByteArrayFormatter;
import org.kiji.schema.util.Hasher;

/** Implements the hashed row key format. */
@ApiAudience.Private
public final class HashedEntityId extends EntityId {
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
  static HashedEntityId getEntityId(byte[] kijiRowKey, RowKeyFormat format) {
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
  static HashedEntityId fromHBaseRowKey(byte[] hbaseRowKey, RowKeyFormat format) {
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
    Preconditions.checkNotNull(format);
    Preconditions.checkArgument(format.getEncoding() == RowKeyEncoding.HASH);
    mKijiRowKey = kijiRowKey;
    mHBaseRowKey = Preconditions.checkNotNull(hbaseRowKey);
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
    Preconditions.checkState(mKijiRowKey != null,
        "Cannot retrieve components as materialization is suppressed");

    return (T)mKijiRowKey.clone();
  }

  /** {@inheritDoc} */
  @Override
  public List<Object> getComponents() {
    Preconditions.checkState(mKijiRowKey != null,
        "Cannot retrieve components as materialization is suppressed");

    List<Object> resp = new ArrayList<Object>();
    resp.add(mKijiRowKey);
    return resp;
  }

  /** @return the Kiji row key, or null. */
  public byte[] getKijiRowKey() {
    return (mKijiRowKey != null) ? mKijiRowKey.clone() : null;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(HashedEntityId.class)
        .add("kiji", (mKijiRowKey == null) ? "<unknown>" : Bytes.toStringBinary(mKijiRowKey))
        .add("hbase", Bytes.toStringBinary(mHBaseRowKey))
        .toString();
  }

  /** {@inheritDoc} */
  @Override
  public String toShellString() {
    if (mKijiRowKey != null) {
      return String.format("kiji=%s", Bytes.toStringBinary(mKijiRowKey));
    } else {
      return String.format("hbase=hex:%s", ByteArrayFormatter.toHex(mHBaseRowKey));
    }
  }
}
