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

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.util.ByteArrayFormatter;
import org.kiji.schema.util.Hasher;

/** Implements the hash-prefixed row key format. */
@ApiAudience.Private
public final class HashPrefixedEntityId extends EntityId {
  /** Kiji row key bytes. May be null if we only know the HBase row key. */
  private final byte[] mKijiRowKey;

  /** HBase row key bytes. */
  private final byte[] mHBaseRowKey;

  /**
   * Creates a HashPrefixedEntityId from the specified Kiji row key.
   *
   * @param kijiRowKey Kiji row key.
   * @param format Row key hashing specification.
   * @return a new HashPrefixedEntityId with the specified Kiji row key.
   */
  static HashPrefixedEntityId getEntityId(byte[] kijiRowKey, RowKeyFormat format) {
    Preconditions.checkNotNull(format);
    Preconditions.checkArgument(format.getEncoding() == RowKeyEncoding.HASH_PREFIX);
    final byte[] hash = hashKijiRowKey(format, kijiRowKey);
    final int hashSize = format.getHashSize();
    // Prepend a subset of the hash to the Kiji row key:
    final byte[] hbaseRowKey = new byte[hashSize + kijiRowKey.length];
    System.arraycopy(hash, 0, hbaseRowKey, 0, hashSize);
    System.arraycopy(kijiRowKey, 0, hbaseRowKey, hashSize, kijiRowKey.length);
    return new HashPrefixedEntityId(kijiRowKey, hbaseRowKey, format);
  }

  /**
   * Creates a HashPrefixedEntityId from the specified HBase row key.
   *
   * @param hbaseRowKey HBase row key.
   * @param format Row key hashing specification.
   * @return a new HashedEntityId with the specified HBase row key.
   */
  static HashPrefixedEntityId fromHBaseRowKey(byte[] hbaseRowKey, RowKeyFormat format) {
    Preconditions.checkNotNull(format);
    Preconditions.checkArgument(format.getEncoding() == RowKeyEncoding.HASH_PREFIX);
    final int hashSize = format.getHashSize();
    final byte[] kijiRowKey =
        Arrays.copyOfRange(hbaseRowKey, hashSize, hbaseRowKey.length);
    // TODO Paranoid expensive check : rehash the kiji key and compare with the hash?
    return new HashPrefixedEntityId(kijiRowKey, hbaseRowKey, format);
  }

  /**
   * Hashes a Kiji row key.
   *
   * @param format Hashing specification.
   * @param kijiRowKey Kiji row key.
   * @return a hash of the Kiji row key.
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
   * Creates a new HashPrefixedEntityId.
   *
   * @param kijiRowKey Kiji row key.
   * @param hbaseRowKey HBase row key.
   * @param format Row key hashing specification.
   */
  private HashPrefixedEntityId(byte[] kijiRowKey, byte[] hbaseRowKey, RowKeyFormat format) {
    Preconditions.checkNotNull(format);
    Preconditions.checkArgument(format.getEncoding() == RowKeyEncoding.HASH_PREFIX);
    mKijiRowKey = Preconditions.checkNotNull(kijiRowKey);
    mHBaseRowKey = Preconditions.checkNotNull(hbaseRowKey);
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getHBaseRowKey() {
    return mHBaseRowKey;
  }

  /** {@inheritDoc} **/
  @Override
  @SuppressWarnings("unchecked")
  public <T> T getComponentByIndex(int idx) {
    Preconditions.checkArgument(idx == 0);
    return (T) mKijiRowKey.clone();
  }

  /** {@inheritDoc} **/
  @Override
  public List<Object> getComponents() {
    return Lists.<Object>newArrayList(mKijiRowKey);
  }

  /** @return the Kiji row key, or null. */
  public byte[] getKijiRowKey() {
    return (mKijiRowKey != null) ? mKijiRowKey.clone() : null;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(HashPrefixedEntityId.class)
        .add("kiji", Bytes.toStringBinary(mKijiRowKey))
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
