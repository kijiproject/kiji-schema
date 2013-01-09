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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.impl.HashPrefixedEntityId;
import org.kiji.schema.impl.HashedEntityId;
import org.kiji.schema.impl.RawEntityId;


/**
 * Factory class for creating EntityIds.
 *
 * Light-weight object, so as many can be created as needed.
 */
@ApiAudience.Public
@Inheritance.Sealed
public abstract class EntityIdFactory {

  /**
   * Creates an entity ID factory for the specified row key format.
   *
   * @param format Row key format that determines the type of EntityIdFactory that's created
   * @return a new entity ID factory for the specified row key format.
   */
  public static EntityIdFactory create(RowKeyFormat format) {
    Preconditions.checkNotNull(format);
    switch (format.getEncoding()) {
    case RAW:
      return new RawEntityIdFactory(format);
    case HASH:
      return new HashedEntityIdFactory(format);
    case HASH_PREFIX:
      return new HashPrefixedEntityIdFactory(format);
    default:
      throw new RuntimeException(String.format("Unknown row key format: '%s'.", format));
    }
  }

  /** Factory for raw entity IDs. */
  private static final class RawEntityIdFactory extends EntityIdFactory {
    /**
     * Creates a RawEntityIdFactory.
     *
     * @param format Row key format.
     */
    private RawEntityIdFactory(RowKeyFormat format) {
      super(format);
      Preconditions.checkArgument(format.getEncoding() == RowKeyEncoding.RAW);
    }

    /** {@inheritDoc} */
    @Override
    public EntityId fromKijiRowKey(byte[] kijiRowKey) {
      return RawEntityId.fromKijiRowKey(kijiRowKey);
    }

    /** {@inheritDoc} */
    @Override
    public EntityId fromHBaseRowKey(byte[] hbaseRowKey) {
      return RawEntityId.fromHBaseRowKey(hbaseRowKey);
    }
  }

  /** Factory for hashed entity IDs. */
  private static final class HashedEntityIdFactory extends EntityIdFactory {
    /**
     * Creates a HashedEntityIdFactory.
     *
     * @param format Row key format.
     */
    private HashedEntityIdFactory(RowKeyFormat format) {
      super(format);
      Preconditions.checkArgument(format.getEncoding() == RowKeyEncoding.HASH);
    }

    /** {@inheritDoc} */
    @Override
    public EntityId fromKijiRowKey(byte[] kijiRowKey) {
      return HashedEntityId.fromKijiRowKey(kijiRowKey, getFormat());
    }

    /** {@inheritDoc} */
    @Override
    public EntityId fromHBaseRowKey(byte[] hbaseRowKey) {
      return HashedEntityId.fromHBaseRowKey(hbaseRowKey, getFormat());
    }
  }

  /** Factory for hash-prefixed entity IDs. */
  private static final class HashPrefixedEntityIdFactory extends EntityIdFactory {
    /**
     * Creates a HashPrefixedEntityIdFactory.
     *
     * @param format Row key format.
     */
    private HashPrefixedEntityIdFactory(RowKeyFormat format) {
      super(format);
      Preconditions.checkArgument(format.getEncoding() == RowKeyEncoding.HASH_PREFIX);
    }

    /** {@inheritDoc} */
    @Override
    public EntityId fromKijiRowKey(byte[] kijiRowKey) {
      return HashPrefixedEntityId.fromKijiRowKey(kijiRowKey, getFormat());
    }

    /** {@inheritDoc} */
    @Override
    public EntityId fromHBaseRowKey(byte[] hbaseRowKey) {
      return HashPrefixedEntityId.fromHBaseRowKey(hbaseRowKey, getFormat());
    }
  }

  /** Format of the row keys. */
  private final RowKeyFormat mFormat;

  /**
   * Creates an entity ID factory.
   *
   * @param format Format of the row keys.
   */
  public EntityIdFactory(RowKeyFormat format) {
    mFormat = Preconditions.checkNotNull(format);
  }

  /** @return the format of the row keys generated by this factory. */
  public RowKeyFormat getFormat() {
    return mFormat;
  }

  /**
   * Creates an entity ID from a Kiji row key.
   *
   * @param kijiRowKey Kiji row key.
   * @return a new EntityId with the specified Kiji row key.
   */
  public abstract EntityId fromKijiRowKey(byte[] kijiRowKey);

  /**
   * Creates an entity ID from a UTF8 text Kiji row key.
   *
   * @param text UTF8 encoded Kiji row key.
   * @return a new EntityId with the specified Kiji row key.
   */
  public EntityId fromKijiRowKey(String text) {
    return fromKijiRowKey(Bytes.toBytes(text));
  }

  /**
   * Creates an entity ID from an HBase row key.
   *
   * @param hbaseRowKey HBase row key.
   * @return a new EntityId with the specified HBase row key.
   */
  public abstract EntityId fromHBaseRowKey(byte[] hbaseRowKey);
}
