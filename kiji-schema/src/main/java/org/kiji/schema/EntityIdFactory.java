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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Preconditions;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.impl.FormattedEntityId;
import org.kiji.schema.impl.HashPrefixedEntityId;
import org.kiji.schema.impl.HashedEntityId;
import org.kiji.schema.impl.RawEntityId;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Factory class for creating EntityIds.
 *
 * Light-weight object, so as many can be created as needed.
 */
@ApiAudience.Framework
@Inheritance.Sealed
public abstract class EntityIdFactory {

  /**
   * Creates an entity ID factory for the specified row key format.
   *
   * @param format Row key format of type RowKeyFormat that determines the
   *               type of EntityIdFactory that's created.
   * @return a new entity ID factory for the specified row key format.
   */
  public static EntityIdFactory getFactory(RowKeyFormat format) {
    Preconditions.checkNotNull(format);
    // FORMATTED encoding is legal here but not supported by RowKeyFormat.
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

  /**
   * Creates an entity ID factory for the specified row key format.
   *
   * @param format Row key format of type RowKeyFormat2 that determines
   *               the type of EntityIdFactory that's created.
   * @return a new entity ID factory for the specified row key format.
   */
  public static EntityIdFactory getFactory(RowKeyFormat2 format) {
    Preconditions.checkNotNull(format);
    // HASH and HASH_PREFIX encoding is legal here, but not supported by RowKeyFormat2.
    switch (format.getEncoding()) {
      case RAW:
        return new RawEntityIdFactory(format);
      case FORMATTED:
        return new FormattedEntityIdFactory(format);
      default:
        throw new RuntimeException(String.format("Unknown row key format: '%s'.", format));
    }
  }

  /**
   * Creates an entity ID factory for the row key format given by the specified
   * table layout.
   *
   * @param kijiTableLayout the layout of the kiji table.
   * @return a new entity ID factory.
   */
  public static EntityIdFactory getFactory(KijiTableLayout kijiTableLayout) {
    Object rowKeyFormat = kijiTableLayout.getDesc().getKeysFormat();
    if (rowKeyFormat instanceof RowKeyFormat) {
      return getFactory((RowKeyFormat) rowKeyFormat);
    } else if (rowKeyFormat instanceof RowKeyFormat2) {
      return getFactory((RowKeyFormat2) rowKeyFormat);
    } else {
      throw new RuntimeException("Kiji Table has unknown RowKeyFormat"
          + rowKeyFormat.getClass().getName());
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
      Preconditions.checkNotNull(format);
      Preconditions.checkArgument(format.getEncoding() == RowKeyEncoding.RAW);
    }

    /**
     * Creates a RawEntityIdFactory.
     *
     * @param format Row key format.
     */
    private RawEntityIdFactory(RowKeyFormat2 format) {
      Preconditions.checkNotNull(format);
      Preconditions.checkArgument(format.getEncoding() == RowKeyEncoding.RAW);
    }

    /** {@inheritDoc} */
    @Override
    public EntityId getEntityId(Object... kijiRowKey) {
      Preconditions.checkNotNull(kijiRowKey);
      Preconditions.checkArgument(kijiRowKey.length == 1);
      if (kijiRowKey[0] instanceof byte[]) {
        return RawEntityId.getEntityId((byte[]) kijiRowKey[0]);
      } else if (kijiRowKey[0] instanceof String) {
        try {
          return RawEntityId.getEntityId(((String) kijiRowKey[0]).getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
          throw new EntityIdException("Unable to convert string to UTF-8 byte array");
        }
      } else {
        throw new EntityIdException("Invalid RAW kiji Row Key");
      }
    }

    /** {@inheritDoc} */
    @Override
    public EntityId getEntityIdFromHBaseRowKey(byte[] hbaseRowKey) {
      return RawEntityId.fromHBaseRowKey(hbaseRowKey);
    }
  }

  /** Factory for formatted entity IDs. */
  private static final class FormattedEntityIdFactory extends EntityIdFactory {
    private final RowKeyFormat2 mRowKeyFormat;

    /**
     * Construct a new Formatted Entity ID factory.
     *
     * @param format The row key format required for creating the factory.
     */
    private FormattedEntityIdFactory(RowKeyFormat2 format) {
      Preconditions.checkNotNull(format);
      Preconditions.checkArgument(format.getEncoding() == RowKeyEncoding.FORMATTED);
      mRowKeyFormat = format;
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public EntityId getEntityId(Object... componentValues) {
      // The user specified the row key in terms of a map of component values.
      Preconditions.checkNotNull(componentValues);
      Preconditions.checkArgument(componentValues.length > 0);
      Preconditions.checkNotNull(componentValues[0]);
      // user provided kiji row key as a List
      if (componentValues.length == 1 && componentValues[0] instanceof List) {
          Preconditions.checkArgument(((List) componentValues[0]).size() > 0);
          return FormattedEntityId.getEntityId((List<Object>) componentValues[0],
              mRowKeyFormat);
      } else {
        return FormattedEntityId.getEntityId(new ArrayList(Arrays.asList(componentValues)),
            mRowKeyFormat);
      }
    }

    /** {@inheritDoc} */
    @Override
    public EntityId getEntityIdFromHBaseRowKey(byte[] hbaseRowKey) {
      return FormattedEntityId.fromHBaseRowKey(hbaseRowKey, mRowKeyFormat);
    }
  }

  /** Factory for hashed entity IDs. */
  private static final class HashedEntityIdFactory extends EntityIdFactory {
    private final RowKeyFormat mRowKeyFormat;

    /**
     * Creates a new Hashed Entity ID factory.
     *
     * @param format The format for creating the hashed entity ID factory.
     */
    private HashedEntityIdFactory(RowKeyFormat format) {
      Preconditions.checkNotNull(format);
      Preconditions.checkArgument(format.getEncoding() == RowKeyEncoding.HASH);
      mRowKeyFormat = format;
    }

    /** {@inheritDoc} */
    @Override
    public EntityId getEntityId(Object... kijiRowKey) {
      Preconditions.checkNotNull(kijiRowKey);
      Preconditions.checkArgument(kijiRowKey.length == 1);
      if (kijiRowKey[0] instanceof byte[]) {
        return HashedEntityId.getEntityId((byte[]) kijiRowKey[0], mRowKeyFormat);
      } else if (kijiRowKey[0] instanceof String) {
        try {
          return HashedEntityId.getEntityId(((String)kijiRowKey[0]).getBytes("UTF-8"),
              mRowKeyFormat);
        } catch (UnsupportedEncodingException e) {
          throw new EntityIdException("Unable to convert string to UTF-8 byte array");
        }
      } else {
        throw new EntityIdException("Invalid components for HashedEntityId. "
            + "Expected a single string or byte array.");
      }
    }

    /** {@inheritDoc} */
    @Override
    public EntityId getEntityIdFromHBaseRowKey(byte[] hbaseRowKey) {
      return HashedEntityId.fromHBaseRowKey(hbaseRowKey, mRowKeyFormat);
    }
  }

  /** Factory for hash-prefixed entity IDs. */
  private static final class HashPrefixedEntityIdFactory extends EntityIdFactory {
    private final RowKeyFormat mRowKeyFormat;
    /**
     * Creates a HashPrefixedEntityIdFactory.
     *
     * @param format Row key format.
     */
    private HashPrefixedEntityIdFactory(RowKeyFormat format) {
      Preconditions.checkNotNull(format);
      Preconditions.checkArgument(format.getEncoding() == RowKeyEncoding.HASH_PREFIX);
      mRowKeyFormat = format;
    }

    /** {@inheritDoc} */
    @Override
    public EntityId getEntityId(Object... kijiRowKey) {
      Preconditions.checkNotNull(kijiRowKey);
      Preconditions.checkArgument(kijiRowKey.length == 1);
      if (kijiRowKey[0] instanceof byte[]) {
        return HashPrefixedEntityId.getEntityId((byte[]) kijiRowKey[0], mRowKeyFormat);
      } else if (kijiRowKey[0] instanceof String) {
        try {
          return HashPrefixedEntityId.getEntityId(((String)kijiRowKey[0]).getBytes("UTF-8"),
              mRowKeyFormat);
        } catch (UnsupportedEncodingException e) {
          throw new EntityIdException("Unable to convert string to UTF-8 byte array");
        }
      } else {
        throw new EntityIdException("Invalid components for HashPrefixedEntityId");
      }
    }

    /** {@inheritDoc} */
    @Override
    public EntityId getEntityIdFromHBaseRowKey(byte[] hbaseRowKey) {
      return HashPrefixedEntityId.fromHBaseRowKey(hbaseRowKey, mRowKeyFormat);
    }
  }

  /**
   * Creates an entity ID from a list of key components.
   *
   * @param componentList This can be one of the following depending on row key encoding:
   *                   <ul>
   *                      <li>
   *                      Raw, Hash, Hash-Prefix EntityId: A single String or byte array
   *                      component.
   *                      </li>
   *                      <li>
   *                      Formatted EntityId: The primitive row key components (string, int,
   *                      long) either passed in their expected order in the key or as an ordered
   *                      list of components.
   *                      </li>
   *                   </ul>
   * @return a new EntityId with the specified Kiji row key.
   */
  public abstract EntityId getEntityId(Object... componentList);

  /**
   * Creates an entity ID from an HBase row key.
   *
   * @param hbaseRowKey HBase row key.
   * @return a new EntityId with the specified HBase row key.
   */
  public abstract EntityId getEntityIdFromHBaseRowKey(byte[] hbaseRowKey);
}
