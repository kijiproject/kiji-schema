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

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Factory class for creating EntityIds.
 *
 * Light-weight object, so as many can be created as needed.
 */
@ApiAudience.Framework
@ApiStability.Stable
@Inheritance.Sealed
public abstract class EntityIdFactory {

  /**
   * Creates a new instance. Package-private constructor.
   */
  EntityIdFactory() { }

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
    case FORMATTED:
      throw new RuntimeException(
          "Row key format encoding FORMATTED is not supported with RowKeyFormat specifications. "
          + "Use RowKeyFormat2 instead.");
    default:
      throw new RuntimeException(String.format("Unhandled row key format: '%s'.", format));
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
      case HASH:
      case HASH_PREFIX:
        throw new RuntimeException(
            "Row key format encodings HASH and HASH_PREFIX are not supported with RowKeyFormat2"
            + " specifications. Use FORMATTED instead.");
      default:
        throw new RuntimeException(String.format("Unhandled row key format: '%s'.", format));
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

  /**
   * Extract a row key component as a byte[].
   *
   * @param keyComponent a row key component expected to be either byte[] or String.
   * @return the row key component as a byte[].
   * @throws EntityIdException if the key component is invalid.
   *     Key components must be String or byte[].
   */
  private static byte[] getBytesKeyComponent(Object keyComponent) {
    if (keyComponent instanceof byte[]) {
      return (byte[]) keyComponent;
    } else if (keyComponent instanceof String) {
      return Bytes.toBytes((String) keyComponent);
    } else {
      throw new EntityIdException(String.format(
          "Invalid row key component: '%s', expecting byte[] or String", keyComponent));
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
    public EntityId getEntityId(Object... components) {
      Preconditions.checkNotNull(components);
      Preconditions.checkArgument(components.length == 1);
      return getEntityId(components[0]);
    }

    /** {@inheritDoc} */
    @Override
    public EntityId getEntityId(List<Object> components) {
      Preconditions.checkNotNull(components);
      Preconditions.checkArgument(components.size() == 1);
      return getEntityId(components.get(0));
    }

    /**
     * Creates a raw entity ID from a single component, either String or byte[].
     *
     * @param rowKey Raw entity ID specification as a String or byte[].
     * @return the specified raw entity ID.
     */
    private EntityId getEntityId(Object rowKey) {
      return RawEntityId.getEntityId(getBytesKeyComponent(rowKey));
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
    public EntityId getEntityId(Object... components) {
      Preconditions.checkNotNull(components);
      Preconditions.checkArgument(components.length > 0);
      Preconditions.checkNotNull(components[0]);
      // TODO: Eliminate the need to convert to a list:
      return FormattedEntityId.getEntityId(Lists.newArrayList(components), mRowKeyFormat);
    }

    /** {@inheritDoc} */
    @Override
    public EntityId getEntityId(List<Object> componentList) {
      Preconditions.checkNotNull(componentList);
      Preconditions.checkArgument(componentList.size() > 0);
      Preconditions.checkNotNull(componentList.get(0));

      // Ensuring the list is copied here since the getEntityId method may modify the
      // list in case of data type mismatches (Int to Long promotion in some cases)
      return FormattedEntityId.getEntityId(Lists.newArrayList(componentList), mRowKeyFormat);
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
    public EntityId getEntityId(Object... components) {
      Preconditions.checkNotNull(components);
      Preconditions.checkArgument(components.length == 1);
      return getEntityId(components[0]);
    }

    /** {@inheritDoc} */
    @Override
    public EntityId getEntityId(List<Object> componentList) {
      Preconditions.checkNotNull(componentList);
      Preconditions.checkArgument(componentList.size() == 1);
      return getEntityId(componentList.get(0));
    }

    /**
     * Creates a hashed entity ID from a single component, either String or byte[].
     *
     * @param rowKey Hashed entity ID specification as a String or byte[].
     * @return the specified hashed entity ID.
     */
    private EntityId getEntityId(Object rowKey) {
      return HashedEntityId.getEntityId(getBytesKeyComponent(rowKey), mRowKeyFormat);
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
    public EntityId getEntityId(Object... components) {
      Preconditions.checkNotNull(components);
      Preconditions.checkArgument(components.length == 1);
      return getEntityId(components[0]);
    }

    /** {@inheritDoc} */
    @Override
    public EntityId getEntityId(List<Object> components) {
      Preconditions.checkNotNull(components);
      Preconditions.checkArgument(components.size() == 1);
      return getEntityId(components.get(0));
    }

    /**
     * Creates a hash-prefixed entity ID from a single component, either String or byte[].
     *
     * @param rowKey Hash-prefixed entity ID specification as a String or byte[].
     * @return the specified hash-prefixed entity ID.
     */
    private EntityId getEntityId(Object rowKey) {
      return HashPrefixedEntityId.getEntityId(getBytesKeyComponent(rowKey), mRowKeyFormat);
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
   * @param components This can be one of the following depending on row key encoding:
   *     <ul>
   *       <li> Raw, Hash, Hash-Prefix EntityId: A single String or byte[] component. </li>
   *       <li> Formatted EntityId: The primitive row key components (string, int, long)
   *            in their expected order in the key. </li>
   *     </ul>
   * @return a new EntityId with the specified Kiji row key.
   */
  public abstract EntityId getEntityId(Object... components);

  /**
   * Creates an entity ID from a list of key components.
   *
   * @param componentList This can be one of the following depending on row key encoding:
   *     <ul>
   *       <li> Raw, Hash, Hash-Prefix EntityId: A single String or byte[] component. </li>
   *       <li> Formatted EntityId: The primitive row key components (string, int, long)
   *            in their expected order in the key. </li>
   *     </ul>
   * @return a new EntityId with the specified Kiji row key.
   */
  public EntityId getEntityId(List<Object> componentList) {
    return getEntityId(componentList.toArray());
  }

  /**
   * Creates an entity ID from a KijiRowKeyComponents.
   *
   * @param kijiRowKeyComponents This should be constructed with one of the following,
   *     depending on row key encoding:
   *     <ul>
   *       <li> Raw, Hash, Hash-Prefix EntityId: A single String or byte[] component. </li>
   *       <li> Formatted EntityId: The primitive row key components (string, int, long)
   *            in their expected order in the key. </li>
   *     </ul>
   * @return a new EntityId for the specified KijiRowKeyComponents.
   */
  public EntityId getEntityId(KijiRowKeyComponents kijiRowKeyComponents) {
    return getEntityId(kijiRowKeyComponents.getComponents());
  }

  /**
   * Creates an entity ID from an HBase row key.
   *
   * @param hbaseRowKey HBase row key.
   * @return a new EntityId with the specified HBase row key.
   */
  public abstract EntityId getEntityIdFromHBaseRowKey(byte[] hbaseRowKey);

  /**
   * Formats an entity ID for pretty-printing. (e.g., to the console.)
   *
   * @deprecated use {@link EntityId#toShellString}.
   * @param eid Entity ID to format.
   * @return the formatted entity ID as a String to print for the user.
   */
  @Deprecated
  public static String formatEntityId(EntityId eid) {
    return eid.toShellString();
  }
}
