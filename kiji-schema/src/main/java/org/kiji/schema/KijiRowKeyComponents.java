/**
 * (c) Copyright 2013 WibiData, Inc.
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
import com.google.common.primitives.UnsignedBytes;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * This class represents an encapsulation of the parts of a Kiji row key,
 * suitable for translation to an {@link EntityId}.
 *
 * <p>KijiRowKeyComponents has one factory method, {@link #fromComponents(Object...)}.</p>
 *
 * <p>KijiRowKeyComponents consist of an ordered series of <code>Object</code>s. There are
 * presently two valid forms of a KijiRowKeyComponents:
 *   <ul>
 *     <li>A single component, either a <code>byte[]</code> or <code>String</code>. This
 *         is suitable for generating raw, hash, or hash-prefix EntityIds.</li>
 *     <li>One or more components of types <code>String</code>, <code>Integer</code>,
 *         or <code>Long</code>. This is suitable for generating formatted EntityIds.
 *         May contain trailing nulls, but the first component must not be null, and no
 *         non-null component is permitted after a null component.</li>
 *   </ul>
 * </p>
 *
 * <p>For more information on these formats, see {@link EntityId}. There are two ways to convert
 * a KijiRowKeyComponents to an EntityId:
 *  <ul>
 *     <li>As a parameter to a {@link EntityIdFactory#getEntityId(KijiRowKeyComponents)}</li>
 *     <li>Via the method {@link #getEntityIdForTable(KijiTable)}</li>
 *   </ul>
 * </p>
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class KijiRowKeyComponents implements Comparable<KijiRowKeyComponents> {
  /** The backing array of components. */
  private Object[] mComponents;

  /**
   * Private constructor.
   *
   * @param components the components.
   */
  private KijiRowKeyComponents(Object[] components) {
    mComponents = components;
  }

  /**
   * Creates a KijiRowKeyComponents from components.
   *
   * @param components the components of the row key.
   * @return a KijiRowKeyComponents
   */
  public static KijiRowKeyComponents fromComponents(Object... components) {
    Preconditions.checkNotNull(components);
    Preconditions.checkArgument(components.length > 0);
    Preconditions.checkNotNull(components[0], "First component cannot be null.");

    // Some of these checks will be redundant when the KijiRowKeyComponents is converted
    // into an EntityId, but putting them here helps the user see them at the time of creation.
    if (components[0] instanceof byte[]) {
      Preconditions.checkArgument(components.length == 1, "byte[] only valid as sole component.");
      // We need to make a deep copy of the byte[] to ensure that a later mutation to the original
      // byte[] doesn't confuse hash codes, etc.
      byte[] original = (byte[])components[0];
      return new KijiRowKeyComponents(new Object[]{Arrays.copyOf(original, original.length)});
    } else {
      boolean seenNull = false;
      for (int i = 0; i < components.length; i++) {
        if (seenNull) {
          Preconditions.checkArgument(
              components[i] == null,
              "Kiji Row Keys cannot contain have a non-null component after a null component");
        } else {
          Object part = components[i];
          if (part == null) {
            seenNull = true;
          } else {
            Preconditions.checkArgument(
                (part instanceof String) || (part instanceof Integer) || (part instanceof Long),
                "Components must be of type String, Integer, or Long.");
          }
        }
      }
    }

    // Pass in a copy rather than the actual array, just in case the user called us with an
    // Object[], which would make components mutable.
    return new KijiRowKeyComponents(Arrays.copyOf(components, components.length));
  }

  /**
   * Creates a KijiRowKeyComponents from a list of components.
   *
   * @param componentsList A List&lt;Object&gt; of the components of the row key.
   * @return a KijiRowKeyComponents
   */
  public static KijiRowKeyComponents fromComponentsList(List<Object> componentsList) {
    return fromComponents(componentsList.toArray());
  }

  /**
   * Gets an EntityId for this KijiRowKeyComponents appropriate to the given table.
   *
   * @param table the KijiTable to generate an EntityId. The table's row key format must be
   *     compatible with the number and type of the components.
   * @return an EntityId appropriate to the table.
   */
  public EntityId getEntityIdForTable(KijiTable table) {
    return table.getEntityId(mComponents);
  }

  /**
   * Package-private accessor to retrieve the array of components. Intended for use
   * internally by other members of the package (e.g. EntityIdFactory).
   *
   * @return a list of the components. Do not modify this array.
   */
  Object[] getComponents() {
    return mComponents;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Arrays.deepHashCode(mComponents);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!getClass().equals(obj.getClass())) {
      return false;
    }
    final KijiRowKeyComponents krkc = (KijiRowKeyComponents)obj;

    return Arrays.deepEquals(mComponents, krkc.mComponents);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("Components", Arrays.toString(mComponents))
        .toString();
  }

  /**
   * {@inheritDoc}
   *
   * *Note* the ordering of KijiRowKeyComponents is not representative of the scan order of rows in
   * a Kiji table. Hashing is not taken into account when comparing {@link KijiRowKeyComponents}.
   *
   * Only {@link KijiRowKeyComponents} from the same table may be compared. The compared row key
   * components must contain the same number of components, and the corresponding components in each
   * row key components must be of the same type.
   */
  @Override
  public int compareTo(KijiRowKeyComponents other) {
    Object[] components1 = this.getComponents();
    Object[] components2 = other.getComponents();

    int size1 = components1.length;
    int size2 = components2.length;

    if (size1 != size2) {
      throw new IllegalArgumentException(
          String.format(
              "Can not compare KijiRowKeyComponents with different numbers of components."
                  + " Components: %s, %s.", this, other));
    }

    // Compare individual components
    for (int i = 0; i < size1; i++) {
      Object a = components1[i];
      Object b = components2[i];
      // null values sort first
      if (a == null && b == null) {
        continue;
      }
      if (a == null) {
        return -1;
      }
      if (b == null) {
        return 1;
      }

      // If both components are non-null, then use natural comparison
      final int comparison;
      try {
        if (a instanceof String) {
          comparison = ((String) a).compareTo((String) b);
        } else if (a instanceof Integer) {
          comparison = ((Integer) a).compareTo((Integer) b);
        } else if (a instanceof Long) {
          comparison = ((Long) a).compareTo((Long) b);
        } else if (a instanceof byte[]) {
          comparison = UnsignedBytes.lexicographicalComparator().compare((byte[]) a, (byte[]) b);
        } else {
          throw new IllegalArgumentException(
              String.format("Unknown Entity Id component type %s.", a.getClass()));
        }
        if (comparison != 0) {
          return comparison;
        }
      } catch (ClassCastException e) {
        throw new IllegalArgumentException(
            String.format(
                "Can not compare KijiRowKeyComponents with different component types."
                    + " Index: %s. Types: %s, %s. Components: %s, %s.",
                i, a.getClass(), b.getClass(), this, other));
      }
    }
    return 0;
  }
}
