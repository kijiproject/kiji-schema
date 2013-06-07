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

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/**
 * EntityId is used to identify a particular row under the key format of a Kiji table.
 *
 * There are two name-spaces for rows:
 * <ul>
 *   <li> Kiji rows are logically indexed by Kiji row keys (arbitrary byte arrays or a list of
 *        key components).
 *   <li> Under the hood, rows are indexed by HBase row keys (arbitrary byte arrays).
 * </ul>
 *
 * The translation between Kiji row keys and HBase row keys depends on the layout of the table
 * the row belongs to. The same Kiji row key may translate to two different EntityIds on two
 * different tables, based on their key format. For a representation of row keys which is
 * agnostic to row key formats, see {@link KijiRowKeyComponents}.
 *
 * There are multiple translation schemes:
 * <ul>
 *   <li> Raw: Kiji row keys and HBase row keys are identical (identity translation), specifically
 *   used when the row key is an array of bytes.
 *   <li> Hash: HBase row keys are MD5 hashes of the Kiji row key (non reversible transform).
 *   <li> Hash-prefix: HBase row keys are Kiji row keys prefixed by a hash of the Kiji row key.
 *   <li> Formatted: The user specifies the composition of the key. The key can be composed of one
 *   or more components of type string, number or a hash of one of the other components.
 * </ul>
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
public abstract class EntityId {

  /**
   * Creates a new instance. Package-private constructor.
   */
  EntityId() { }

  /**
   * Translates this Kiji row key into an HBase row key.
   *
   * @return the HBase row key.
   */
  public abstract byte[] getHBaseRowKey();

  /**
   * Get the individual components of the kiji Row Key representation. This excludes hash
   * prefixes and only includes user-addressible components.
   * E.g. If the key is composed of a String followed by an Int, getComponentByIndex(0)
   * returns the String component. Zero based indexing.
   *
   * @param idx The index of the component. An integer between 0 and numComponents - 1.
   * @param <T> The type of the row key component.
   * @return The specific component of the row key.
   */
  public abstract <T> T getComponentByIndex(int idx);

  /**
   * Get the components of the row key as a List of Objects. This excludes hash
   * prefixes and only includes user-addressible components.
   *
   * @return List of Objects representing the individual components of a row key.
   */
  public abstract List<Object> getComponents();

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Arrays.hashCode(getHBaseRowKey());
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
    final EntityId eid = (EntityId) obj;
    return Arrays.equals(getHBaseRowKey(), eid.getHBaseRowKey());
  }

  /**
   * A String which can be copied into CLI and parsed to the same EntityId.
   * Characters interpreted by the JSON parser for formatted entity ids
   * as part of the JSON structure are automatically escaped with '\'.
   * Other characters which may be interpreted by your shell environment
   * must be escaped manually.
   *
   * @return A copyable string.
   */
  public abstract String toShellString();
}
