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

import org.kiji.schema.avro.RowKeyFormat;

/**
 * EntityId is used to identify a particular row in a Kiji table.
 *
 * There are two name-spaces for rows:
 * <ul>
 *   <li> Kiji rows are primarily indexed by Kiji row keys (arbitrary byte arrays).
 *   <li> Under the hood, rows are indexed by HBase row keys (arbitrary byte arrays).
 * </ul>
 *
 * The translation between Kiji row keys and HBase row keys depends on the layout of the table
 * the row belongs to.
 *
 * There are multiple translation schemes:
 * <ul>
 *   <li> Raw: Kiji row keys and HBase row keys are identical (identity translation).
 *   <li> MD5: HBase row keys are MD5 hashes of the Kiji row key (non reversible transform).
 *   <li> Hash-prefix: HBase row keys are Kiji row keys prefixed by a hash of the Kiji row key.
 *   <li> Composite: to be determined.
 * </ul>
 */
public abstract class EntityId {
  /** @return the format of this row key. */
  public abstract RowKeyFormat getFormat();

  /** @return the Kiji row key as a byte array. */
  public abstract byte[] getKijiRowKey();

  /**
   * Translates this Kiji row key into an HBase row key.
   *
   * @return the HBase row key.
   */
  public abstract byte[] getHBaseRowKey();

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
}
