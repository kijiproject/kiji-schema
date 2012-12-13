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

import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.annotations.ApiAudience;

/**
 * An HBase column name.
 */
@ApiAudience.Public
public final class HBaseColumnName {
  /** The HBase column family. */
  private final byte[] mFamily;

  /** The HBase column qualifier. */
  private final byte[] mQualifier;

  /** A cached string version of the family. */
  private String mFamilyString;

  /** A cached string version of the qualifier. */
  private String mQualifierString;

  /**
   * Creates a new <code>HBaseColumnName</code> instance.
   *
   * @param family The column family.
   * @param qualifier The column qualifier.
   */
  public HBaseColumnName(byte[] family, byte[] qualifier) {
    mFamily = family;
    mQualifier = qualifier;
  }

  /**
   * Gets the HBase column family.
   *
   * @return The family.
   */
  public byte[] getFamily() {
    return mFamily.clone();
  }

  /**
   * Gets the HBase column family as a string.
   *
   * @return The family as a string.
   */
  public String getFamilyAsString() {
    if (null == mFamilyString) {
      mFamilyString = Bytes.toString(mFamily);
    }
    return mFamilyString;
  }

  /**
   * Gets the HBase column qualifier.
   *
   * @return The qualifier.
   */
  public byte[] getQualifier() {
    return mQualifier.clone();
  }

  /**
   * Gets the HBase column qualifier as a string.
   *
   * @return The qualifier as a string.
   */
  public String getQualifierAsString() {
    if (null == mQualifierString) {
      mQualifierString = Bytes.toString(mQualifier);
    }
    return mQualifierString;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return getFamilyAsString() + ":" + getQualifierAsString();
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof HBaseColumnName)) {
      return false;
    }
    return toString().equals(other.toString());
  }
}
