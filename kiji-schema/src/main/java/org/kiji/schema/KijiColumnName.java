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

/**
 * A Kiji column name is composed of one or two parts: a family and a qualifier.
 * The qualifier is sometimes also referred to as the key.
 * If the column is of type map, the qualifier is null.
 * If the column is of type group, then the qualifier is required but may be empty.
 */
@ApiAudience.Public
public final class KijiColumnName implements Comparable<KijiColumnName> {

  /** The full name of the column "family(:qualifier)?". */
  private final String mFullName;

  /** The column family name. */
  private final String mFamily;

  /**
   * The column qualifier name:
   * <li> null means unqualified column.
   * <li> empty string means qualifier "".
   */
  private final String mQualifier;

  // Column family and qualifier names as byte arrays. Lazily initialized as needed.

  private byte[] mFamilyBytes;
  private byte[] mQualifierBytes;

  /**
   * Constructs a column name from a string "family" or "family:qualifier".
   *
   * @param fullName The name of a kiji column "family:qualifier".
   */
  public KijiColumnName(String fullName) {
    this(new Parser(Preconditions.checkNotNull(fullName)));
  }

  /**
   * Constructs a column name from the two-part family:qualifier.
   *
   * @param family The kiji column family.
   * @param qualifier The kiji column qualifier: null means unqualified.
   */
  public KijiColumnName(String family, String qualifier) {
    this(makeFullName(Preconditions.checkNotNull(family), qualifier), family, qualifier);
  }

  /**
   * Formats a column full name, given a family name and an optional qualifier.
   *
   * @param family Family name.
   * @param qualifier Optional qualifier (null means unqualified).
   * @return the column full name.
   */
  private static String makeFullName(String family, String qualifier) {
    Preconditions.checkNotNull(family);
    if (qualifier == null) {
      return family;
    } else {
      return String.format("%s:%s", family, qualifier);
    }
  }

  /**
   * Constructs a column name from a parser.
   *
   * @param parser A parsed column name.
   */
  private KijiColumnName(Parser parser) {
    this(parser.getFullName(), parser.getFamily(), parser.getQualifier());
  }

  /**
   * Constructs a column name from the three parts: full name, family, qualifier.
   *
   * @param fullName The full column name.
   * @param family The family.
   * @param qualifier The qualifier (may be null).
   */
  private KijiColumnName(String fullName, String family, String qualifier) {
    mFullName = fullName;
    mFamily = family;
    mQualifier = qualifier;
  }

  /**
   * Parses a column name into parts.
   */
  private static class Parser {
    private final String mFullName;
    private final String mFamily;
    private final String mQualifier;

    /**
     * Constructs a parser over an input column name.
     *
     * @param input The column name.
     */
    public Parser(String input) {
      if (null == input) {
        throw new NullPointerException("Column name may not be null");
      }
      int colon = input.indexOf(":");
      mFullName = input;
      mFamily = colon < 0 ? input : input.substring(0, colon);
      mQualifier = colon < 0 ? null : input.substring(colon + 1);
    }

    /** @return The full column name. */
    public String getFullName() {
      return mFullName;
    }

    /** @return The family name. */
    public String getFamily() {
      return mFamily;
    }

    /** @return The qualifier name. */
    public String getQualifier() {
      return mQualifier;
    }
  }

  /**
   * Gets the full name of the column.
   *
   * @return the full name of the column.
   */
  public String getName() {
    return mFullName;
  }

  /**
   * Gets the name of the column family.
   *
   * @return the family component of the column name.
   */
  public String getFamily() {
    return mFamily;
  }

  /**
   * Gets the name of the column family as a byte array.
   * Caches the result for subsequent calls. This method returns a shared
   * array instance for all calls; do not modify the returned byte array.
   *
   * @return the family component of the column name.
   */
  public byte[] getFamilyBytes() {
    if (null == mFamilyBytes) {
      mFamilyBytes = Bytes.toBytes(mFamily);
    }

    return mFamilyBytes;
  }

  /**
   * Gets the name of the column qualifier, which may be null or empty.
   *
   * @return the qualifier component of the column name.
   */
  public String getQualifier() {
    return mQualifier;
  }

  /**
   * Gets the name of the column qualifier as a byte array.
   * Caches the result for subsequent calls. This method returns a shared
   * array instance for all calls; do not modify the returned byte array.
   *
   * @return the qualifier component of the column name.
   */
  public byte[] getQualifierBytes() {
    if (null == mQualifierBytes) {
      mQualifierBytes = Bytes.toBytes(mQualifier);
    }

    return mQualifierBytes;
  }

  /**
   * Determines whether the name refers to a qualified family (vs. an entire family).
   *
   * @return whether the column refers to a fully qualified family.
   */
  public boolean isFullyQualified() {
    return (mQualifier != null);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return getName();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object otherObj) {
    if (otherObj == this) {
      return true;
    } else if (null == otherObj) {
      return false;
    } else if (!otherObj.getClass().equals(getClass())) {
      return false;
    }
    final KijiColumnName other = (KijiColumnName) otherObj;
    return mFullName.equals(other.mFullName);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return mFullName.hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public int compareTo(KijiColumnName o) {
    return this.toString().compareTo(o.toString());
  }
}
