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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.util.KijiNameValidator;

/**
 * A Kiji column name is composed of one or two parts: a family and a qualifier.
 * The qualifier can be either of map type or group type. If the column is of
 * group type, the qualifier should ideally match VALID_LAYOUT_PATTERN. Empty
 * qualifiers are normalized to null. That is, if fullName is of the form
 * "family" or "family:", the qualifier will be treated as null. Qualifiers must
 * be valid UTF-8.
 */
@ApiAudience.Public
@ApiStability.Stable
public final class KijiColumnName implements Comparable<KijiColumnName> {

  /**
   * Create a new KijiColumnName from a family and qualifier.
   *
   * @param family Family of the Kiji column for which to create a name.
   * @param qualifier Qualifier of the Kiji column for which to create a name.
   * @return a new KijiColumnName from the given family and qualifier.
   */
  public static KijiColumnName create(
      final String family,
      final String qualifier
  ) {
    return new KijiColumnName(family, qualifier);
  }

  /**
   * Create a new KijiColumnName from a column's full name.
   *
   * <p>
   *   The full name of a column is either "family" or "family:qualifier". An empty qualifier is
   *   interpreted as a null qualifier (i.e. "family" and "family:" are treated the same.
   * </p>
   *
   * @param fullName The full name of the Kiji column for which to create a name.
   * @return a new KijiColumnName from the given full name.
   */
  public static KijiColumnName create(
      final String fullName
  ) {
    return new KijiColumnName(fullName);
  }

  /** The column family name must match VALID_LAYOUT_NAME_PATTERN. */
  private final String mFamily;

  /**
   * The column qualifier name is:
   * <li> either null meaning unqualified column
   * <li> or nonempty UTF-8 String.
   */
  private final String mQualifier;

  /**
   * Constructs a column name from a string "family" or "family:qualifier".
   * Empty string qualifiers are normalized to null. That is, if fullName is
   * of the form "family" or "family:", the qualifier will be treated as null.
   *
   * @param fullName The name of a kiji column "family:qualifier".
   * @deprecated KijiColumnName constructors are deprecated. Please use the factory method
   *     {@link KijiColumnName#create(String)}. This constructor will become private in the future.
   */
  @Deprecated
  public KijiColumnName(String fullName) {
    Preconditions.checkArgument(fullName != null,
        "Column name may not be null. At least specify family");
    final int colon = fullName.indexOf(":");
    mFamily = colon < 0 ? fullName : fullName.substring(0, colon);
    mQualifier = colon < 0 || fullName.length() == colon + 1 ? null : fullName.substring(colon + 1);
    validateNames();
  }

  /**
   * Constructs a column name from the two-part family:qualifier.
   *
   * @param family The kiji column family.
   * @param qualifier The kiji column qualifier: null means unqualified.
   *   Empty string qualifiers are forced to be null.
   * @deprecated KijiColumnName constructors are deprecated. Please use the factory method
   *     {@link KijiColumnName#create(String, String)}. This constructor will become private in
   *     the future.
   */
  @Deprecated
  public KijiColumnName(String family, String qualifier) {
    Preconditions.checkArgument(family != null, "Family name may not be null.");
    mFamily = family;
    mQualifier = ("".equals(qualifier) ? null : qualifier);
    validateNames();
  }

  /**
   * Validates family names.
   */
  private void validateNames() {
    // Validate family name.
    if (!KijiNameValidator.isValidLayoutName(mFamily)) {
      throw new KijiInvalidNameException(String.format(
          "Invalid family name: %s Name must match pattern: %s",
          mFamily, KijiNameValidator.VALID_LAYOUT_NAME_PATTERN));
    }
  }

  /**
   * Gets the full name of the column.
   *
   * @return the full name of the column.
   */
  public String getName() {
    return (mQualifier == null ? mFamily : String.format("%s:%s", mFamily, mQualifier));
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
   * Gets the name of the column family as a UTF-8 byte array. This method
   * does not cache the array. If expecting ubiquitous use, caller should
   * cache the output.
   *
   * @return the family component of the column name.
   */
  public byte[] getFamilyBytes() {
    return Bytes.toBytes(mFamily);
  }

  /**
   * Gets the name of the column qualifier, which is either null or a non-empty UTF-8 String.
   *
   * @return the qualifier component of the column name.
   */
  public String getQualifier() {
    return mQualifier;
  }

  /**
   * Gets the name of the column qualifier as a UTF-8 byte array. This method
   * does not cache the array. If expecting ubiquitous use, caller should
   * cache the output.
   *
   * @return The qualifier component of the column name or null if qualifier is unspecified.
   */
  public byte[] getQualifierBytes() {
    return null == mQualifier ? null : Bytes.toBytes(mQualifier);
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
    return other.getFamily().equals(mFamily) && Objects.equal(other.getQualifier(), mQualifier);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(mFamily).append(mQualifier).toHashCode();
  }

  /** {@inheritDoc} */
  @Override
  public int compareTo(KijiColumnName otherObj) {
    final int comparison = this.mFamily.compareTo(otherObj.mFamily);
    if (0 == comparison) {
      return (this.isFullyQualified() ? mQualifier : "")
          .compareTo(otherObj.isFullyQualified() ? otherObj.getQualifier() : "");
    } else {
      return comparison;
    }
  }
}
