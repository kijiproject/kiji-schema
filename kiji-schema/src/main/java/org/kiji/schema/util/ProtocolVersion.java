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

package org.kiji.schema.util;

import com.google.common.base.Preconditions;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Parses version information for a file format or wire protocol and provides
 * comparison / reporting functions.
 *
 * <p>
 * This class facilitates the parsing of strings of the form "protocol-x.y.z",
 * where <em>protocol</em> is a string representing some namespace or qualifier
 * (i.e., a file format name, like "kiji" in the string "kiji-1.1" representing
 * a table layout JSON file), and <em>x</em>, <em>y</em> and <em>z</em> are
 * integers. <em>protocol</em>, <em>y</em> and <em>z</em> are optional.
 * </p>
 *
 * <p>You can parse a string into a ProtocolVersion using the static {@link #parse}
 * method. Its javadoc specifies further what strings constitute valid ProtocolVersions.</p>
 *
 * <p>Version numbers are compared numerically starting at the major version number
 * and moving to the right; each field is treated as a separate integer, not a
 * decimal value. If minor or revision numbers are omitted, they are treated as zero.</p>
 *
 * <p>The {@link #compareTo(ProtocolVersion)} function treats identical version numbers
 * with different protocol names as different; <code>foo-1.0</code> and
 * <code>bar-1.0</code> are not the same version number. ProtocolVersion instances will be
 * sorted first by protocol name (alphabetically), then by version number.</p>
 *
 * <p>The {@link #compareTo(ProtocolVersion)}, {@link #hashCode()}, and {@link
 * #equals(Object)} methods will regard versions omitting trailing <code>.0</code>'s as
 * equal. For example, <code>foo-1</code>, <code>foo-1.0</code>, and
 * <code>foo-1.0.0</code> are all equal.  The {@link #toString()} method will return the
 * exact string that was parsed; so equal objects may have unequal toString()
 * representations. Use {@link #toCanonicalString()} to get the same string representation
 * out of each.</p>
 *
 * <p>ProtocolVersion instances are immutable.</p>
 */
@ApiAudience.Framework
@ApiStability.Stable
public final class ProtocolVersion implements Comparable<ProtocolVersion> {
  /** The protocol name. May be null. */
  private final String mProtocol;

  /** The major version number. */
  private final int mMajorVer;

  /** The minor version number. */
  private final int mMinorVer;

  /** The version revision number. */
  private final int mRevision;

  /** The string representation of this version that was parsed. */
  private final String mParsedStr;

  /**
   * Primary constructor. Hidden; use {@link #parse}. Initializes the main fields.
   * These are boxed values to allow them to be optional.
   *
   * @param raw the raw string that was parsed.
   * @param protocol the protocol name; may be null.
   * @param major the major version digits. Must not be null.
   * @param minor the minor version digits. May be null.
   * @param revision the revision number digits. May be null.
   *     May only be null if minor is not null.
   */
  private ProtocolVersion(String raw, String protocol, int major, int minor, int revision) {
    mParsedStr = raw;
    mProtocol = protocol;
    mMajorVer = major;
    mMinorVer = minor;
    mRevision = revision;
  }

  /**
   * Static factory method that creates new ProtocolVersion instances.
   *
   * <p>This method parses its argument as a string of the form:
   * <tt>protocol-maj.min.rev</tt>. All fields except the major digit are optional.
   * maj, min, and rev must be non-negative integers. protocol is a string; the trailing
   * dash character is not part of the protocol name. The protocol name may not include
   * a '-' character and must not start with a digit.
   * If the protocol name is not specified, then the dash must be omitted.</p>
   *
   * <p>The following are examples of legal protocol versions:</p>
   * <ul>
   *   <li>"data-1.0"</li>
   *   <li>"kiji-1.2.3"</li>
   *   <li>"1.3.1"</li>
   *   <li>"76"</li>
   *   <li>"2.3"</li>
   *   <li>"spec-1"</li>
   * </ul>
   *
   * <p>The following are illegal:</p>
   * <ul>
   *   <li>"foo" (no version number)</li>
   *   <li>"foo1.2" (no dash after the protocol name)</li>
   *   <li>"-foo-1.2" (protocol may not start with a dash)</li>
   *   <li>"1.2.3.4" (only 3-component version numbers are supported)</li>
   *   <li>"bar-1.2.3.4" (only 3-component version numbers are supported)</li>
   *   <li>"-1.2.3" (version numbers may not be negative)</li>
   *   <li>"1.-2" (version numbers may not be negative)</li>
   *   <li>"1.x" (version numbers must be integers)</li>
   *   <li>"foo-1.x" (version numbers must be integers)</li>
   *   <li>"2foo-1.3" (protocol names may not start with a digit)</li>
   *   <li>"foo-bar-1.x" (protocol names may not contain a dash)</li>
   *   <li>"" (the empty string is not allowed as a version)</li>
   *   <li>null</li>
   * </ul>
   *
   * @param verString the version string to parse.
   * @return a new, parsed ProtocolVersion instance.
   * @throws IllegalArgumentException if the version string cannot be parsed according
   *     to the rules above.
   */
  public static ProtocolVersion parse(String verString) {
    Preconditions.checkArgument(null != verString, "verString may not be null");
    Preconditions.checkArgument(!verString.isEmpty(), "verString may not be empty");
    Preconditions.checkArgument(verString.charAt(0) != '-', "verString may not start with a dash");

    String proto = null;
    int major = 0;
    int minor = 0;
    int rev = 0;

    int pos = 0;
    String numericPart = verString;
    if (!Character.isDigit(verString.charAt(pos))) {
      // Start by parsing a protocol name. Scan forward to the '-'.
      String[] nameParts = verString.split("-");
      if (nameParts.length != 2) {
        throw new IllegalArgumentException("verString may contain at most one dash character, "
            + "separating the protocol name from the version number.");
      }

      proto = nameParts[0];
      numericPart = nameParts[1]; // only use the string part after the '-' for ver numbers.
    }

    String[] verParts = numericPart.split("\\.");
    if (verParts.length > 3) {
      throw new IllegalArgumentException("Version numbers may have at most three components.");
    }

    try {
      major = Integer.parseInt(verParts[0]);
      if (verParts.length > 1) {
        minor = Integer.parseInt(verParts[1]);
      }
      if (verParts.length > 2) {
        rev = Integer.parseInt(verParts[2]);
      }
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException("Could not parse numeric version info in " + verString);
    }

    if (major < 0) {
      throw new IllegalArgumentException("Major version number must be non-negative.");
    }
    if (minor < 0) {
      throw new IllegalArgumentException("Minor version number must be non-negative.");
    }
    if (rev < 0) {
      throw new IllegalArgumentException("Revision number must be non-negative.");
    }

    return new ProtocolVersion(verString, proto, major, minor, rev);
  }

  /**
   * Returns the protocol name associated with this version string. If
   * the entire version string is <tt>datafmt-1.2.3</tt>, then this will
   * return <tt>"datafmt"</tt>.
   *
   * @return a string representing the protocol name; may be null if no
   *     protocol name was specified when constructing this instance.
   */
  public String getProtocolName() {
    return mProtocol;
  }

  /**
   * Returns the major version (first digit) in this version. If
   * the entire version string is <tt>datafmt-1.2.3</tt>, then this will
   * return <tt>1</tt>.
   *
   * @return an integer representing the major version number.
   */
  public int getMajorVersion() {
    return mMajorVer;
  }

  /**
   * Returns the minor version (second digit) in this version. If
   * the entire version string is <tt>datafmt-1.2.3</tt>, then this will
   * return <tt>2</tt>. If the entire version string is <tt>datafmt-1</tt>,
   * then this will return <tt>0</tt>.
   *
   * @return an integer representing the minor version number. This number
   *     will be zero if no minor version was specified during construction.
   */
  public int getMinorVersion() {
    return mMinorVer;
  }

  /**
   * Returns the revision version (third digit) in this version. If
   * the entire version string is <tt>datafmt-1.2.3</tt>, then this will
   * return <tt>3</tt>. If the entire version string is <tt>datafmt-1</tt>
   * or <tt>datafmt-1.2</tt>, then this will return <tt>0</tt>.
   *
   * @return an integer representing the revision number. This number
   *     will be zero if no revision was specified during construction.
   */
  public int getRevision() {
    return mRevision;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (null == other) {
      return false;
    } else if (this == other) {
      return true;
    } else if (!(other instanceof ProtocolVersion)) {
      return false;
    }

    ProtocolVersion otherVer = (ProtocolVersion) other;

    if (!checkEquality(mProtocol, otherVer.mProtocol)) {
      return false;
    }

    return getMajorVersion() == otherVer.getMajorVersion()
        && getMinorVersion() == otherVer.getMinorVersion()
        && getRevision() == otherVer.getRevision();
  }

  /**
   * Returns true if both are null, or both are non-null and
   * thing1.equals(thing2).
   *
   * @param thing1 an element to check
   * @param thing2 the other element to check
   * @return true if they're both null, or if thing1.equals(thing2);
   *   false otherwise.
   */
  static boolean checkEquality(Object thing1, Object thing2) {
    if (null == thing1 && null != thing2) {
      return false;
    }

    if (null != thing1 && null == thing2) {
      return false;
    }

    if (thing1 != null) {
      return thing1.equals(thing2);
    } else {
      assert null == thing2;
      return true; // they're both null.
    }
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    int hash = 23;
    hash = hash * 31 + ((null != mProtocol) ? mProtocol.hashCode() : 0);
    hash = hash * 31 + mMajorVer;
    hash = hash * 31 + mMinorVer;
    hash = hash * 31 + mRevision;
    return hash;
  }

  /** {@inheritDoc} */
  @Override
  public int compareTo(ProtocolVersion other) {
    if (null == mProtocol && null != other.mProtocol) {
      // null protocol sorts ahead of any alphabetic protocol.
      return -1;
    } else if (mProtocol != null) {
      if (null == other.mProtocol) {
        return 1; // they sort as 'less than' us since their protocol name is null.
      }

      int protoCmp = mProtocol.compareTo(other.mProtocol);
      if (0 != protoCmp) {
        return protoCmp;
      }
    }

    int majCmp = Integer.valueOf(getMajorVersion()).compareTo(other.getMajorVersion());
    if (0 != majCmp) {
      return majCmp;
    }
    int minCmp = Integer.valueOf(getMinorVersion()).compareTo(other.getMinorVersion());
    if (0 != minCmp) {
      return minCmp;
    }
    int revCmp = Integer.valueOf(getRevision()).compareTo(other.getRevision());
    return revCmp;
  }

  /**
   * Returns the string representation of this ProtocolVersion that was initially
   * parsed to create this ProtocolVersion. Use {@link #toCanonicalString()} to get
   * a string representation that preserves the equals() relationship.
   *
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return mParsedStr;
  }

  /**
   * Returns a string representation of this ProtocolVersion that includes any
   * optional trailing version components. For example "foo-1.0" and "foo-1"
   * would both have canonical representations of "foo-1.0.0". ProtocolVersions
   * for which equals() returns true will have equal canonical string representations.
   *
   * @return the canonical string representation of this ProtocolVersion.
   */
  public String toCanonicalString() {
    StringBuilder sb = new StringBuilder();
    if (null != mProtocol) {
      sb.append(mProtocol);
      sb.append("-");
    }

    sb.append(mMajorVer);
    sb.append(".");
    sb.append(mMinorVer);
    sb.append(".");
    sb.append(mRevision);

    return sb.toString();
  }

}
