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

package org.kiji.schema.layout.impl;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.impl.InvalidColumnNameException;

/**
 * <p>A very short physical identifier for a column family or qualifier to be used in HBase.</p>
 *
 * <p>Since HBase is a sparse storage system, every cell's data must be stored along with
 * its full address: its row key, family name, column qualifier, and timestamp.  Because
 * of this, it is important to keep the names of families and qualifiers as short as
 * possible.</p>
 *
 * <p>A ColumnId is the physical byte[] that is used as a family or qualifier name in
 * HBase.  It is an encoded number using a 64-character alphabet with the more significant
 * digits to the right.  This class allows you to convert between the integers and the
 * UTF-8 encoded physical names.  For example:</p>
 *
 *   <table>
 *     <tr><th>id</th><th>name</th></tr>
 *     <tr><td>1</td><td>B</td></tr>
 *     <tr><td>1</td><td>BA</td></tr>
 *     <tr><td>1</td><td>BAA</td></tr>
 *     <tr><td>2</td><td>C</td></tr>
 *     <tr><td>25</td><td>Z</td></tr>
 *     <tr><td>26</td><td>a</td></tr>
 *     <tr><td>51</td><td>z</td></tr>
 *     <tr><td>52</td><td>0</td></tr>
 *     <tr><td>61</td><td>9</td></tr>
 *     <tr><td>62</td><td>+</td></tr>
 *     <tr><td>63</td><td>/</td></tr>
 *     <tr><td>64</td><td>AB</td></tr>
 *     <tr><td>65</td><td>BB</td></tr>
 *     <tr><td>66</td><td>CB</td></tr>
 *   </table>
 *
 * <p>The benefit here is that until a user has defined at least 64 column names in their
 * layout, all of the names used in HBase will only be one byte.  The next 64 names will
 * only be two bytes, and so on.
 */
@ApiAudience.Private
public final class ColumnId {
  /**
   * The special value reserved to mean that a symbolic name has not been assigned a column id.
   */
  public static final int UNASSIGNED = 0;

  /** The alphabet used to generate the short physical names. */
  public static final String ALPHABET =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

  /** The size of the alphabet for a digit, which is the radix of our printed number. */
  public static final int RADIX = 64;

  /**
   * The base 2 logarithm of the size of the alphabet (64), which is the number of bits
   * we can encode with a single digit.
   */
  public static final int BITS_PER_DIGIT = 6;

  /** A map from characters in the alphabet to the integer it represents. */
  public static final Map<Character, Integer> VALUE_MAP;
  static {
    // Make sure our alphabet is the size we expect.
    if (RADIX != ALPHABET.length()) {
      throw new InternalKijiError(
          "Expected ColumnId alphabet size to be " + RADIX + " but was " + ALPHABET.length());
    }

    // Initialize the map from digit to value.
    VALUE_MAP = new HashMap<Character, Integer>();
    for (int i = 0; i < ALPHABET.length(); i++) {
      VALUE_MAP.put(ALPHABET.charAt(i), i);
    }
  }

  /** The integer encoded by this column id. */
  private final int mId;

  /** The Base64 string encoding of this column id. */
  private final String mStringEncoding;

  /**
   * Constructs a column id that encodes the given integer.
   *
   * @param id The integer identifier for this column.
   */
  public ColumnId(int id) {
    Preconditions.checkArgument(id >= 0, "id may not be negative");
    mId = id;
    mStringEncoding = intToBase64(mId);
  }

  /**
   * Converts the given integer to Base64 encoding with the MSB to the right.
   * @param id is the decimal number to convert.
   * @return a string representing the Base64 encoding of the incoming integer.
   */
  public static String intToBase64(int id) {
    StringBuilder sb = new StringBuilder();
    int val = id;
    do {
      sb.append(ALPHABET.charAt(val % ALPHABET.length()));
      val >>= BITS_PER_DIGIT;
    } while (val > 0);
    return sb.toString();
  }

  /**
   * Translates HBase column names to ColumnIds.  The HBase byte arrays are UTF-8 encoded
   * numbers from our base-64 alphabet.
   *
   * @param encoded The family or qualifier bytes from HBase.
   * @return A ColumnId from the byte array.
   */
  public static ColumnId fromByteArray(byte[] encoded) {
    return fromString(Bytes.toString(encoded));
  }

  /**
   * Translates HBase column names to ColumnIds.  The HBase names are UTF-8 encoded
   * numbers from our base-64 alphabet.
   *
   * @param encoded The family or qualifier bytes from HBase.
   * @return A ColumnId from the encoded name.
   */
  public static ColumnId fromString(String encoded) {
    int val = 0;
    for (int i = 0; i < encoded.length(); i++) {
      try {
        val += VALUE_MAP.get(encoded.charAt(i)) << i * BITS_PER_DIGIT;
      } catch (NullPointerException e) {
        throw new InvalidColumnNameException("Contained a character not in the alphabet: "
            + encoded);
      }
    }
    return new ColumnId(val);
  }

  /** @return the column id. */
  public int getId() {
    return mId;
  }

  /**
   * <p>Translates ColumnIds to HBase column names.</p>
   *
   * <p>Encodes to a byte array making it as short as possible. We use characters that
   * HBase allows (no control characters and no colon).</p>
   *
   * @return The column id encoded as an HBase-friendly byte array.
   */
  public byte[] toByteArray() {
    return Bytes.toBytes(toString());
  }

  /**
   * Gets the id as a string of digits from our alphabet.
   *
   * @return The digit string, with the significant digits on the right.
   */
  @Override
  public String toString() {
    return mStringEncoding;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ColumnId)) {
      return false;
    }
    return getId() == ((ColumnId) other).getId();
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return getId();
  }
}
